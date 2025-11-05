package consistenthash

import (
	"fmt"
	"math"
	"sort"
	"testing"
	"sync/atomic"
	"time"
	"sync"
)

// --------- 测试辅助：并发安全地设置/读取计数 ---------

// 将某个节点的计数设置为 v（自动初始化 *int64 指针）
func storeCount(m *Map, node string, v int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.nodeCounts[node]
	if !ok || p == nil {
		var z int64
		m.nodeCounts[node] = &z
		p = &z
	}
	atomic.StoreInt64(p, v)
}

// 读取某个节点的计数（原子读；若不存在则视为 0）
func loadCount(m *Map, node string) int64 {
	m.mu.RLock()
	p := m.nodeCounts[node]
	m.mu.RUnlock()
	if p == nil {
		return 0
	}
	return atomic.LoadInt64(p)
}

// --------- 工具：构建环 ---------

// 替换原来的 newRing，允许传入 nil（用于 Benchmark）
func newRing(tb testing.TB, nodes ...string) *Map {
	if tb != nil {
		tb.Helper()
	}
	m := New()
	if err := m.Add(nodes...); err != nil {
		if tb != nil {
			tb.Fatalf("Add nodes error: %v", err)
		} else {
			// 基准里传了 nil，就直接 panic 让它失败即可
			panic(err)
		}
	}
	return m
}

func newRingWith(tb testing.TB, cfg *Config, nodes ...string) *Map {
	if tb != nil { tb.Helper() }
	m := New(WithConfig(cfg))
	if err := m.Add(nodes...); err != nil {
		if tb != nil { tb.Fatalf("Add nodes error: %v", err) } else { panic(err) }
	}
	return m
}

// --------- 常规功能测试 ---------

func TestEmptyRing(t *testing.T) {
	m := New()
	if n, ok := m.Get("any"); ok || n != "" {
		t.Fatalf("expect miss on empty ring, got ok=%v node=%q", ok, n)
	}
}

func TestAddAndGetDeterministic(t *testing.T) {
	m := newRing(t, "nodeA", "nodeB", "nodeC")

	// 同一个 key 多次查询，应稳定映射到同一个节点
	keys := []string{"k1", "k2", "k3", "用户:42", "img:0001.png"}
	for _, k := range keys {
		n1, ok1 := m.Get(k)
		n2, ok2 := m.Get(k)
		if !ok1 || !ok2 {
			t.Fatalf("expect hit for key %q", k)
		}
		if n1 != n2 {
			t.Fatalf("determinism broken for key %q: %q != %q", k, n1, n2)
		}
	}
}

func TestWrapAround(t *testing.T) {
	// 仅两个节点，便于触发环回
	m := newRing(t, "nodeA", "nodeZ")
	keys := []string{
		"zzzzzzzzzz-1",
		"zzzzzzzzzz-2",
		"zzzzzzzzzz-3",
	}
	for _, k := range keys {
		n, ok := m.Get(k)
		if !ok || n == "" {
			t.Fatalf("wrap-around expect hit for %q", k)
		}
	}
}

func TestRemove(t *testing.T) {
	m := newRing(t, "nodeA", "nodeB", "nodeC")

	// 先采样一批 key，记录哪些落在 nodeB
	var affected []string
	for i := 0; i < 500; i++ {
		k := fmt.Sprintf("key-%d", i)
		if n, ok := m.Get(k); ok && n == "nodeB" {
			affected = append(affected, k)
		}
	}
	if len(affected) == 0 {
		t.Fatalf("no keys mapped to nodeB, test setup too small")
	}

	// 移除 nodeB
	if err := m.Remove("nodeB"); err != nil {
		t.Fatalf("remove nodeB failed: %v", err)
	}

	// 1) 再次查询任何 key，都不应映射到 nodeB
	for i := 0; i < 500; i++ {
		k := fmt.Sprintf("key-%d", i)
		if n, ok := m.Get(k); ok && n == "nodeB" {
			t.Fatalf("after remove, key %q still maps to nodeB", k)
		}
	}

	// 2) 原先属于 nodeB 的那部分 key，现在应映射到其它节点（局部迁移）
	for _, k := range affected {
		n, ok := m.Get(k)
		if !ok || n == "" || n == "nodeB" {
			t.Fatalf("key %q should remap to other node, got %q", k, n)
		}
	}
}

func TestDistributionRoughlyEven(t *testing.T) {
	// 多一些节点与副本可以让分布更均匀（默认 replicas=100）
	m := newRing(t, "nodeA", "nodeB", "nodeC", "nodeD")

	// 采样很多 key，统计命中分布
	counts := map[string]int{}
	total := 20000
	for i := 0; i < total; i++ {
		k := fmt.Sprintf("k-%d", i)
		n, ok := m.Get(k)
		if !ok {
			t.Fatalf("unexpected miss for key %q", k)
		}
		counts[n]++
	}

	// 计算各节点占比，并检查是否在容忍阈内（±15%）
	type kv struct {
		node  string
		count int
		ratio float64
	}
	var arr []kv
	for node, c := range counts {
		arr = append(arr, kv{node: node, count: c, ratio: float64(c) / float64(total)})
	}
	sort.Slice(arr, func(i, j int) bool { return arr[i].node < arr[i].node })

	expected := 1.0 / 4.0
	tolerance := 0.15 // 允许 ±15%
	for _, it := range arr {
		if math.Abs(it.ratio-expected) > tolerance {
			t.Fatalf("distribution too skewed: node=%s ratio=%.3f expect≈%.3f(±%.2f)",
				it.node, it.ratio, expected, tolerance)
		}
	}
}

func TestAddThenGetAfterSort(t *testing.T) {
	// 验证 Add 完成后 keys 已排序且可正常查询
	m := newRing(t, "nodeX", "nodeY")
	// 任意 key 均应获得命中
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("hello-%d", i)
		if n, ok := m.Get(k); !ok || n == "" {
			t.Fatalf("expect hit after add/sort for key %q", k)
		}
	}
}

// --------- 再均衡相关测试（已改为原子读写计数） ---------

func TestRebalanceAdjustsReplicasByLoad(t *testing.T) {
	// 构造 3 节点
	cfg := *DefaultConfig
	cfg.MinReplicas = 50
	cfg.MaxReplicas = 200
	cfg.DefaultReplicas = 100
	cfg.LoadBalanceThreshold = 0.20

	m := newRingWith(t, &cfg, "A","B","C")

	// 模拟严重不均衡的负载：A特忙、B中、C很闲
	storeCount(m, "A", 700)
	storeCount(m, "B", 200)
	storeCount(m, "C", 100)
	atomic.StoreInt64(&m.totalRequests, 1000)

	// 触发再均衡
	m.checkAndRebalance()

	m.mu.RLock()
	defer m.mu.RUnlock()

	a := m.nodeReplicas["A"]
	b := m.nodeReplicas["B"]
	c := m.nodeReplicas["C"]

	if a != 50 {
		t.Fatalf("A replicas expect=50 (min cap) got=%d", a)
	}
	if b < 120 || b > 150 {
		t.Fatalf("B replicas expect around 140, got=%d", b)
	}
	if c < 160 || c > 180 {
		t.Fatalf("C replicas expect around 170, got=%d", c)
	}

	// 计数应被清零
	if atomic.LoadInt64(&m.totalRequests) != 0 {
		t.Fatalf("totalRequests should be reset to 0")
	}
	if loadCount(m, "A") != 0 || loadCount(m, "B") != 0 || loadCount(m, "C") != 0 {
		t.Fatalf("nodeCounts should be reset to 0")
	}
}

func TestRebalanceRespectsMaxMinReplicas(t *testing.T) {
	cfg := *DefaultConfig
	cfg.MinReplicas = 10
	cfg.MaxReplicas = 20
	cfg.DefaultReplicas = 15
	cfg.LoadBalanceThreshold = 0.10

	m := newRingWith(t, &cfg, "X","Y")

	// 让 X 过载、Y 几乎空闲，迫使 X 降到 Min、Y 升到 Max
	storeCount(m, "X", 900)
	storeCount(m, "Y", 100)
	atomic.StoreInt64(&m.totalRequests, 1000)

	m.checkAndRebalance()

	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.nodeReplicas["X"] != m.config.MinReplicas {
		t.Fatalf("X should be clamped to MinReplicas=%d, got=%d", m.config.MinReplicas, m.nodeReplicas["X"])
	}
	if m.nodeReplicas["Y"] != m.config.MaxReplicas {
		t.Fatalf("Y should be clamped to MaxReplicas=%d, got=%d", m.config.MaxReplicas, m.nodeReplicas["Y"])
	}
}

func TestNoRebalanceWhenBelowThreshold(t *testing.T) {
	cfg := *DefaultConfig
	cfg.MinReplicas = 50
	cfg.MaxReplicas = 200
	cfg.DefaultReplicas = 100
	cfg.LoadBalanceThreshold = 0.25

	m := newRingWith(t, &cfg, "A","B","C")

	// 轻微偏差（最大相对偏差约 < 25%）
	storeCount(m, "A", 350)
	storeCount(m, "B", 330)
	storeCount(m, "C", 320)
	atomic.StoreInt64(&m.totalRequests, 1000) // 触发判定

	// 记录原始副本
	before := map[string]int{}
	m.mu.RLock()
	for n, r := range m.nodeReplicas {
		before[n] = r
	}
	m.mu.RUnlock()

	m.checkAndRebalance()

	// 副本应不变
	m.mu.RLock()
	defer m.mu.RUnlock()
	for n, r := range before {
		if m.nodeReplicas[n] != r {
			t.Fatalf("replicas changed under threshold: node=%s before=%d after=%d", n, r, m.nodeReplicas[n])
		}
	}
}

func TestGetStatsReturnsRatios(t *testing.T) {
	m := newRing(t, "A", "B")

	// 模拟：A 70%，B 30%
	storeCount(m, "A", 700)
	storeCount(m, "B", 300)
	atomic.StoreInt64(&m.totalRequests, 1000)

	s := m.GetStats()
	if len(s) != 2 {
		t.Fatalf("expect 2 entries, got %d", len(s))
	}
	if math.Abs(s["A"]-0.7) > 0.01 || math.Abs(s["B"]-0.3) > 0.01 {
		t.Fatalf("unexpected ratios: %+v", s)
	}
}

// --------- 并发压测 ---------

func BenchmarkGet(b *testing.B) {
	m := newRing(nil, "nodeA", "nodeB", "nodeC", "nodeD", "nodeE")
	keys := make([]string, 0, 1<<10)
	for i := 0; i < cap(keys); i++ {
		keys = append(keys, fmt.Sprintf("k-%d", i))
	}
	b.ResetTimer()
	var sink string
	for i := 0; i < b.N; i++ {
		k := keys[i&(len(keys)-1)]
		n, _ := m.Get(k)
		sink = n
	}
	_ = sink
}

func TestConcurrentGetAndRebalance_NoDataRaces(t *testing.T) {
	t.Parallel()

	cfg := *DefaultConfig
	cfg.MinReplicas = 50
	cfg.MaxReplicas = 200
	cfg.DefaultReplicas = 100
	cfg.LoadBalanceThreshold = 0.20

	m := newRingWith(t, &cfg, "n1", "n2", "n3", "n4")

	// 构造一批热点+冷门 key
	keys := make([]string, 0, 5000)
	for i := 0; i < 4800; i++ {
		keys = append(keys, fmt.Sprintf("hot-%d", i%64)) // 高频
	}
	for i := 0; i < 200; i++ {
		keys = append(keys, fmt.Sprintf("cold-%d", i))   // 低频
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// 读流量 goroutines
	readers := 16
	wg.Add(readers)
	for r := 0; r < readers; r++ {
		go func(id int) {
			defer wg.Done()
			idx := id
			for {
				select {
				case <-stop:
					return
				default:
					k := keys[idx%len(keys)]
					m.Get(k) // 忽略返回，只施压
					idx += 7
				}
			}
		}(r)
	}

	// 触发再均衡 goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				m.checkAndRebalance()
			}
		}
	}()

	// 跑一小段时间
	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()

	// 基本断言：仍可正常查询
	for i := 0; i < 100; i++ {
		if n, ok := m.Get(fmt.Sprintf("probe-%d", i)); !ok || n == "" {
			t.Fatalf("probe get failed at %d", i)
		}
	}
}
