package consistenthash

import (
	"fmt"
	"math"
	"sort"
	"testing"
)

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
	// 构造一些 key，使其 hash 落在较大区间末尾
	// 无需精确控制，只要验证能返回合法节点且稳定
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
	sort.Slice(arr, func(i, j int) bool { return arr[i].node < arr[j].node })

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
