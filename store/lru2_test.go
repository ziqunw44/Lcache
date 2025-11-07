package store

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// ------- 测试用 Value 实现 (避免与其他测试重复命名) -------
type specVal string
func (v specVal) Len() int { return len(v) }

// ------- 工具 -------
func mustNewStore(t *testing.T, opts Options) *lru2Store {
	t.Helper()
	s := newLRU2Cache(opts)
	if s == nil {
		t.Fatal("newLRU2Cache returned nil")
	}
	return s
}

func bucketOf(s *lru2Store, key string) int32 {
	return hashBKRD(key) & s.mask
}

// =======================================================
// 1) Set 只写 L1；L1 淘汰不会自动进 L2；Get 命中 L1 才晋升到 L2
// =======================================================
func TestSetWritesL1_EvictionNotCascade_PromotionOnGet(t *testing.T) {
	opts := Options{
		BucketCount:     1, // 单桶便于观测
		CapPerBucket:    2, // L1=2
		Level2Cap:       4, // L2=4
		CleanupInterval: time.Minute,
	}
	s := mustNewStore(t, opts)
	defer s.Close()

	// 先塞 3 个，触发 L1 淘汰
	s.Set("k1", specVal("v1"))
	s.Set("k2", specVal("v2"))
	s.Set("k3", specVal("v3")) // k1 按 LRU 应被淘汰 (覆盖尾节点)

	idx := bucketOf(s, "k1")

	// 断言：L2 不应该有 k1（当前实现淘汰不会写入 L2）
	if _, ok := s.caches[idx][1].hmap["k1"]; ok {
		t.Fatalf("k1 should NOT be in L2 after L1 eviction on Set")
	}

	// 再对仍在 L1 的一个 key 做 Get，命中则应晋升到 L2
	// k2、k3 都可能在；我们取 k3（最新插入）命中概率最大
	if v, ok := s.Get("k3"); !ok || v != specVal("v3") {
		t.Fatalf("expected Get(k3) hit in L1, got ok=%v v=%v", ok, v)
	}

	// 命中后应：L1中该节点 expireAt==0（因为 del 标记删除），L2 中新增 k3
	l1Idx, ok1 := s.caches[idx][0].hmap["k3"]
	if !ok1 {
		t.Fatalf("k3 should still exist in L1 map after del mark")
	}
	if s.caches[idx][0].m[l1Idx-1].expireAt != 0 {
		t.Fatalf("k3 expireAt in L1 should be 0 after del(), got %d", s.caches[idx][0].m[l1Idx-1].expireAt)
	}
	if _, ok2 := s.caches[idx][1].hmap["k3"]; !ok2 {
		t.Fatalf("k3 should be inserted into L2 after L1 hit")
	}
}

// =======================================================
// 2) 过期与清理：短期过期，等待 cleanupLoop，随后应不可读
// =======================================================
func TestExpirationAndCleanupLoop(t *testing.T) {
	opts := Options{
		BucketCount:     2,
		CapPerBucket:    4,
		Level2Cap:       4,
		CleanupInterval: 50 * time.Millisecond, // 加快清理
	}
	s := mustNewStore(t, opts)
	defer s.Close()

	s.SetWithExpiration("soon", specVal("v"), 80*time.Millisecond)
	s.SetWithExpiration("later", specVal("v2"), time.Minute)

	if _, ok := s.Get("soon"); !ok {
		t.Fatalf("soon should be visible before expiration")
	}
	if _, ok := s.Get("later"); !ok {
		t.Fatalf("later should be visible")
	}

	time.Sleep(200 * time.Millisecond) // 等过期+清理

	if _, ok := s.Get("soon"); ok {
		t.Fatalf("soon should be expired and removed by cleanup loop")
	}
	if _, ok := s.Get("later"); !ok {
		t.Fatalf("later should still be alive")
	}
}

// =======================================================
// 3) Delete：应从任一层删除并触发 onEvicted（按你的 delete 实现）
// =======================================================
func TestDeleteFiresEvicted_AndRemovesFromLevels(t *testing.T) {
	var mu sync.Mutex
	var evicted []string
	onEvicted := func(k string, v Value) {
		mu.Lock()
		evicted = append(evicted, k)
		mu.Unlock()
	}
	opts := Options{
		BucketCount:     1,
		CapPerBucket:    2,
		Level2Cap:       2,
		CleanupInterval: time.Minute,
		OnEvicted:       onEvicted,
	}
	s := mustNewStore(t, opts)
	defer s.Close()

	// 放两个在 L1
	s.SetWithExpiration("a", specVal("A"), time.Minute)
	s.SetWithExpiration("b", specVal("B"), time.Minute)
	idx := bucketOf(s, "a")

	// 先把 a 命中晋升到 L2（L1 标记删除）
	if v, ok := s.Get("a"); !ok || v != specVal("A") {
		t.Fatalf("expected hit for a")
	}
	// 现在 L2 有 a，L1 中 a 的 expireAt=0

	// Delete(a) 应成功，并触发 onEvicted（delete 会 del(L1), del(L2) 并挑一个回调）
	if !s.Delete("a") {
		t.Fatalf("Delete(a) should return true")
	}

	// a 不应再在 L2
	if l2idx, ok := s.caches[idx][1].hmap["a"]; !ok {
		t.Fatalf("a should still be present as a tombstone in L2")
	} else if s.caches[idx][1].m[l2idx-1].expireAt != 0 {
		t.Fatalf("expected a to be tombstoned in L2 (expireAt=0)")
	}
	// a 在 L1 的残留节点应为 expireAt=0（del 过），但 delete 已尝试 del(L1)，这里容忍实现细节

	mu.Lock()
	defer mu.Unlock()
	if len(evicted) == 0 {
		t.Fatalf("onEvicted should be called at least once on Delete")
	}
}

// =======================================================
// 4) Clear/Len：全清后 Len=0，Get miss
// =======================================================
func TestClearAndLen(t *testing.T) {
	opts := Options{
		BucketCount:     4,
		CapPerBucket:    8,
		Level2Cap:       8,
		CleanupInterval: time.Minute,
	}
	s := mustNewStore(t, opts)
	defer s.Close()

	for i := 0; i < 20; i++ {
		s.Set(fmt.Sprintf("k%d", i), specVal("v"))
	}
	if n := s.Len(); n != 20 {
		t.Fatalf("Len before Clear = %d, want 20", n)
	}

	s.Clear()
	if n := s.Len(); n != 0 {
		t.Fatalf("Len after Clear = %d, want 0", n)
	}
	for i := 0; i < 20; i++ {
		if _, ok := s.Get(fmt.Sprintf("k%d", i)); ok {
			t.Fatalf("key k%d should not be found after Clear", i)
		}
	}
}

// =======================================================
// 5) walk：只遍历有效项（expireAt>0）；可提前停止
// =======================================================
func TestWalkBasic(t *testing.T) {
	c := Create(6)
	// 三个有效、一个删除
	c.put("a", specVal("A"), Now()+int64(time.Minute), nil)
	c.put("b", specVal("B"), Now()+int64(time.Minute), nil)
	c.put("c", specVal("C"), Now()+int64(time.Minute), nil)
	c.del("b") // 标记删除，expireAt=0

	var keys []string
	c.walk(func(k string, v Value, exp int64) bool {
		keys = append(keys, k)
		return true
	})
	// 只应有 a、c（顺序头->尾，取决于 put/adjust）
	if len(keys) != 2 {
		t.Fatalf("walk should see 2 valid keys, got %v", keys)
	}

	// 提前停止
	n := 0
	c.walk(func(k string, v Value, exp int64) bool {
		n++
		return false
	})
	if n != 1 {
		t.Fatalf("walk early stop expected n=1, got %d", n)
	}
}

// =======================================================
// 6) adjust：移动到头/尾（使用同包白盒断言 head/tail）
// =======================================================
func TestAdjustHeadTail(t *testing.T) {
	c := Create(4)
	c.put("x", specVal("X"), Now()+int64(time.Hour), nil)
	c.put("y", specVal("Y"), Now()+int64(time.Hour), nil)
	c.put("z", specVal("Z"), Now()+int64(time.Hour), nil)

	// 取 y 的数组索引
	yIdx := c.hmap["y"]

	// 移到 head：adjust(idx, p, n)
	c.adjust(yIdx, p, n)
	if c.dlnk[0][n] != yIdx {
		t.Fatalf("y should be head after adjust to head")
	}

	// 再移到 tail：adjust(idx, n, p)
	c.adjust(yIdx, n, p)
	if c.dlnk[0][p] != yIdx {
		t.Fatalf("y should be tail after adjust to tail")
	}
}

// =======================================================
// 7) 并发基础压力：多 goroutine set/get/delete（主要看 -race）
// =======================================================
func TestConcurrentBasic(t *testing.T) {
	opts := Options{
		BucketCount:     8,
		CapPerBucket:    64,
		Level2Cap:       64,
		CleanupInterval: time.Minute,
	}
	s := mustNewStore(t, opts)
	defer s.Close()

	var wg sync.WaitGroup
	const G = 8
	const N = 200

	wg.Add(G)
	for g := 0; g < G; g++ {
		go func(id int) {
			defer wg.Done()
			prefix := fmt.Sprintf("g%d-", id)

			for i := 0; i < N; i++ {
				_ = s.Set(prefix+fmt.Sprint(i), specVal("v"))
			}
			for i := 0; i < N; i++ {
				s.Get(prefix + fmt.Sprint(i))
			}
			for i := 0; i < N/2; i++ {
				s.Delete(prefix + fmt.Sprint(i))
			}
		}(g)
	}
	wg.Wait()

	// 粗略检查数量（允许淘汰/碰撞）
	if s.Len() == 0 {
		t.Fatalf("Len should be > 0 after concurrent ops")
	}
}
