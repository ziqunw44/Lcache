package Lcache

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"Lcache/store"
)

// ====================== Fake Store（仅用于单测） ======================

// 用于触发类型断言失败的“非 ByteView”值（但实现了 store.Value）
type fakeValue struct{ b []byte }

func (f fakeValue) Len() int { return len(f.b) }

// fakeStore 的元素
type testEntry struct {
	v         store.Value
	expiresAt time.Time // 零值表示不过期
}

// 最小可用的 fakeStore，实现 store.Store
type fakeStore struct {
	mu     sync.RWMutex
	items  map[string]testEntry
	closed int32
}

func newFakeStore() *fakeStore {
	return &fakeStore{items: make(map[string]testEntry)}
}

func (fs *fakeStore) Get(key string) (store.Value, bool) {
	if atomic.LoadInt32(&fs.closed) == 1 {
		return nil, false
	}
	fs.mu.RLock()
	e, ok := fs.items[key]
	fs.mu.RUnlock()
	if !ok {
		return nil, false
	}
	// 过期检查
	if !e.expiresAt.IsZero() && time.Now().After(e.expiresAt) {
		fs.Delete(key)
		return nil, false
	}
	return e.v, true
}

func (fs *fakeStore) Set(key string, val store.Value) error {
	if atomic.LoadInt32(&fs.closed) == 1 {
		return nil
	}
	fs.mu.Lock()
	fs.items[key] = testEntry{v: val}
	fs.mu.Unlock()
	return nil
}

func (fs *fakeStore) SetWithExpiration(key string, val store.Value, ttl time.Duration) error {
	if atomic.LoadInt32(&fs.closed) == 1 {
		return nil
	}
	fs.mu.Lock()
	fs.items[key] = testEntry{v: val, expiresAt: time.Now().Add(ttl)}
	fs.mu.Unlock()
	return nil
}

func (fs *fakeStore) Delete(key string) bool {
	if atomic.LoadInt32(&fs.closed) == 1 {
		return false
	}
	fs.mu.Lock()
	_, ok := fs.items[key]
	delete(fs.items, key)
	fs.mu.Unlock()
	return ok
}

func (fs *fakeStore) Clear() {
	if atomic.LoadInt32(&fs.closed) == 1 {
		return
	}
	fs.mu.Lock()
	fs.items = make(map[string]testEntry)
	fs.mu.Unlock()
}

func (fs *fakeStore) Len() int {
	if atomic.LoadInt32(&fs.closed) == 1 {
		return 0
	}
	fs.mu.RLock()
	n := len(fs.items)
	fs.mu.RUnlock()
	return n
}

func (fs *fakeStore) Close() {
	atomic.StoreInt32(&fs.closed, 1)
	fs.Clear()
}

// 让 Cache 直接使用 fakeStore，跳过 ensureInitialized()
func newCacheWithFakeStore() *Cache {
	c := NewCache(DefaultCacheOptions())
	c.store = newFakeStore()
	atomic.StoreInt32(&c.initialized, 1)
	return c
}

// 若你的 ByteView 有专门构造器（如 FromBytes），可以替换这里
func bvFromBytes(_ []byte) ByteView {
	var bv ByteView
	return bv
}

func keyOf(i int) string { return "k_" + strconv.Itoa(i) }

// =========================== 单元测试 ===========================

func TestCache_Basic_AddGet(t *testing.T) {
	c := newCacheWithFakeStore()
	ctx := context.Background()

	c.Add("k1", bvFromBytes([]byte("v1")))
	if _, ok := c.Get(ctx, "k1"); !ok {
		t.Fatalf("expected hit for k1")
	}
	stats := c.Stats()
	if stats["hits"].(int64) != 1 || stats["misses"].(int64) != 0 {
		t.Fatalf("unexpected hits/misses: %v/%v", stats["hits"], stats["misses"])
	}
}

func TestCache_Get_NotInitialized_IsMiss(t *testing.T) {
	// 未初始化直接 Get，应 miss +1
	c := NewCache(DefaultCacheOptions())
	if _, ok := c.Get(context.Background(), "nope"); ok {
		t.Fatalf("expected miss when cache not initialized")
	}
	stats := c.Stats()
	if stats["misses"].(int64) != 1 {
		t.Fatalf("expected misses=1, got %v", stats["misses"])
	}
}

func TestCache_AddWithExpiration(t *testing.T) {
	c := newCacheWithFakeStore()
	ctx := context.Background()

	c.AddWithExpiration("exp", bvFromBytes([]byte("v")), time.Now().Add(120*time.Millisecond))
	if _, ok := c.Get(ctx, "exp"); !ok {
		t.Fatalf("expected hit before expiration")
	}
	time.Sleep(160 * time.Millisecond)
	if _, ok := c.Get(ctx, "exp"); ok {
		t.Fatalf("expected miss after expiration")
	}
}

func TestCache_Delete_Clear_Len(t *testing.T) {
	c := newCacheWithFakeStore()
	c.Add("a", bvFromBytes(nil))
	c.Add("b", bvFromBytes(nil))
	c.Add("c", bvFromBytes(nil))

	if got := c.Len(); got != 3 {
		t.Fatalf("expected len=3, got %d", got)
	}
	if !c.Delete("b") {
		t.Fatalf("expected delete(b)=true")
	}
	if got := c.Len(); got != 2 {
		t.Fatalf("expected len=2 after delete, got %d", got)
	}

	c.Clear()
	if got := c.Len(); got != 0 {
		t.Fatalf("expected len=0 after clear, got %d", got)
	}
}

func TestCache_Stats_HitRate(t *testing.T) {
	c := newCacheWithFakeStore()
	ctx := context.Background()

	c.Add("h", bvFromBytes(nil))
	// 1 hit
	c.Get(ctx, "h")
	// 2 misses
	c.Get(ctx, "no1")
	c.Get(ctx, "no2")

	stats := c.Stats()
	h := stats["hits"].(int64)
	m := stats["misses"].(int64)
	if h != 1 || m != 2 {
		t.Fatalf("unexpected hits/misses %d/%d", h, m)
	}
	hr := stats["hit_rate"].(float64)
	if hr <= 0 || hr >= 1 {
		t.Fatalf("unexpected hit_rate: %v", hr)
	}
}

func TestCache_Close_IsIdempotent_And_BlocksOps(t *testing.T) {
	c := newCacheWithFakeStore()
	ctx := context.Background()

	c.Add("x", bvFromBytes(nil))
	c.Close()
	// 多次 Close 也应安全
	c.Close()

	if _, ok := c.Get(ctx, "x"); ok {
		t.Fatalf("expected miss after close")
	}
	// Add/Delete/Len/Stats 均可调用但不应出现问题
	c.Add("y", bvFromBytes(nil))
	if c.Len() != 0 {
		t.Fatalf("expected len=0 after close")
	}
}

func TestCache_TypeAssertionFailed_IncrementsMiss(t *testing.T) {
	// 直接构造 fakeStore 并塞入“非 ByteView”的 store.Value，触发断言失败路径
	c := NewCache(DefaultCacheOptions())
	fs := newFakeStore()
	_ = fs.Set("weird", fakeValue{b: []byte("x")})
	c.store = fs
	atomic.StoreInt32(&c.initialized, 1)

	if _, ok := c.Get(context.Background(), "weird"); ok {
		t.Fatalf("expected ok=false when type assertion to ByteView fails")
	}
	if misses := c.Stats()["misses"].(int64); misses != 1 {
		t.Fatalf("expected misses=1, got %d", misses)
	}
}

func TestCache_ConcurrentAccess_NoDataRace(t *testing.T) {
	c := newCacheWithFakeStore()
	ctx := context.Background()

	// 预热一些 key
	for i := 0; i < 200; i++ {
		c.Add(keyOf(i), bvFromBytes(nil))
	}

	var wg sync.WaitGroup
	readers, writers, deleters := 16, 8, 4
	wg.Add(readers + writers + deleters)

	for i := 0; i < readers; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 4000; j++ {
				c.Get(ctx, keyOf((id*37+j)%300))
			}
		}(i)
	}
	for i := 0; i < writers; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 2000; j++ {
				c.Add(keyOf((id*53+j)%350), bvFromBytes(nil))
			}
		}(i)
	}
	for i := 0; i < deleters; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				c.Delete(keyOf((id*71+j)%350))
			}
		}(i)
	}

	wg.Wait()
	// 运行时配合： go test -race
}

// =========================== 基准测试 ===========================

func BenchmarkCache_Get(b *testing.B) {
	c := newCacheWithFakeStore()
	ctx := context.Background()

	for i := 0; i < 20000; i++ {
		c.Add(keyOf(i), bvFromBytes(nil))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get(ctx, keyOf(i%20000))
	}
}
