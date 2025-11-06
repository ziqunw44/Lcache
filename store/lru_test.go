package store

import (
	"math/rand"
	"sync"
	"testing"
	"time"
)

// ------- 测试辅助类型与函数 -------

// 实现 Value 接口的简单类型
type testValue []byte

func (v testValue) Len() int { return len(v) }

func tv(s string) testValue { return testValue([]byte(s)) }

func mustSet(t *testing.T, c *lruCache, k string, v Value) {
	t.Helper()
	if err := c.Set(k, v); err != nil {
		t.Fatalf("Set(%q) error = %v", k, err)
	}
}

func mustSetTTL(t *testing.T, c *lruCache, k string, v Value, ttl time.Duration) {
	t.Helper()
	if err := c.SetWithExpiration(k, v, ttl); err != nil {
		t.Fatalf("SetWithExpiration(%q) error = %v", k, err)
	}
}

// ------- 单元测试 -------

func TestSetGetBasic(t *testing.T) {
	c := newLRUCache(Options{
		MaxBytes:        1 << 20,
		CleanupInterval: 24 * time.Hour, // 测试中不依赖后台清理
	})
	defer c.Close()

	mustSet(t, c, "a", tv("1"))
	if got, ok := c.Get("a"); !ok || string(got.(testValue)) != "1" {
		t.Fatalf("Get(a) = %q, %v; want 1, true", got, ok)
	}

	// 覆盖写
	mustSet(t, c, "a", tv("22"))
	if got, ok := c.Get("a"); !ok || string(got.(testValue)) != "22" {
		t.Fatalf("Get(a) after overwrite = %q, %v; want 22, true", got, ok)
	}

	// 不存在
	if _, ok := c.Get("no-such"); ok {
		t.Fatalf("Get(no-such) should be false")
	}
}

func TestExpiration_GetAndBackground(t *testing.T) {
	evicted := struct {
		mu sync.Mutex
		ks []string
	}{}
	c := newLRUCache(Options{
		MaxBytes: 1 << 20,
		OnEvicted: func(k string, _ Value) {
			evicted.mu.Lock()
			defer evicted.mu.Unlock()
			evicted.ks = append(evicted.ks, k)
		},
		CleanupInterval: 10 * time.Millisecond,
	})
	defer c.Close()

	// 短 TTL
	mustSetTTL(t, c, "k1", tv("v1"), 30*time.Millisecond)
	// 长 TTL
	mustSetTTL(t, c, "k2", tv("v2"), time.Second)

	// 立即可取
	if v, ok := c.Get("k1"); !ok || string(v.(testValue)) != "v1" {
		t.Fatalf("Get(k1) got=%v ok=%v", v, ok)
	}

	// 等待 k1 过期
	time.Sleep(50 * time.Millisecond)

	// 命中过期：Get 路径应返回 false，并触发删除（若你的实现是同步删除则立刻不存在）
	if _, ok := c.Get("k1"); ok {
		t.Fatalf("Get(k1) should be expired")
	}
	// 留一点时间给后台清理与回调
	time.Sleep(30 * time.Millisecond)

	// k2 仍然存在
	if v, ok := c.Get("k2"); !ok || string(v.(testValue)) != "v2" {
		t.Fatalf("Get(k2) got=%v ok=%v", v, ok)
	}
}

func TestGetWithExpirationAndTTL(t *testing.T) {
	c := newLRUCache(Options{
		MaxBytes:        1 << 20,
		CleanupInterval: 24 * time.Hour,
	})
	defer c.Close()

	mustSetTTL(t, c, "x", tv("vx"), 80*time.Millisecond)

	v, ttl, ok := c.GetWithExpiration("x")
	if !ok || string(v.(testValue)) != "vx" || ttl <= 0 {
		t.Fatalf("GetWithExpiration(x) got=(%v,%v,%v) want v, ttl>0, true", v, ttl, ok)
	}

	time.Sleep(100 * time.Millisecond)

	if _, ttl2, ok2 := c.GetWithExpiration("x"); ok2 || ttl2 != 0 {
		t.Fatalf("GetWithExpiration(x) after expiry should be (nil,0,false)")
	}
}

func TestUpdateExpiration(t *testing.T) {
	c := newLRUCache(Options{
		MaxBytes:        1 << 20,
		CleanupInterval: 24 * time.Hour,
	})
	defer c.Close()

	mustSetTTL(t, c, "k", tv("v"), 40*time.Millisecond)
	time.Sleep(25 * time.Millisecond)

	// 延长 TTL
	if !c.UpdateExpiration("k", 200*time.Millisecond) {
		t.Fatalf("UpdateExpiration(k) = false")
	}
	time.Sleep(50 * time.Millisecond)

	// 仍应存在
	if _, ok := c.Get("k"); !ok {
		t.Fatalf("Get(k) should still exist after TTL update")
	}

	// 移除 TTL
	if !c.UpdateExpiration("k", 0) {
		t.Fatalf("UpdateExpiration(k,0) = false")
	}
	// 再等仍不应过期
	time.Sleep(80 * time.Millisecond)
	if _, ok := c.Get("k"); !ok {
		t.Fatalf("Get(k) should still exist after removing TTL")
	}
}

func TestEvictByMaxBytes_LRUOrder(t *testing.T) {
	var evictedKeysMu sync.Mutex
	var evictedKeys []string
	c := newLRUCache(Options{
		MaxBytes: 6, // 很小，触发淘汰
		OnEvicted: func(k string, _ Value) {
			evictedKeysMu.Lock()
			defer evictedKeysMu.Unlock()
			evictedKeys = append(evictedKeys, k)
		},
		CleanupInterval: 24 * time.Hour,
	})
	defer c.Close()

	// 每个键开销：len(key)+value.Len()
	// 我们用非常短的 value 来精确触发 LRU 淘汰
	mustSet(t, c, "a", tv("1")) // used ≈ len("a")+1 = 2
	mustSet(t, c, "b", tv("2")) // used ≈ 2+2=4
	mustSet(t, c, "c", tv("3")) // used ≈ 4+2=6

	// 访问顺序：触发 LRU 移动（让 a 最新）
	if _, _ = c.Get("a"); true {}

	// 插入新项，触发淘汰最久未用的项（应为 b 或 c，取决于你的 MoveToBack 语义；我们更精确地再访问 c）
	if _, _ = c.Get("c"); true {}

	// 再插入，迫使淘汰 "b"
	mustSet(t, c, "d", tv("4"))

	// b 应该被淘汰
	if _, ok := c.Get("b"); ok {
		t.Fatalf("b should be evicted")
	}
	// a/c/d 应该存在
	for _, k := range []string{"a", "c", "d"} {
		if _, ok := c.Get(k); !ok {
			t.Fatalf("%s should exist", k)
		}
	}
}

func TestDeleteAndClear(t *testing.T) {
	var evicted []string
	var mu sync.Mutex
	c := newLRUCache(Options{
		MaxBytes: 1 << 20,
		OnEvicted: func(k string, _ Value) {
			mu.Lock()
			evicted = append(evicted, k)
			mu.Unlock()
		},
		CleanupInterval: 24 * time.Hour,
	})
	defer c.Close()

	mustSet(t, c, "k1", tv("v1"))
	mustSet(t, c, "k2", tv("v2"))
	if !c.Delete("k1") {
		t.Fatalf("Delete(k1) = false")
	}
	if _, ok := c.Get("k1"); ok {
		t.Fatalf("k1 should not exist")
	}

	c.Clear()
	if c.Len() != 0 || c.UsedBytes() != 0 {
		t.Fatalf("Clear() did not reset state")
	}
}

func TestUsedAndMaxBytes(t *testing.T) {
	c := newLRUCache(Options{
		MaxBytes:        8,
		CleanupInterval: 24 * time.Hour,
	})
	defer c.Close()

	mustSet(t, c, "a", tv("12")) // key 1 + val 2 = 3
	if got := c.MaxBytes(); got != 8 {
		t.Fatalf("MaxBytes()=%d want 8", got)
	}
	if ub := c.UsedBytes(); ub <= 0 {
		t.Fatalf("UsedBytes() should be >0 after set, got %d", ub)
	}

	// 调整阈值，触发淘汰
	c.SetMaxBytes(3)
	time.Sleep(10 * time.Millisecond)
	if _, ok := c.Get("a"); !ok {
		t.Fatalf("a should remain when limit equals its size")
	}

	c.SetMaxBytes(2) // 小于 a 的占用，必须被淘汰
	time.Sleep(10 * time.Millisecond)
	if _, ok := c.Get("a"); ok {
		t.Fatalf("a should be evicted after SetMaxBytes(2)")
	}
}

func TestCloseIdempotent(t *testing.T) {
	c := newLRUCache(Options{
		MaxBytes:        1 << 20,
		CleanupInterval: time.Millisecond, // 快速 ticker
	})
	// 多次 Close 不应 panic
	c.Close()
	c.Close()
}

func TestConcurrentSetGetDelete(t *testing.T) {
	c := newLRUCache(Options{
		MaxBytes:        1 << 20,
		CleanupInterval: 1 * time.Millisecond,
	})
	defer c.Close()

	const N = 200
	var wg sync.WaitGroup
	wg.Add(3)

	// writer
	go func() {
		defer wg.Done()
		for i := 0; i < N; i++ {
			k := "k" + string('a'+rune(i%26))
			_ = c.SetWithExpiration(k, tv("v"), time.Duration(rand.Intn(4))*time.Millisecond)
			time.Sleep(time.Microsecond * 200)
		}
	}()

	// reader
	go func() {
		defer wg.Done()
		for i := 0; i < N*3; i++ {
			k := "k" + string('a'+rune(rand.Intn(26)))
			c.Get(k)
			c.GetWithExpiration(k)
			time.Sleep(time.Microsecond * 100)
		}
	}()

	// deleter
	go func() {
		defer wg.Done()
		for i := 0; i < N; i++ {
			k := "k" + string('a'+rune(rand.Intn(26)))
			c.Delete(k)
			time.Sleep(time.Microsecond * 150)
		}
	}()

	wg.Wait()
}

// ------- 基准测试（可选） -------

func BenchmarkSet(b *testing.B) {
	c := newLRUCache(Options{
		MaxBytes:        1 << 30,
		CleanupInterval: time.Hour,
	})
	defer c.Close()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = c.Set("k"+string(rune(i%1024)), tv("0123456789"))
	}
}

func BenchmarkGet(b *testing.B) {
	c := newLRUCache(Options{
		MaxBytes:        1 << 30,
		CleanupInterval: time.Hour,
	})
	defer c.Close()

	for i := 0; i < 10000; i++ {
		_ = c.Set("k"+string(rune(i)), tv("x"))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get("k" + string(rune(i%10000)))
	}
}
