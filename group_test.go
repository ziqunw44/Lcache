package Lcache

import (
	"context"
	"sync"
	"testing"
	"time"
)

// 简单 getter：返回 key 作为值
func echoGetter() Getter {
	return GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return []byte("V:" + key), nil
	})
}

func TestGroup_Set_Get_LocalHit(t *testing.T) {
	defer DestroyAllGroups()

	g := NewGroup("g_basic", 8<<20, echoGetter(), WithExpiration(0))
	ctx := context.Background()

	// 直接 Set，然后 Get 本地命中
	if err := g.Set(ctx, "k1", []byte("v1")); err != nil {
		t.Fatalf("Set error: %v", err)
	}
	v, err := g.Get(ctx, "k1")
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if string(v.b) != "v1" {
		t.Fatalf("unexpected value: %q", v.b)
	}
	st := g.Stats()
	if st["local_hits"].(int64) != 1 || st["local_misses"].(int64) != 0 {
		t.Fatalf("unexpected local hits/misses: %v/%v", st["local_hits"], st["local_misses"])
	}
}

func TestGroup_Get_Miss_LoadFromGetter_WriteBack(t *testing.T) {
	defer DestroyAllGroups()

	g := NewGroup("g_load", 8<<20, echoGetter())
	ctx := context.Background()

	// 不 Set，直接 Get → miss → 调用 getter → 写回缓存
	v, err := g.Get(ctx, "abc")
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if string(v.b) != "V:abc" {
		t.Fatalf("unexpected loaded value: %q", v.b)
	}
	// 再次 Get，应本地命中
	v2, err := g.Get(ctx, "abc")
	if err != nil {
		t.Fatalf("Get2 error: %v", err)
	}
	if string(v2.b) != "V:abc" {
		t.Fatalf("unexpected cached value: %q", v2.b)
	}

	st := g.Stats()
	if st["local_hits"].(int64) < 1 {
		t.Fatalf("expected local hits >= 1, got %v", st["local_hits"])
	}
	if st["loader_hits"].(int64) != 1 {
		t.Fatalf("expected loader_hits=1, got %v", st["loader_hits"])
	}
	if st["loads"].(int64) != 1 {
		t.Fatalf("expected loads=1, got %v", st["loads"])
	}
}

func TestGroup_Expiration(t *testing.T) {
	defer DestroyAllGroups()

	g := NewGroup("g_exp", 8<<20, echoGetter(), WithExpiration(50*time.Millisecond))
	ctx := context.Background()

	// 先回源写回
	if _, err := g.Get(ctx, "x"); err != nil {
		t.Fatalf("Get error: %v", err)
	}
	// 立刻命中
	if _, err := g.Get(ctx, "x"); err != nil {
		t.Fatalf("Get error: %v", err)
	}
	time.Sleep(80 * time.Millisecond)
	// 过期后再次 Get，应再次触发回源
	if _, err := g.Get(ctx, "x"); err != nil {
		t.Fatalf("Get after expire error: %v", err)
	}
	st := g.Stats()
	if st["loads"].(int64) < 2 {
		t.Fatalf("expected loads >=2 after expiration, got %v", st["loads"])
	}
}

func TestGroup_Delete_Clear_Close(t *testing.T) {
	defer DestroyAllGroups()

	g := NewGroup("g_del", 8<<20, echoGetter())
	ctx := context.Background()

	// Set → Delete → miss 回源
	if err := g.Set(ctx, "k", []byte("v")); err != nil {
		t.Fatalf("Set error: %v", err)
	}
	if err := g.Delete(ctx, "k"); err != nil {
		t.Fatalf("Delete error: %v", err)
	}
	v, err := g.Get(ctx, "k")
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if string(v.b) != "V:k" {
		t.Fatalf("unexpected value after delete & load: %q", v.b)
	}

	// Clear → miss 回源
	g.Clear()
	v2, err := g.Get(ctx, "k2")
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if string(v2.b) != "V:k2" {
		t.Fatalf("unexpected value after clear & load: %q", v2.b)
	}

	// Close 后操作
	if err := g.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}
	if _, err := g.Get(ctx, "z"); err == nil {
		t.Fatalf("expected error after close")
	}
	if err := g.Set(ctx, "z", []byte("1")); err == nil {
		t.Fatalf("expected error on Set after close")
	}
}

func TestGroup_List_Destroy(t *testing.T) {
	defer DestroyAllGroups()
	_ = NewGroup("g1", 8<<20, echoGetter())
	_ = NewGroup("g2", 8<<20, echoGetter())

	names := ListGroups()
	if len(names) < 2 {
		t.Fatalf("expected >=2 groups, got %v", names)
	}
	if !DestroyGroup("g1") {
		t.Fatalf("DestroyGroup g1 failed")
	}
	names2 := ListGroups()
	for _, n := range names2 {
		if n == "g1" {
			t.Fatalf("g1 should be destroyed")
		}
	}
}

func TestGroup_Concurrent_Get_Set_Delete(t *testing.T) {
	defer DestroyAllGroups()

	g := NewGroup("g_conc", 64<<20, echoGetter(), WithExpiration(0))
	ctx := context.Background()

	var wg sync.WaitGroup
	readers, writers, deleters := 16, 8, 4
	wg.Add(readers + writers + deleters)

	for i := 0; i < writers; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 1500; j++ {
				_ = g.Set(ctx, keyOf(id*10000+j), []byte("v"))
			}
		}(i)
	}
	for i := 0; i < deleters; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 800; j++ {
				_ = g.Delete(ctx, keyOf(id*10000+j))
			}
		}(i)
	}
	for i := 0; i < readers; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 3000; j++ {
				_, _ = g.Get(ctx, keyOf(id*7+j*
