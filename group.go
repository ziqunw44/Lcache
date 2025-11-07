package Lcache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

// ErrKeyRequired 键不能为空错误
var ErrKeyRequired = errors.New("key is required")

// ErrValueRequired 值不能为空错误
var ErrValueRequired = errors.New("value is required")

// ErrGroupClosed 组已关闭错误
var ErrGroupClosed = errors.New("cache group is closed")


var (
	groupsMu sync.RWMutex
	groups   = make(map[string]*Group)
)

type Group struct {
	name       string
	mainCache  *Cache
	expiration time.Duration // 缓存过期时间，0表示永不过期
	closed     int32         // 原子变量，标记组是否已关闭
	stats      groupStats    // 统计信息
}

type groupStats struct {
	loads        int64 // 加载次数
	localHits    int64 // 本地缓存命中次数
	localMisses  int64 // 本地缓存未命中次数
	peerHits     int64 // 从对等节点获取成功次数
	peerMisses   int64 // 从对等节点获取失败次数
	loaderHits   int64 // 从加载器获取成功次数
	loaderErrors int64 // 从加载器获取失败次数
	loadDuration int64 // 加载总耗时（纳秒）
}

func NewGroup(name string, cacheBytes int64, opts ...GroupOption) *Group {
	// if getter == nil {
	// 	panic("nil Getter")
	// }

	// 创建默认缓存选项
	cacheOpts := DefaultCacheOptions()
	cacheOpts.MaxBytes = cacheBytes

	g := &Group{
		name:      name,
		// getter:    getter,
		mainCache: NewCache(cacheOpts),
		// loader:    &singleflight.Group{},
	}

	// 应用选项
	for _, opt := range opts {
		opt(g)
	}

	// 注册到全局组映射
	groupsMu.Lock()
	defer groupsMu.Unlock()

	if _, exists := groups[name]; exists {
		logrus.Warnf("Group with name %s already exists, will be replaced", name)
	}

	groups[name] = g
	logrus.Infof("Created cache group [%s] with cacheBytes=%d, expiration=%v", name, cacheBytes, g.expiration)

	return g
}

type GroupOption func(*Group)

func WithExpiration(d time.Duration) GroupOption {
	return func(g *Group) {
		g.expiration = d
	}
}

func GetGroup(name string) *Group {
	groupsMu.RLock()
	defer groupsMu.RUnlock()
	return groups[name]
}

func (g *Group) Get(ctx context.Context, key string) (ByteView, error) {
	if atomic.LoadInt32(&g.closed) == 1 {
		return ByteView{}, ErrGroupClosed
	}

	if key == "" {
		return ByteView{}, ErrKeyRequired
	}

	view, ok := g.mainCache.Get(ctx, key)
	if ok {
		atomic.AddInt64(&g.stats.localHits, 1)
		return view, nil
	}

	atomic.AddInt64(&g.stats.localMisses, 1)

	return g.load(ctx, key)
}

// Set 设置缓存值
func (g *Group) Set(ctx context.Context, key string, value []byte) error {
	// 检查组是否已关闭
	if atomic.LoadInt32(&g.closed) == 1 {
		return ErrGroupClosed
	}

	if key == "" {
		return ErrKeyRequired
	}
	if len(value) == 0 {
		return ErrValueRequired
	}

	// 检查是否是从其他节点同步过来的请求
	// isPeerRequest := ctx.Value("from_peer") != nil

	// 创建缓存视图
	view := ByteView{b: cloneBytes(value)}

	// 设置到本地缓存
	if g.expiration > 0 {
		g.mainCache.AddWithExpiration(key, view, time.Now().Add(g.expiration))
	} else {
		g.mainCache.Add(key, view)
	}

	// 如果不是从其他节点同步过来的请求，且启用了分布式模式，同步到其他节点
	// if !isPeerRequest && g.peers != nil {
	// 	go g.syncToPeers(ctx, "set", key, value)
	// }

	return nil
}

// Delete 删除缓存值
func (g *Group) Delete(ctx context.Context, key string) error {
	// 检查组是否已关闭
	if atomic.LoadInt32(&g.closed) == 1 {
		return ErrGroupClosed
	}

	if key == "" {
		return ErrKeyRequired
	}

	// 从本地缓存删除
	g.mainCache.Delete(key)

	// 检查是否是从其他节点同步过来的请求
	// isPeerRequest := ctx.Value("from_peer") != nil

	// 如果不是从其他节点同步过来的请求，且启用了分布式模式，同步到其他节点
	// if !isPeerRequest && g.peers != nil {
	// 	go g.syncToPeers(ctx, "delete", key, nil)
	// }

	return nil
}

// Clear 清空缓存
func (g *Group) Clear() {
	// 检查组是否已关闭
	if atomic.LoadInt32(&g.closed) == 1 {
		return
	}

	g.mainCache.Clear()
	logrus.Infof("LCache cleared cache for group [%s]", g.name)
}

// Close 关闭组并释放资源
func (g *Group) Close() error {
	// 如果已经关闭，直接返回
	if !atomic.CompareAndSwapInt32(&g.closed, 0, 1) {
		return nil
	}

	// 关闭本地缓存
	if g.mainCache != nil {
		g.mainCache.Close()
	}

	// 从全局组映射中移除
	groupsMu.Lock()
	delete(groups, g.name)
	groupsMu.Unlock()

	logrus.Infof("LCache closed cache group [%s]", g.name)
	return nil
}

func (g *Group) Stats() map[string]interface{} {
	stats := map[string]interface{}{
		"name":          g.name,
		"closed":        atomic.LoadInt32(&g.closed) == 1,
		"expiration":    g.expiration,
		"loads":         atomic.LoadInt64(&g.stats.loads),
		"local_hits":    atomic.LoadInt64(&g.stats.localHits),
		"local_misses":  atomic.LoadInt64(&g.stats.localMisses),
		"peer_hits":     atomic.LoadInt64(&g.stats.peerHits),
		"peer_misses":   atomic.LoadInt64(&g.stats.peerMisses),
		"loader_hits":   atomic.LoadInt64(&g.stats.loaderHits),
		"loader_errors": atomic.LoadInt64(&g.stats.loaderErrors),
	}

	// 计算各种命中率
	totalGets := stats["local_hits"].(int64) + stats["local_misses"].(int64)
	if totalGets > 0 {
		stats["hit_rate"] = float64(stats["local_hits"].(int64)) / float64(totalGets)
	}

	totalLoads := stats["loads"].(int64)
	if totalLoads > 0 {
		stats["avg_load_time_ms"] = float64(atomic.LoadInt64(&g.stats.loadDuration)) / float64(totalLoads) / float64(time.Millisecond)
	}

	// 添加缓存大小
	if g.mainCache != nil {
		cacheStats := g.mainCache.Stats()
		for k, v := range cacheStats {
			stats["cache_"+k] = v
		}
	}

	return stats
}

// ListGroups 返回所有缓存组的名称
func ListGroups() []string {
	groupsMu.RLock()
	defer groupsMu.RUnlock()

	names := make([]string, 0, len(groups))
	for name := range groups {
		names = append(names, name)
	}

	return names
}

// DestroyGroup 销毁指定名称的缓存组
func DestroyGroup(name string) bool {
	groupsMu.Lock()
	defer groupsMu.Unlock()

	if g, exists := groups[name]; exists {
		g.Close()
		delete(groups, name)
		logrus.Infof("LCache destroyed cache group [%s]", name)
		return true
	}

	return false
}

// DestroyAllGroups 销毁所有缓存组
func DestroyAllGroups() {
	groupsMu.Lock()
	defer groupsMu.Unlock()

	for name, g := range groups {
		g.Close()
		delete(groups, name)
		logrus.Infof("LCache destroyed cache group [%s]", name)
	}
}