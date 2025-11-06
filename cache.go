package Lcache

// import (
// 	"context"
// 	"sync"
// 	"sync/atomic"
// 	"time"

// )


// type Cache struct {
// 	mu          sync.RWMutex
// 	store       store.Store  // 底层存储实现
// 	opts        CacheOptions // 缓存配置选项
// 	hits        int64        // 缓存命中次数
// 	misses      int64        // 缓存未命中次数
// 	initialized int32        // 原子变量，标记缓存是否已初始化
// 	closed      int32        // 原子变量，标记缓存是否已关闭
// }

// type CacheOptions struct {
// 	CacheType    store.CacheType                     // 缓存类型: LRU, LRU2 等
// 	MaxBytes     int64                               // 最大内存使用量
// 	BucketCount  uint16                              // 缓存桶数量 (用于 LRU2)
// 	CapPerBucket uint16                              // 每个缓存桶的容量 (用于 LRU2)
// 	Level2Cap    uint16                              // 二级缓存桶的容量 (用于 LRU2)
// 	CleanupTime  time.Duration                       // 清理间隔
// 	OnEvicted    func(key string, value store.Value) // 驱逐回调
// }

// func DefaultCacheOptions() CacheOptions {
// 	return CacheOptions{
// 		CacheType:    store.LRU2,
// 		MaxBytes:     8 * 1024 * 1024, // 8MB
// 		BucketCount:  16,
// 		CapPerBucket: 512,
// 		Level2Cap:    256,
// 		CleanupTime:  time.Minute,
// 		OnEvicted:    nil,
// 	}
// }

// func NewCache(opts CacheOptions) *Cache {
// 	return &Cache{
// 		opts: opts,
// 	}
// }

// func (c *Cache) ensureInitialized() {
// 	// 快速检查缓存是否已初始化，避免不必要的锁争用
// 	if atomic.LoadInt32(&c.initialized) == 1 {
// 		return
// 	}

// 	// 双重检查锁定模式
// 	c.mu.Lock()
// 	defer c.mu.Unlock()

// 	if c.initialized == 0 {
// 		// 创建存储选项
// 		storeOpts := store.Options{
// 			MaxBytes:        c.opts.MaxBytes,
// 			BucketCount:     c.opts.BucketCount,
// 			CapPerBucket:    c.opts.CapPerBucket,
// 			Level2Cap:       c.opts.Level2Cap,
// 			CleanupInterval: c.opts.CleanupTime,
// 			OnEvicted:       c.opts.OnEvicted,
// 		}

// 		// 创建存储实例
// 		c.store = store.NewStore(c.opts.CacheType, storeOpts)

// 		// 标记为已初始化
// 		atomic.StoreInt32(&c.initialized, 1)

// 		logrus.Infof("Cache initialized with type %s, max bytes: %d", c.opts.CacheType, c.opts.MaxBytes)
// 	}
// }