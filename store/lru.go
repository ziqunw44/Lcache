package store

import (
	"container/list"
	"sync"
	"time"
)

type lruCache struct {
	mu              sync.RWMutex
	list            *list.List               // 双向链表，用于维护 LRU 顺序
	items           map[string]*list.Element // 键到链表节点的映射
	expires         map[string]time.Time     // 过期时间映射
	maxBytes        int64                    // 最大允许字节数
	usedBytes       int64                    // 当前使用的字节数
	onEvicted       func(key string, value Value)
	cleanupInterval time.Duration
	cleanupTicker   *time.Ticker
	closeCh         chan struct{} // 用于优雅关闭清理协程
	closeOnce 		sync.Once
}

type lruEntry struct {
	key   string
	value Value
}

func newLRUCache(opts Options) *lruCache {
	cleanupInterval := opts.CleanupInterval
	if cleanupInterval <= 0 {
		cleanupInterval = time.Minute
	}

	c := &lruCache{
		list:            list.New(),
		items:           make(map[string]*list.Element),
		expires:         make(map[string]time.Time),
		maxBytes:        opts.MaxBytes,
		onEvicted:       opts.OnEvicted,
		cleanupInterval: cleanupInterval,
		closeCh:         make(chan struct{}),
	}

	c.cleanupTicker = time.NewTicker(c.cleanupInterval)
	go c.cleanupLoop()

	return c
}

func (c *lruCache) Get(key string) (Value, bool) {
	c.mu.RLock()
	elem, ok := c.items[key]
	if !ok {
		c.mu.RUnlock()
		return nil, false
	}
    expTime, hasExp := c.expires[key]
    c.mu.RUnlock()
	// 检查是否过期
	if hasExp && time.Now().After(expTime) {

		// 异步删除过期项，避免在读锁内操作
		c.Delete(key)

		return nil, false
	}
    c.mu.Lock()
    defer c.mu.Unlock()
	// 获取值并释放读锁
	entry := elem.Value.(*lruEntry)
	value := entry.value

	// 更新 LRU 位置需要写锁
	// 再次检查元素是否仍然存在（可能在获取写锁期间被其他协程删除）
	if elem2, ok := c.items[key]; ok {
		c.list.MoveToBack(elem2)
	}

	return value, true
}

func (c *lruCache) Delete(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		c.removeElement(elem)
		return true
	}
	return false
}

// removeElement 从缓存中删除元素，调用此方法前必须持有锁
func (c *lruCache) removeElement(elem *list.Element) {
	entry := elem.Value.(*lruEntry)
	c.list.Remove(elem)
	delete(c.items, entry.key)
	delete(c.expires, entry.key)
	c.usedBytes -= int64(len(entry.key) + entry.value.Len())

	if c.onEvicted != nil {
		c.onEvicted(entry.key, entry.value)
	}
}

func (c *lruCache) Set(key string, value Value) error {
	return c.SetWithExpiration(key, value, 0)
}

// SetWithExpiration 添加或更新缓存项，并设置过期时间
func (c *lruCache) SetWithExpiration(key string, value Value, expiration time.Duration) error {
	if value == nil {
		c.Delete(key)
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// 计算过期时间
	var expTime time.Time
	if expiration > 0 {
		expTime = time.Now().Add(expiration)
		c.expires[key] = expTime
	} else {
		delete(c.expires, key)
	}

	// 如果键已存在，更新值
	if elem, ok := c.items[key]; ok {
		oldEntry := elem.Value.(*lruEntry)
		c.usedBytes += int64(value.Len() - oldEntry.value.Len())
		oldEntry.value = value
		c.list.MoveToBack(elem)
		return nil
	}

	// 添加新项
	entry := &lruEntry{key: key, value: value}
	elem := c.list.PushBack(entry)
	c.items[key] = elem
	c.usedBytes += int64(len(key) + value.Len())

	// 检查是否需要淘汰旧项
	c.evict()

	return nil
}

func (c *lruCache) evict() {
	// 先清理过期项
	now := time.Now()
	for key, expTime := range c.expires {
		if now.After(expTime) {
			if elem, ok := c.items[key]; ok {
				c.removeElement(elem)
			}
		}
	}

	// 再根据内存限制清理最久未使用的项
	for c.maxBytes > 0 && c.usedBytes > c.maxBytes && c.list.Len() > 0 {
		elem := c.list.Front() // 获取最久未使用的项（链表头部）
		if elem != nil {
			c.removeElement(elem)
		}
	}
}

func (c *lruCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 如果设置了回调函数，遍历所有项调用回调
	if c.onEvicted != nil {
		for _, elem := range c.items {
			entry := elem.Value.(*lruEntry)
			c.onEvicted(entry.key, entry.value)
		}
	}

	c.list.Init()
	c.items = make(map[string]*list.Element)
	c.expires = make(map[string]time.Time)
	c.usedBytes = 0
}

func (c *lruCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.list.Len()
}

func (c *lruCache) cleanupLoop() {
	for {
		select {
		case <-c.cleanupTicker.C:
			c.mu.Lock()
			c.evict()
			c.mu.Unlock()
		case <-c.closeCh:
			return
		}
	}
}

// Close 关闭缓存，停止清理协程
func (c *lruCache) Close() {
    c.closeOnce.Do(func() {
        if c.cleanupTicker != nil {
            c.cleanupTicker.Stop()
        }
        close(c.closeCh)
    })
}

func (c *lruCache) GetWithExpiration(key string) (Value, time.Duration, bool) {
    now := time.Now()

    // 1) 读锁：读 items 和 expires，并只作“判断”，不操作结构
    c.mu.RLock()
    _, ok := c.items[key]
    if !ok {
        c.mu.RUnlock()
        return nil, 0, false
    }
    expTime, hasExp := c.expires[key]
    expired := hasExp && now.After(expTime)
    c.mu.RUnlock()

    if expired {
        // 2) 已过期：同步删除，返回
        c.Delete(key)
        return nil, 0, false
    }

    // 3) 写锁：重新查表、移动到队尾、在锁内拷贝 value；TTL 也在锁内根据“当前的 expires”计算一次
    c.mu.Lock()
    defer c.mu.Unlock()

    elem2, ok := c.items[key]
    if !ok {
        return nil, 0, false
    }
    c.list.MoveToBack(elem2)
    val := elem2.Value.(*lruEntry).value

    var ttl time.Duration
    if et, ok := c.expires[key]; ok {
        ttl = time.Until(et)
        if ttl < 0 {
            // 极端并发下可能刚好到期
            // 这里也可以选择解锁后 Delete 再返回 false，但简单起见置 0 返回 true
            ttl = 0
        }
    }
    return val, ttl, true
}


func (c *lruCache) GetExpiration(key string) (time.Time, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	expTime, ok := c.expires[key]
	return expTime, ok
}

func (c *lruCache) UpdateExpiration(key string, expiration time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.items[key]; !ok {
		return false
	}

	if expiration > 0 {
		c.expires[key] = time.Now().Add(expiration)
	} else {
		delete(c.expires, key)
	}

	return true
}

func (c *lruCache) UsedBytes() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.usedBytes
}

// MaxBytes 返回最大允许字节数
func (c *lruCache) MaxBytes() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.maxBytes
}

// SetMaxBytes 设置最大允许字节数并触发淘汰
func (c *lruCache) SetMaxBytes(maxBytes int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.maxBytes = maxBytes
	if maxBytes > 0 {
		c.evict()
	}
}
