package Lcache

// var ErrGroupClosed = errors.New("cache group is closed")

// var (
// 	groupsMu sync.RWMutex
// 	groups   = make(map[string]*Group)
// )

// type Group struct {
// 	name       string
// 	expiration time.Duration
// 	stats      groupStats 
// 	close      int32
// }

// type groupStats struct {

// }

// type GroupOption func(*Group)

// func WithExpiration(d time.Duration) GroupOption {
// 	return func(g *Group) {
// 		g.expiration = d
// 	}
// }

// func GetGroup(name string) *Group {
// 	groupsMu.RLock()
// 	defer groupsMu.RUnlock()
// 	return groups[name]
// }

// func (g *Group) Get(ctx context.Context, key string) (ByteView, error) {
// 	if atomic.LoadInt32(&g.closed) == 1 {
// 		return ByteView{}, ErrGroupClosed
// 	}

// 	if key == "" {
// 		return ByteView{}, ErrKeyRequired
// 	}

// 	view, ok := g.mainCache.Get(ctx, key)
// 	if ok {
// 		atomic.AddInt64(&g.stats.localHits, 1)
// 		return view, nil
// 	}

// 	atomic.AddInt64(&g.stats.localMisses, 1)

// 	return g.load(ctx, key)
// }