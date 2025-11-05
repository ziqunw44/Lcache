package consistenthash

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)
type Map struct {
	mu sync.RWMutex
	config *Config
	keys []int
	hashMap map[int]string
	nodeReplicas map[string]int
	nodeCounts map[string]*int64
	totalRequests int64
}

func New(opts ...Option) *Map {
	m := &Map{
		config:       DefaultConfig,
		hashMap:      make(map[int]string),
		nodeReplicas: make(map[string]int),
		nodeCounts:   make(map[string]*int64),
	}

	for _, opt := range opts {
		opt(m)
	}

	m.startBalancer()
	return m
}


type Option func(*Map)


func WithConfig(config *Config) Option {
	return func(m *Map) {
		m.config = config
	}
}

func (m *Map) Add(nodes ...string) error {
	if len(nodes) == 0 {
		return errors.New("no nodes provided")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, node := range nodes {
		if node == "" {
			continue
		}

		m.addNode(node, m.config.DefaultReplicas)
	}

	sort.Ints(m.keys)
	return nil
}

func (m *Map) Remove(node string) error {
	if node == "" {
		return errors.New("invalid node")
	}


	replicas := m.nodeReplicas[node]
	if replicas == 0 {
		return fmt.Errorf("node %s not found", node)
	}

	for i := 0; i < replicas; i++ {
		hash := int(m.config.HashFunc([]byte(fmt.Sprintf("%s-%d", node, i))))
		delete(m.hashMap, hash)
		for j := 0; j < len(m.keys); j++ {
			if m.keys[j] == hash {
				m.keys = append(m.keys[:j], m.keys[j+1:]...)
				break
			}
		}
	}

	delete(m.nodeReplicas, node)
	delete(m.nodeCounts, node)
	return nil
}

func (m *Map) Get(key string) (string,bool) {
	if key == "" {
		return "",false
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.keys) == 0 {
		return "",false
	}

	hash := int(m.config.HashFunc([]byte(key)))

	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})

	if idx == len(m.keys) {
		idx = 0
	}

	node := m.hashMap[m.keys[idx]]
	count := m.nodeCounts[node]
	atomic.AddInt64(count, 1) 
	atomic.AddInt64(&m.totalRequests, 1)

	return node,true
}

func (m *Map) addNode(node string, replicas int) {
	for i := 0; i < replicas; i++ {
		hash := int(m.config.HashFunc([]byte(fmt.Sprintf("%s-%d", node, i))))
		m.keys = append(m.keys, hash)
		m.hashMap[hash] = node
	}
	m.nodeReplicas[node] = replicas
	if _, ok := m.nodeCounts[node]; !ok || m.nodeCounts[node] == nil {
		var z int64
		m.nodeCounts[node] = &z
	}
}

func (m *Map) checkAndRebalance() {
	if atomic.LoadInt64(&m.totalRequests) < 1000 {
		return 
	}
	m.mu.RLock()
	avgLoad := float64(atomic.LoadInt64(&m.totalRequests)) / float64(len(m.nodeReplicas))
	var maxDiff float64

	for _, count := range m.nodeCounts {
		diff := math.Abs(float64(atomic.LoadInt64(count)) - avgLoad)
		if diff/avgLoad > maxDiff {
			maxDiff = diff / avgLoad
		}
	}
	m.mu.RUnlock()

	if maxDiff > m.config.LoadBalanceThreshold {
		m.rebalanceNodes()
	}
}


func (m *Map) rebalanceNodes() {
	m.mu.Lock()
	defer m.mu.Unlock()

	avgLoad := float64(atomic.LoadInt64(&m.totalRequests)) / float64(len(m.nodeReplicas))

	for node, count := range m.nodeCounts {
		currentReplicas := m.nodeReplicas[node]
		loadRatio := float64(atomic.LoadInt64(count)) / avgLoad

		var newReplicas int
		if loadRatio > 1 {
		
			newReplicas = int(float64(currentReplicas) / loadRatio)
		} else {
		
			newReplicas = int(float64(currentReplicas) * (2 - loadRatio))
		}


		if newReplicas < m.config.MinReplicas {
			newReplicas = m.config.MinReplicas
		}
		if newReplicas > m.config.MaxReplicas {
			newReplicas = m.config.MaxReplicas
		}

		if newReplicas != currentReplicas {
	
			if err := m.Remove(node); err != nil {
				continue
			}
			m.addNode(node, newReplicas)
		}
	}


	for node := range m.nodeCounts {
		if p := m.nodeCounts[node]; p != nil {
			atomic.StoreInt64(p, 0)
		}
	}
	atomic.StoreInt64(&m.totalRequests, 0)


	sort.Ints(m.keys)
}


func (m *Map) GetStats() map[string]float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := make(map[string]float64)
	total := atomic.LoadInt64(&m.totalRequests)
	if total == 0 {
		return stats
	}

	for node, count := range m.nodeCounts {
		stats[node] =  float64(atomic.LoadInt64(count) )  / float64(total)
	}
	return stats
}


func (m *Map) startBalancer() {
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for range ticker.C {
			m.checkAndRebalance()
		}
	}()
}
