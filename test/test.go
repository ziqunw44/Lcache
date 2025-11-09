// 文件路径：test/test.go
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	lcache "Lcache"
)

func main() {
	// 参数：节点端口、节点标识
	port := flag.Int("port", 8001, "节点端口（也是 gRPC 端口）")
	nodeID := flag.String("node", "A", "节点标识符（A/B/C 等）")
	flag.Parse()

	addr := fmt.Sprintf(":%d", *port)
	log.Printf("[节点%s] 启动，地址: %s", *nodeID, addr)

	// 1) 创建并启动 gRPC 节点（会向 etcd 注册本 gRPC 地址）
	node, err := lcache.NewServer(
		addr,
		"lcache",
		lcache.WithEtcdEndpoints([]string{"localhost:2379"}),
		lcache.WithDialTimeout(5*time.Second),
	)
	if err != nil {
		log.Fatal("创建节点失败:", err)
	}

	// 2) 创建节点选择器（用于一致性哈希选址 + gRPC 拨号）
	picker, err := lcache.NewClientPicker(addr, lcache.WithServiceName("lcache"))
	if err != nil {
		log.Fatal("创建节点选择器失败:", err)
	}

	// 3) 创建缓存组，并注入 peers
	group := lcache.NewGroup("test", 2<<20, lcache.GetterFunc(
		func(ctx context.Context, key string) ([]byte, error) {
			log.Printf("[节点%s] 触发数据源加载: key=%s", *nodeID, key)
			return []byte(fmt.Sprintf("节点%s的数据源值", *nodeID)), nil
		}),
	)
	group.RegisterPeers(picker)

	// 4) 启动 gRPC 服务
	go func() {
		log.Printf("[节点%s] 开始启动服务...", *nodeID)
		if err := node.Start(); err != nil {
			log.Fatal("启动节点失败:", err)
		}
	}()

	// 5) 等待本节点完成注册与发现（简单等待，稳定后可换成轮询判断 peers 数量）
	log.Printf("[节点%s] 等待节点注册...", *nodeID)
	time.Sleep(5 * time.Second)

	ctx := context.Background()

	// 6) 本节点写入一个本地键值
	localKey := fmt.Sprintf("key_%s", *nodeID)
	localValue := []byte(fmt.Sprintf("这是节点%s的数据", *nodeID))

	fmt.Printf("\n=== 节点%s：设置本地数据 ===\n", *nodeID)
	if err := group.Set(ctx, localKey, localValue); err != nil {
		log.Fatal("设置本地数据失败:", err)
	}
	fmt.Printf("节点%s: 设置键 %s 成功\n", *nodeID, localKey)

	// 再等一会儿，给其他节点时间完成启动与注册
	log.Printf("[节点%s] 等待其他节点准备就绪...", *nodeID)
	time.Sleep(30 * time.Second)

	// 7) 打印当前已发现的节点
	picker.PrintPeers()

	// 8) 读取本地键，验证本地命中
	fmt.Printf("\n=== 节点%s：获取本地数据 ===\n", *nodeID)
	fmt.Printf("直接查询本地缓存...\n")
	stats := group.Stats()
	fmt.Printf("缓存统计: %+v\n", stats)
	if val, err := group.Get(ctx, localKey); err == nil {
		fmt.Printf("节点%s: 获取本地键 %s 成功: %s\n", *nodeID, localKey, val.String())
	} else {
		fmt.Printf("节点%s: 获取本地键失败: %v\n", *nodeID, err)
	}

	// 9) 测试跨节点读取（读取其他两个节点的键）
	otherKeys := []string{"key_A", "key_B", "key_C"}
	for _, key := range otherKeys {
		if key == localKey {
			continue // 跳过本节点的键
		}
		fmt.Printf("\n=== 节点%s：尝试获取远程数据 %s ===\n", *nodeID, key)
		log.Printf("[节点%s] 开始查找键 %s 的远程节点", *nodeID, key)
		if val, err := group.Get(ctx, key); err == nil {
			fmt.Printf("节点%s: 获取远程键 %s 成功: %s\n", *nodeID, key, val.String())
		} else {
			fmt.Printf("节点%s: 获取远程键失败: %v\n", *nodeID, err)
		}
	}

	// 10) 测试删除与再次读取（验证传播/回源）
	// delKey := "key_A"
	// fmt.Printf("\n=== 节点%s：尝试删除 %s 并再次读取 ===\n", *nodeID, delKey)
	// if err := group.Delete(ctx, delKey); err != nil {
	// 	fmt.Printf("节点%s: 删除 %s 失败: %v\n", *nodeID, delKey, err)
	// } else {
	// 	fmt.Printf("节点%s: 删除 %s 成功\n", *nodeID, delKey)
	// }

	// // 删除后再次读取，可能回源或 miss（视你的 Getter 实现而定）
	// if val, err := group.Get(ctx, delKey); err == nil {
	// 	fmt.Printf("节点%s: 删除后再次读取 %s -> %s（可能回源到 Getter）\n", *nodeID, delKey, val.String())
	// } else {
	// 	fmt.Printf("节点%s: 删除后读取 %s 失败: %v\n", *nodeID, delKey, err)
	// }

	// 11) 常驻
	select {}
}
