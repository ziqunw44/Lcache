package registry

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

//
// =============== 公共工具 ===============
//

// 覆盖 endpoints，确保 Register() 与测试都走 127.0.0.1:2379（IPv4）
func setLocal127Endpoint(t *testing.T) func() {
	t.Helper()
	old := *DefaultConfig
	DefaultConfig.Endpoints = []string{"127.0.0.1:2379"} // 强制 IPv4，避免 localhost 解析到 ::1
	DefaultConfig.DialTimeout = 5 * time.Second          // 稍微给大一点时间
	return func() { *DefaultConfig = old }
}

// 连接 etcd；连不上就 Skip（不算失败）
func mustEtcdOrSkip(t *testing.T) *clientv3.Client {
	t.Helper()
	ep := "127.0.0.1:2379"
	fmt.Println("🔧 connecting etcd at", ep, "...")

	// 先做一个原生 TCP 探测，快速发现端口是否可达
	d := net.Dialer{Timeout: 2 * time.Second}
	c, err := d.Dial("tcp", ep)
	if err != nil {
		t.Skipf("⚠️ skip: tcp dial %s failed: %v", ep, err)
		return nil
	}
	_ = c.Close()

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{ep},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Skipf("⚠️ skip: cannot create etcd client: %v", err)
		return nil
	}

	// 把 Status 超时拉长到 8s，避免刚启动未就绪
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	fmt.Println("🔧 checking etcd status ...")
	if _, err := cli.Status(ctx, ep); err != nil {
		_ = cli.Close()
		t.Skipf("⚠️ skip: etcd not ready: %v", err)
		return nil
	}

	fmt.Println("✅ etcd connected")
	return cli
}

// 等待直到 key 存在
func waitKeyExists(t *testing.T, cli *clientv3.Client, key string, timeout time.Duration) (val string, ok bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)

	fmt.Printf("🔍 waiting for key to appear: %s\n", key)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		resp, err := cli.Get(ctx, key)
		cancel()

		if err == nil && len(resp.Kvs) > 0 {
			fmt.Println("✅ key found in etcd")
			return string(resp.Kvs[0].Value), true
		}
		time.Sleep(50 * time.Millisecond)
	}
	fmt.Println("❌ key not found within timeout")
	return "", false
}

// 等待直到 key 消失
func waitKeyGone(t *testing.T, cli *clientv3.Client, key string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)

	fmt.Printf("🧽 waiting for key to be deleted: %s\n", key)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		resp, err := cli.Get(ctx, key)
		cancel()

		if err == nil && len(resp.Kvs) == 0 {
			fmt.Println("✅ key deleted from etcd")
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	fmt.Println("❌ key still exists after timeout")
	return false
}

//
// =============== 测试用例 ===============
//

// 1) 传入短地址 :port，应自动补齐本机 IP，并能在 etcd 下创建 key
func TestRegister_ComposesIP_FromShortAddr_AndCreatesKey(t *testing.T) {
	defer setLocal127Endpoint(t)() // 覆盖 DefaultConfig.Endpoints 为 127.0.0.1:2379

	fmt.Println("===== TEST 1: Register short addr :port =====")
	cli := mustEtcdOrSkip(t)
	if cli == nil {
		return
	}
	defer cli.Close()

	svc := fmt.Sprintf("Lcache-it-%d", time.Now().UnixNano())
	localIP, err := getLocalIP()
	if err != nil {
		t.Fatalf("getLocalIP failed: %v", err)
	}
	addr := ":5001"
	expectAddr := localIP + addr
	expectKey := "/services/" + svc + "/" + expectAddr

	fmt.Println("🚀 calling Register()")
	stopCh := make(chan error, 1)
	if err := Register(svc, addr, stopCh); err != nil {
		t.Fatalf("Register error: %v", err)
	}
	t.Cleanup(func() { stopCh <- fmt.Errorf("stop") })

	if _, ok := waitKeyExists(t, cli, expectKey, 5*time.Second); !ok {
		t.Fatalf("key not found: %s", expectKey)
	}

	fmt.Println("✅ TEST PASSED")
}

// 2) 传入完整地址 ip:port，应按原样写入，并能在 etcd 下创建 key
func TestRegister_UsesFullAddr_AndCreatesKey(t *testing.T) {
	defer setLocal127Endpoint(t)()

	fmt.Println("===== TEST 2: Register full ip:port =====")
	cli := mustEtcdOrSkip(t)
	if cli == nil {
		return
	}
	defer cli.Close()

	svc := fmt.Sprintf("Lcache-it-%d", time.Now().UnixNano())
	addr := "10.9.8.7:4321" // 仅当作字符串存入
	expectKey := "/services/" + svc + "/" + addr

	fmt.Println("🚀 calling Register()")
	stopCh := make(chan error, 1)
	if err := Register(svc, addr, stopCh); err != nil {
		t.Fatalf("Register error: %v", err)
	}
	t.Cleanup(func() { stopCh <- fmt.Errorf("stop") })

	if _, ok := waitKeyExists(t, cli, expectKey, 5*time.Second); !ok {
		t.Fatalf("key not found: %s", expectKey)
	}

	fmt.Println("✅ TEST PASSED")
}

// 3) 发 stop 信号后，应撤销租约并使 key 很快消失
func TestRegister_Stop_Revoke_RemovesKey(t *testing.T) {
	defer setLocal127Endpoint(t)()

	fmt.Println("===== TEST 3: stopCh should delete key =====")
	cli := mustEtcdOrSkip(t)
	if cli == nil {
		return
	}
	defer cli.Close()

	svc := fmt.Sprintf("Lcache-it-%d", time.Now().UnixNano())
	localIP, err := getLocalIP()
	if err != nil {
		t.Fatalf("getLocalIP failed: %v", err)
	}
	addr := ":5555"
	expectKey := "/services/" + svc + "/" + localIP + addr

	fmt.Println("🚀 calling Register()")
	stopCh := make(chan error, 1)
	if err := Register(svc, addr, stopCh); err != nil {
		t.Fatalf("Register error: %v", err)
	}

	// 先确认已经创建
	if _, ok := waitKeyExists(t, cli, expectKey, 5*time.Second); !ok {
		t.Fatalf("key not found after Register: %s", expectKey)
	}

	fmt.Println("🛑 sending stop signal")
	stopCh <- fmt.Errorf("stop")

	if !waitKeyGone(t, cli, expectKey, 5*time.Second) {
		t.Fatalf("key still exists after stop: %s", expectKey)
	}

	fmt.Println("✅ TEST PASSED")
}
