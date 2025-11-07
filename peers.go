import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"LCache/consistenthash"
	"LCache/registry"
	clientv3 "go.etcd.io/etcd/client/v3"
)