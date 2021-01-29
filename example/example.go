package main

import (
	"fmt"
	"github.com/difficultwork/go-zk-svc/client"
	"sync"
	"time"
)

type AWatcher struct {
	name string
	wg   *sync.WaitGroup
}

type processor struct{}

func (p *processor) ProcChildrenChange(nodeName string, subNodeNames []string) {
	fmt.Printf("name: %s, children: %v\n", nodeName, subNodeNames)
}

func main() {
	var wg sync.WaitGroup
	wg.Add(1)
	// zookeeper server address
	clientA, := client.NewClient([]string{"127.0.0.1:18001"}, 10)
	defer clientA.Close()
	clientA.Register("/test", "child1", []byte("this is a test"))

	// ensure service B register after service A
	time.Sleep(time.Second * time.Duration(3))
	clientB := client.NewClient([]string{"127.0.0.1:18001"}, 10)
	defer clientB.Close()
	clientB.Watch("/", "test", p)
	time.Sleep(time.Second * time.Duration(2))
	clientA.Unregister()
	time.Sleep(time.Second * time.Duration(20))
	wg.Done()
	wg.Wait()
}
