package main

import (
	"fmt"
	"go-zk-svc/client"
	"sync"
	"time"
)

type AWatcher struct {
	name string
	wg   *sync.WaitGroup
}

func (a *AWatcher) ProcServiceAddrsChange(svcName string, addrs []string) {
	fmt.Printf("%s watched %s address changed, new addresses as below:\n", a.name, svcName)
	for _, v := range addrs {
		fmt.Println(v)
	}
	if len(addrs) > 0 {
		a.wg.Done()
	}
}

func main() {
	var wg sync.WaitGroup
	// zookeeper server address
	servers := []string{"127.0.0.1:2181"}
	// service A will watch service B
	serviceA := &client.ServiceNode{"serviceA", "127.0.0.1:18001"}
	clientA, err := client.NewClient(servers, "/test", serviceA, []string{"serviceB"}, &AWatcher{name: serviceA.Name, wg: &wg}, 1)
	if err != nil {
		panic(err)
	}
	wg.Add(1)
	defer clientA.Close()

	// ensure service B register after service A
	time.Sleep(time.Second * time.Duration(3))
	serviceB := &client.ServiceNode{"serviceB", "127.0.0.1:18002"}
	clientB, err := client.NewClient(servers, "/test", serviceB, []string{"serviceA"}, nil, 1)
	if err != nil {
		panic(err)
	}
	defer clientB.Close()

	wg.Wait()
}
