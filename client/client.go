package client

import (
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

// Service registration information for znode
type ServiceNode struct {
	Name string // service name
	Addr string // service address(format [ip]:[port])
}

// Interface for service watcher
type ServiceWatcher interface {
	ProcServiceAddrsChange(serviceName string, addrs []string)
}

type serviceInfo struct {
	name    string
	mt      sync.Mutex
	addrs   map[string]bool
	blocker Blocker // used to block watch channel while disconnect from zk
}

// Service register client information
type RegisterClient struct {
	zkServers     []string                // zk server addresses
	zkRoot        string                  // service root path on zk
	conn          *zk.Conn                // zk client connection
	watchServices map[string]*serviceInfo // names of services that should be watched
	watcher       ServiceWatcher          // service watcher
	serviceNode   *ServiceNode            // service information
	eventChan     <-chan zk.Event         // connection event channel
	addrRegexp    *regexp.Regexp          // regexp for address
	closed        bool                    // close client
}

// Create new zk client for service
// If dont register service, param node could be nil
// If dont watch service, watchSves and watcher could be nil
func NewClient(
	zkServers []string,
	zkRoot string,
	node *ServiceNode,
	watchServices []string,
	watcher ServiceWatcher,
	timeout int) (*RegisterClient, error) {
	client := &RegisterClient{
		zkServers:     zkServers,
		zkRoot:        zkRoot,
		serviceNode:   node,
		watcher:       watcher,
		watchServices: make(map[string]*serviceInfo),
	}
	if watchServices != nil && len(watchServices) != 0 {
		for _, v := range watchServices {
			client.watchServices[string(client.zkRoot+"/"+v)] = &serviceInfo{name: v, addrs: make(map[string]bool)}
		}
	}

	var err error
	if client.addrRegexp, err = regexp.Compile(`\(([^)]+)\)`); err != nil {
		return nil, err
	}

	if client.conn, client.eventChan, err = zk.Connect(zkServers, time.Duration(timeout)*time.Second); err != nil {
		return nil, err
	}
	client.watch()
	client.register()

	return client, nil
}

// Close zk connection and remove ephemeral znode
func (s *RegisterClient) Close() {
	s.conn.Close()
	s.closed = true
	// unblock all watch routine
	for _, v := range s.watchServices {
		v.blocker.Unblock()
	}
}

// Register service to zookeeper
// znode format such as "servicename(127.0.0.1:80)"
func (s *RegisterClient) register() {
	if s.serviceNode == nil {
		return
	}
	go func() {
		for {
			event, ok := <-s.eventChan
			if !ok || s.closed {
				break
			}

			if event.Type != zk.EventSession || event.State != zk.StateConnected {
				continue
			}

			// unblock all watch routine
			for _, v := range s.watchServices {
				v.blocker.Unblock()
			}

			err := s.ensurePath(s.zkRoot + "/" + s.serviceNode.Name)
			if err != nil && s.conn.State() == zk.StateConnected {
				panic("client: unexcept error in ensure path")
			}
			path := s.zkRoot + "/" + s.serviceNode.Name + "/" + s.serviceNode.Name + "(" + s.serviceNode.Addr + ")"
			_, err = s.conn.Create(path, []byte(""), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
			if err != nil && err != zk.ErrNodeExists && s.conn.State() == zk.StateConnected {
				panic("client: unexcept error in create protected ephemeral sequential")
			}
		}
	}()
}

// Watch services' children changed
func (s *RegisterClient) watch() {
	if len(s.watchServices) == 0 || s.watcher == nil {
		return
	}

	for path, service := range s.watchServices {
		go s.watchService(path, service)
	}
	return
}

// Watch service children changed
func (s *RegisterClient) watchService(path string, service *serviceInfo) {
	for {
		addrs, _, ch, err := s.conn.ChildrenW(path)
		if err != nil {
			if s.closed {
				break
			} else if err == zk.ErrNoNode {
				err := s.ensurePath(path)
				if err != nil && s.conn.State() == zk.StateConnected {
					panic("client: unexcept error in ensure path")
				}
			} else if s.conn.State() != zk.StateConnected {
				// if disconnected, block and wait reconnect
				service.blocker.Block()
			}
			continue
		}

		if s.checkServiceAddrsChange(path, addrs) {
			s.handleServiceAddrsChange(path, addrs)
		}

		watchEvent, ok := <-ch
		if s.closed {
			break
		} else if !ok {
			continue
		}

		switch watchEvent.Type {
		case zk.EventNodeChildrenChanged:
			addrs, _, err := s.conn.Children(watchEvent.Path)
			if err == nil {
				s.handleServiceAddrsChange(watchEvent.Path, addrs)
			}
		// zk.EventNotWatching identify watch failed
		// go loop do rewatch
		default:
		}
	}
}

// Ensure that service path exists
func (s *RegisterClient) ensurePath(path string) error {
	if err := s.ensureRoot(); err != nil {
		return err
	}

	if exists, _, err := s.conn.Exists(path); err != nil {
		return err
	} else if exists {
		return nil
	}

	_, err := s.conn.Create(path, []byte(""), 0, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		return err
	}
	return nil
}

// Ensure that service root exists
func (s *RegisterClient) ensureRoot() error {
	if s.zkRoot == "/" {
		return nil
	}
	nodes := strings.Split(s.zkRoot, "/")
	root := "/"
	for _, v := range nodes[1:] {
		root += v
		if exists, _, err := s.conn.Exists(root); err != nil {
			return err
		} else if exists {
			continue
		}
		_, err := s.conn.Create(root, []byte(""), 0, zk.WorldACL(zk.PermAll))
		if err != nil && err != zk.ErrNodeExists {
			return err
		}
	}

	return nil
}

// Check service addresses change or not
func (s *RegisterClient) checkServiceAddrsChange(servicePath string, addrs []string) bool {
	service, ok := s.watchServices[servicePath]
	if !ok {
		return false
	}
	service.mt.Lock()
	defer service.mt.Unlock()
	if len(service.addrs) != len(addrs) {
		return true
	}
	for _, v := range addrs {
		if _, ok := service.addrs[v]; !ok {
			return true
		}
	}
	return false
}

// Record new addresses of service and notify watcher
func (s *RegisterClient) handleServiceAddrsChange(servicePath string, addrs []string) {
	service, ok := s.watchServices[servicePath]
	if !ok {
		return
	}
	service.mt.Lock()
	service.addrs = make(map[string]bool)
	for _, v := range addrs {
		service.addrs[v] = true
	}
	service.mt.Unlock()

	var ipAddrs []string
	for _, v := range addrs {
		params := s.addrRegexp.FindStringSubmatch(v)
		if len(params) < 2 {
			continue
		}
		ipAddrs = append(ipAddrs, params[1])
	}

	s.watcher.ProcServiceAddrsChange(service.name, ipAddrs)
}
