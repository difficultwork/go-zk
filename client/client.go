
import (
	"strings"
	"sync"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

// Interface for node watching processor
type WatchProcessor interface {
	ProcChildrenChange(nodeName string, subNodeNames []string)
}

type Client interface {
	Register(root, nodeName string, data []byte)
	Unregister()
	Watch(root, nodeName string, processor WatchProcessor)
	Unwatch(root, nodeName string)
	Get(path string) ([]byte, error)
	GetChildren(path string) ([]string, error)
	Close()
}

type watcherInfo struct {
	path      string
	name      string
	mt        sync.Mutex
	children  map[string]bool
	processor WatchProcessor
	blocker   Blocker   // used to block watch channel while disconnect from zk
	close     chan bool // close client
	closed    bool
}

// client information
type client struct {
	zkAddrs      []string        // zk server addresses
	zkRoot       string          // service root path on zk
	conn         *zk.Conn        // zk client connection
	watchers     sync.Map        // names of services that should be watched
	eventChan    <-chan zk.Event // connection event channel
	timeout      time.Duration   //
	unregister   chan bool       // register client
	registerPath string          //
}

// Create new zk client
func NewClient(zkAddr []string, timeout int) (Client, error) {
	c := &client{
		zkAddrs: zkAddr,
		timeout: time.Duration(timeout) * time.Second}

	var err error
	if c.conn, c.eventChan, err = zk.Connect(c.zkAddrs, time.Duration(timeout)*time.Second); err != nil {
		return nil, err
	}
	return c, nil
}

// Close zk connection and remove ephemeral znode
func (c *client) Close() {
	c.conn.Close()
	if c.unregister != nil {
		close(c.unregister)
	}
	// unblock all watch routine
	c.watchers.Range(func(k, v interface{}) bool {
		w, ok := v.(*watcherInfo)
		if ok {
			close(w.close)
			w.blocker.Unblock()
		}
		return true
	})
}

// Register node to zookeeper
func (c *client) Register(root, nodeName string, data []byte) {
	if c.unregister != nil {
		return
	}
	c.registerPath = getPath(root, nodeName)
	c.unregister = make(chan bool)
	go func() {
		defer func() {
			c.unregister = nil
		}()
		for {
			var event zk.Event
			var ok bool
			select {
			case event, ok = <-c.eventChan:
				if !ok {
					return
				}
			case <-c.unregister:
				return
			}

			if event.Type != zk.EventSession || event.State != zk.StateConnected {
				continue
			}

			c.watchers.Range(func(k, v interface{}) bool {
				w, ok := v.(*watcherInfo)
				if ok {
					w.blocker.Unblock()
				}
				return true
			})

			err := c.ensurePath(root)
			if err != nil && c.conn.State() == zk.StateConnected {
				panic("client: unexcept error in ensure path")
			}
			_, err = c.conn.Create(c.registerPath, data, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
			if err != nil && err != zk.ErrNodeExists && c.conn.State() == zk.StateConnected {
				panic("client: unexcept error in create protected ephemeral sequential")
			}
		}
	}()
}

func (c *client) Unregister() {
	if c.unregister == nil {
		return
	}
	close(c.unregister)
	_, sate, err := c.conn.Get(c.registerPath)
	if err != nil {
		return
	}
	c.conn.Delete(c.registerPath, sate.Version)
}

func (c *client) Get(path string) ([]byte, error) {
	data, _, err := c.conn.Get(path)
	return data, err
}

func (c *client) GetChildren(path string) ([]string, error) {
	children, _, err := c.conn.Children(path)
	return children, err
}

// Watch node's children change
func (c *client) Watch(root, nodeName string, processor WatchProcessor) {
	w := &watcherInfo{
		path:      getPath(root, nodeName),
		name:      nodeName,
		processor: processor,
		close:     make(chan bool)}
	if _, ok := c.watchers.LoadOrStore(w.path, w); ok {
		close(w.close)
		return
	}

	go func() {
		defer func() {
			w.close = nil
		}()
		for {
			children, _, ch, err := c.conn.ChildrenW(w.path)
			if err != nil {
				if w.closed {
					return
				} else if err == zk.ErrNoNode {
					err := c.ensurePath(w.path)
					if err != nil && c.conn.State() == zk.StateConnected {
						panic("client: unexcept error in ensure path")
					}
				} else if c.conn.State() != zk.StateConnected {
					// if disconnected, block and wait reconnect
					w.blocker.Block()
				}
				continue
			}

			if c.checkChildrenChange(w.path, children) {
				c.handleChildrenChange(w.path, children)
			}

			var watchEvent zk.Event
			var ok bool
			select {
			case watchEvent, ok = <-ch:
				if ok {
					switch watchEvent.Type {
					case zk.EventNodeChildrenChanged:
						children, _, err := c.conn.Children(watchEvent.Path)
						if err == nil {
							c.handleChildrenChange(watchEvent.Path, children)
						}
					// zk.EventNotWatching identify watch failed
					// go loop do rewatch
					default:
					}
				}
			case <-w.close:
				return
			}
		}
	}()

	return
}

// Unwatch node's children
func (c *client) Unwatch(root, nodeName string) {
	path := getPath(root, nodeName)
	v, ok := c.watchers.Load(path)
	if !ok {
		return
	}
	w, ok := v.(*watcherInfo)
	if ok {
		close(w.close)
		w.closed = true
		w.blocker.Unblock()
	}
}

// Ensure that node path exists
func (c *client) ensurePath(path string) error {
	if path == "/" {
		return nil
	}
	nodes := strings.Split(path, "/")
	root := "/"
	for _, v := range nodes[1:] {
		root += v
		if exists, _, err := c.conn.Exists(root); err != nil {
			return err
		} else if exists {
			continue
		}
		_, err := c.conn.Create(root, []byte(""), 0, zk.WorldACL(zk.PermAll))
		if err != nil && err != zk.ErrNodeExists {
			return err
		}
	}

	return nil
}

// Check children change or not
func (c *client) checkChildrenChange(path string, children []string) bool {
	v, ok := c.watchers.Load(path)
	if !ok {
		return false
	}

	watcher, ok := v.(*watcherInfo)
	if !ok {
		return false
	}

	watcher.mt.Lock()
	defer watcher.mt.Unlock()
	if len(watcher.children) != len(children) {
		return true
	}
	for _, v := range children {
		if _, ok := watcher.children[v]; !ok {
			return true
		}
	}
	return false
}

// Record new children and notify watcher
func (c *client) handleChildrenChange(path string, children []string) {
	v, ok := c.watchers.Load(path)
	if !ok {
		return
	}

	w, ok := v.(*watcherInfo)
	if !ok {
		return
	}

	sub := make(map[string]bool)
	for _, v := range children {
		sub[v] = true
	}
	w.mt.Lock()
	w.children = sub
	w.mt.Unlock()
	w.processor.ProcChildrenChange(w.name, children)
}

func getPath(root, nodeName string) string {
	if len(root) > 0 && root[len(root)-1] == '/' {
		return root + nodeName
	} else {
		return root + "/" + nodeName
	}
}
