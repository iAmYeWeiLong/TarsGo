package tars

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/TarsCloud/TarsGo/tars/protocol/res/basef"
	"github.com/TarsCloud/TarsGo/tars/protocol/res/endpointf"
	"github.com/TarsCloud/TarsGo/tars/protocol/res/requestf"
	"github.com/TarsCloud/TarsGo/tars/transport"
)

var reconnectMsg = "_reconnect_"

// AdapterProxy : Adapter proxy
// ywl: 拥有一个 tarsclient
// ywl: 实现了 ClientProtocol 接口
type AdapterProxy struct {
	resp            sync.Map
	point           *endpointf.EndpointF
	tarsClient      *transport.TarsClient
	conf            *transport.TarsClientConf
	// ywl: 看不出存为成员变量的必要性，没有使用的地方。
	comm            *Communicator

	//proto    model.Protocol // ywl: model.Protocol 更应该是这里的成员变量，而不是 ServantProxy 的成员变量
	// ywl: 如果 model.Protocol 是自身成员变量，下面这一行持有 ServantProxy 就可以不要了。现在的引用关系有点复杂。
	obj             *ServantProxy
	failCount       int32
	lastFailCount   int32
	sendCount       int32
	successCount    int32
	status          bool // true for good
	lastSuccessTime int64
	lastBlockTime   int64
	lastCheckTime   int64

	count  int
	closed bool
}

// NewAdapterProxy : Construct an adapter proxy
func NewAdapterProxy(point *endpointf.EndpointF, comm *Communicator) *AdapterProxy {
	c := &AdapterProxy{}
	c.comm = comm
	c.point = point
	proto := "tcp"
	if point.Istcp == 0 {
		proto = "udp"
	}
	conf := &transport.TarsClientConf{
		Proto: proto,
		//NumConnect:   netthread,
		QueueLen:     comm.Client.ClientQueueLen,
		IdleTimeout:  comm.Client.ClientIdleTimeout,
		ReadTimeout:  comm.Client.ClientReadTimeout,
		WriteTimeout: comm.Client.ClientWriteTimeout,
		DialTimeout:  comm.Client.ClientDialTimeout,
	}
	c.conf = conf
	// ywl: 用户不能使用自定义的 TarsClient
	c.tarsClient = transport.NewTarsClient(fmt.Sprintf("%s:%d", point.Host, point.Port), c, conf)
	c.status = true
	return c
}

// ParsePackage : Parse packet from bytes
// ywl: 实现了 ClientProtocol 接口的 ParsePackage()
// ywl: tarsclient.go 的 connection.recv() 调用到这里

func (c *AdapterProxy) ParsePackage(buff []byte) (int, int) {
	return c.obj.proto.ParsePackage(buff)
}

// Recv : Recover read channel when closed for timeout
// ywl: 实现了 ClientProtocol 接口的 Recv()
// ywl: 客户端的收包处理函数。在 tarsclient.go 的 connection.recv() 开了协程调用到这里
// ywl: 因为只支持单向 rpc ,这里就是处理 rpc 的响应。
func (c *AdapterProxy) Recv(pkg []byte) {
	defer func() {
		// TODO readCh has a certain probability to be closed after the load, and we need to recover
		// Maybe there is a better way
		if err := recover(); err != nil {
			TLOG.Error("recv pkg painc:", err)
		}
	}()
	packet, err := c.obj.proto.ResponseUnpack(pkg)
	if err != nil {
		TLOG.Error("decode packet error", err.Error())
		return
	}
	if packet.IRequestId == 0 { // ywl: 是特殊的 “消息推送”
		go c.onPush(packet)
		return
	}
	// ywl: 作为客户端，这个标志是没有意义的吧 ？？？！！
	if packet.CPacketType == basef.TARSONEWAY {
		return
	}
	chIF, ok := c.resp.Load(packet.IRequestId) // ywl: 请求已经得到回复了，取出 异步结果，准备唤醒正在等待的协程。
	if ok {
		ch := chIF.(chan *requestf.ResponsePacket)
		select {
		// ywl: 塞入回应的消息体，唤醒阻塞的协程
		case ch <- packet:
		default:
			TLOG.Errorf("response timeout, write channel error, now time :%v, RequestId:%v",
				time.Now().UnixNano()/1e6, packet.IRequestId)
		}
	} else {
		TLOG.Errorf("response timeout, req has been drop, now time :%v, RequestId:%v",
			time.Now().UnixNano()/1e6, packet.IRequestId)
	}
}

// Send : Send packet
// ywl: 客户端发包时，ServantProxy.doInvoke() 调用到这里
func (c *AdapterProxy) Send(req *requestf.RequestPacket) error {
	TLOG.Debug("send req:", req.IRequestId)
	c.sendAdd()
	sbuf, err := c.obj.proto.RequestPack(req)
	if err != nil {
		TLOG.Debug("protocol wrong:", req.IRequestId)
		return err
	}
	return c.tarsClient.Send(sbuf)
}

// GetPoint : Get an endpoint
func (c *AdapterProxy) GetPoint() *endpointf.EndpointF {
	return c.point
}

// Close : Close the client
func (c *AdapterProxy) Close() {
	c.tarsClient.Close()
	c.closed = true
}

func (c *AdapterProxy) sendAdd() {
	atomic.AddInt32(&c.sendCount, 1)
}

func (c *AdapterProxy) succssAdd() {
	now := time.Now().Unix()
	atomic.SwapInt64(&c.lastSuccessTime, now)
	atomic.AddInt32(&c.successCount, 1)
	atomic.SwapInt32(&c.lastFailCount, 0)
}

func (c *AdapterProxy) failAdd() {
	atomic.AddInt32(&c.lastFailCount, 1)
	atomic.AddInt32(&c.failCount, 1)
}

func (c *AdapterProxy) reset() {
	now := time.Now().Unix()
	atomic.SwapInt32(&c.sendCount, 0)
	atomic.SwapInt32(&c.successCount, 0)
	atomic.SwapInt32(&c.failCount, 0)
	atomic.SwapInt32(&c.lastFailCount, 0)
	atomic.SwapInt64(&c.lastBlockTime, now)
	atomic.SwapInt64(&c.lastCheckTime, now)
	c.status = true
}

func (c *AdapterProxy) checkActive() (firstTime bool, needCheck bool) {
	if c.closed {
		return false, false
	}

	now := time.Now().Unix()
	if c.status {
		//check if healthy
		if (now-c.lastSuccessTime) >= failInterval && c.lastFailCount >= fainN {
			c.status = false
			c.lastBlockTime = now
			return true, false
		}
		if (now - c.lastCheckTime) >= checkTime {
			if c.failCount >= overN && (float32(c.failCount)/float32(c.sendCount)) >= failRatio {
				c.status = false
				c.lastBlockTime = now
				return true, false
			}
			c.lastCheckTime = now
			return false, false
		}
		return false, false
	}

	if (now - c.lastBlockTime) >= tryTimeInterval {
		c.lastBlockTime = now
		if err := c.tarsClient.ReConnect(); err != nil {
			return false, false
		}

		return false, true
	}

	return false, false
}

// ywl: 是特殊的 “消息推送”
func (c *AdapterProxy) onPush(pkg *requestf.ResponsePacket) {
	if pkg.SResultDesc == reconnectMsg {
		TLOG.Infof("reconnect %s:%d", c.point.Host, c.point.Port)
		oldClient := c.tarsClient
		c.tarsClient = transport.NewTarsClient(fmt.Sprintf("%s:%d", c.point.Host, c.point.Port), c, c.conf)

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*ClientIdleTimeout)
		defer cancel()
		oldClient.GraceClose(ctx) // grace shutdown
	}
	//TODO: support push msg
}
