package tars

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/TarsCloud/TarsGo/tars/model"
	"github.com/TarsCloud/TarsGo/tars/protocol"
	"github.com/TarsCloud/TarsGo/tars/protocol/res/basef"
	"github.com/TarsCloud/TarsGo/tars/protocol/res/requestf"
	"github.com/TarsCloud/TarsGo/tars/util/current"
	"github.com/TarsCloud/TarsGo/tars/util/endpoint"
	"github.com/TarsCloud/TarsGo/tars/util/rtimer"
	"github.com/TarsCloud/TarsGo/tars/util/tools"
)

var (
	maxInt32 int32 = 1<<31 - 1
	msgID    int32
)

const (
	STAT_SUCCESS = iota
	STAT_FAILED
)

// ServantProxy tars servant proxy instance
type ServantProxy struct {
	name string
	// ywl: 只是为了 取得 comm 里面的 client config .
	comm    *Communicator
	manager EndpointManager
	timeout int
	version int16
	// ywl: 就是 codec. 内部都没有见使用过，不应该作为自己的成员变量吧 ？？？！
	// 更应该是 AdapterProxy 的成员变量
	proto model.Protocol
	// ywl: 正在等对端回复的 rpc 调用数量
	queueLen int32
}

// NewServantProxy creates and initializes a servant proxy
func NewServantProxy(comm *Communicator, objName string) *ServantProxy {
	return newServantProxy(comm, objName)
}

func newServantProxy(comm *Communicator, objName string) *ServantProxy {
	s := &ServantProxy{}
	pos := strings.Index(objName, "@")
	if pos > 0 {
		s.name = objName[0:pos]
	} else {
		s.name = objName
	}
	pos = strings.Index(s.name, "://")
	if pos > 0 {
		s.name = s.name[pos+3:]
	}

	// init manager
	s.manager = GetManager(comm, objName)
	s.comm = comm
	// ywl: 这里不能定义自己的 TarsProtocol
	s.proto = &protocol.TarsProtocol{}
	s.timeout = s.comm.Client.AsyncInvokeTimeout
	s.version = basef.TARSVERSION
	return s
}

// TarsSetTimeout sets the timeout for client calling the server , which is in ms.
// ywl: 只是一个默认值，具体到某一次的 rpc 时，可以单独指定超时时间
func (s *ServantProxy) TarsSetTimeout(t int) {
	s.timeout = t
}

// TarsSetVersion set tars version
func (s *ServantProxy) TarsSetVersion(iVersion int16) {
	s.version = iVersion
}

// TarsSetProtocol tars set model protocol
func (s *ServantProxy) TarsSetProtocol(proto model.Protocol) {
	s.proto = proto
}

// 生成请求 ID
func (s *ServantProxy) genRequestID() int32 {
	// 尽力防止溢出
	atomic.CompareAndSwapInt32(&msgID, maxInt32, 1)
	for {
     // 0比较特殊,用于表示 server 端推送消息给 client 端进行主动 close()
		 // 溢出后回转成负数
		if v := atomic.AddInt32(&msgID, 1); v != 0 {  
			return v
		}
	}
}

// Tars_invoke is used for client inoking server.
// ywl: 应用层代码调用 rpc 时 call 到这里
func (s *ServantProxy) Tars_invoke(ctx context.Context, ctype byte,
	sFuncName string,
	buf []byte,
	status map[string]string,
	reqContext map[string]string,
	resp *requestf.ResponsePacket) error {
	defer CheckPanic()

	// 将ctx中的dyeinglog信息传入到request中
	var msgType int32
	dyeingKey, ok := current.GetDyeingKey(ctx) // ywl: 染色，没有看到后面的代码有染色相关的代码？？
	if ok {
		TLOG.Debug("dyeing debug: find dyeing key:", dyeingKey)
		if status == nil {
			status = make(map[string]string)
		}
		status[current.STATUS_DYED_KEY] = dyeingKey
		msgType = basef.TARSMESSAGETYPEDYED
	}

	req := requestf.RequestPacket{
		IVersion:     s.version,
		CPacketType:  int8(ctype),
		IRequestId:   s.genRequestID(),
		SServantName: s.name,
		SFuncName:    sFuncName,
		SBuffer:      tools.ByteToInt8(buf),
		// ywl: rpc时间，实际等待与告知对方的不一样 ！！！！ 并且对端也没有使用这个值
		//ITimeout:     s.comm.Client.ReqDefaultTimeout,
		ITimeout:     int32(s.timeout),
		Context:      reqContext,
		Status:       status,
		IMessageType: msgType,
	}
	msg := &Message{Req: &req, Ser: s, Resp: resp}
	msg.Init()

	timeout := time.Duration(s.timeout) * time.Millisecond
	ok, hashType, hashCode, isHash := current.GetClientHash(ctx)
	if ok {
		msg.isHash = isHash
		msg.hashType = HashType(hashType)
		msg.hashCode = hashCode
	}
	// ywl: contex 也可以覆盖默认的 timeout 值
	ok, to, isTimeout := current.GetClientTimeout(ctx)
	if ok && isTimeout {
		timeout = time.Duration(to) * time.Millisecond
	}

	var err error
	s.manager.preInvoke()
	if allFilters.cf != nil {
		err = allFilters.cf(ctx, msg, s.doInvoke, timeout)
	} else {
		// execute pre client filters
		for i, v := range allFilters.preCfs {
			err = v(ctx, msg, s.doInvoke, timeout)
			if err != nil {
				TLOG.Errorf("Pre filter error, no: %v, err: %v", i, err.Error())
			}
		}
		// execute rpc
		err = s.doInvoke(ctx, msg, timeout) // ywl: 返回的 err 不处理的吗？！ 像是下面处理，但是下一段逻辑不会覆盖掉 err 吗？？
		// execute post client filters
		for i, v := range allFilters.postCfs {
			filterErr := v(ctx, msg, s.doInvoke, timeout)
			if filterErr != nil {
				TLOG.Errorf("Post filter error, no: %v, err: %v", i, filterErr.Error())
			}
		}
	}
	// ywl: 要求很高，从 preInvoke() 到这里必须保证没有任何 panic()
	s.manager.postInvoke()

	if err != nil {
		msg.End()
		TLOG.Errorf("Invoke error: %s, %s, %v, cost:%d", s.name, sFuncName, err.Error(), msg.Cost())
		if msg.Resp == nil {
			ReportStat(msg, STAT_SUCCESS, STAT_SUCCESS, STAT_FAILED)
		} else if msg.Status == basef.TARSINVOKETIMEOUT {
			ReportStat(msg, STAT_SUCCESS, STAT_FAILED, STAT_SUCCESS)
		} else {
			ReportStat(msg, STAT_SUCCESS, STAT_SUCCESS, STAT_FAILED)
		}
		return err
	}
	msg.End()
	*resp = *msg.Resp
	ReportStat(msg, STAT_FAILED, STAT_SUCCESS, STAT_SUCCESS)
	return err
}

// ywl: 从上面的 Tars_invoke() 调用到这里
func (s *ServantProxy) doInvoke(ctx context.Context, msg *Message, timeout time.Duration) error {
	adp, needCheck := s.manager.SelectAdapterProxy(msg)
	if adp == nil {
		return errors.New("no adapter Proxy selected:" + msg.Req.SServantName)
	}
	// ywl: ObjQueueMax 更应该是成员变量吧，并给机会用户修改
	if s.queueLen > ObjQueueMax {
		return errors.New("invoke queue is full:" + msg.Req.SServantName)
	}
	ep := adp.GetPoint()
	current.SetServerIPWithContext(ctx, ep.Host)
	current.SetServerPortWithContext(ctx, fmt.Sprintf("%v", ep.Port))
	// ywl: 很奇怪的手法。 传参形式把 adp 传到函数更好
	msg.Adp = adp
	// ywl: 没有在创建 adp 时初始化好 obj ，在这里进行 obj 的赋值
	adp.obj = s
	atomic.AddInt32(&s.queueLen, 1)
	readCh := make(chan *requestf.ResponsePacket)
	adp.resp.Store(msg.Req.IRequestId, readCh) // ywl: 注册 异步结果(无回复调用 TARSONEWAY 也注册了)
	defer func() {
		CheckPanic()
		atomic.AddInt32(&s.queueLen, -1)
		adp.resp.Delete(msg.Req.IRequestId)
	}()
	if err := adp.Send(msg.Req); err != nil {
		adp.failAdd()
		return err
	}
	if msg.Req.CPacketType == basef.TARSONEWAY {
		adp.succssAdd()
		return nil
	}
	// ywl: 阻塞地等待对端回复结果
	select {
	case <-rtimer.After(timeout):
		// ywl: 等到超时了
		msg.Status = basef.TARSINVOKETIMEOUT
		adp.failAdd()
		msg.End()
		return fmt.Errorf("request timeout, begin time:%d, cost:%d, obj:%s, func:%s, addr:(%s:%d), reqid:%d",
			msg.BeginTime, msg.Cost(), msg.Req.SServantName, msg.Req.SFuncName, adp.point.Host, adp.point.Port, msg.Req.IRequestId)
	case msg.Resp = <-readCh:
		// ywl: 对端回复了结果
		if needCheck {
			go func() {
				adp.reset()
				ep := endpoint.Tars2endpoint(*msg.Adp.point)
				s.manager.addAliveEp(ep)
			}()
		}
		adp.succssAdd()
		if msg.Resp != nil {
			if msg.Status != basef.TARSSERVERSUCCESS || msg.Resp.IRet != 0 {
				if msg.Resp.SResultDesc == "" {
					return fmt.Errorf("basef error code %d", msg.Resp.IRet)
				}
				return errors.New(msg.Resp.SResultDesc)
			}
		} else {
			TLOG.Debug("recv nil Resp, close of the readCh?")
		}
		TLOG.Debug("recv msg succ ", msg.Req.IRequestId)
	}
	return nil
}
