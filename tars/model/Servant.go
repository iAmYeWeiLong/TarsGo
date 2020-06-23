package model

import (
	"context"

	"github.com/TarsCloud/TarsGo/tars/protocol/res/requestf"
)

// Servant is interface for call the remote server.
type Servant interface {
	Tars_invoke(ctx context.Context, ctype byte,
		sFuncName string,
		buf []byte,
		status map[string]string,
		context map[string]string,
		Resp *requestf.ResponsePacket) error
	TarsSetTimeout(t int)
	TarsSetProtocol(Protocol)
}

type Protocol interface {
	// ywl: 加上包头 4 byte 表示业务包的长度
	RequestPack(*requestf.RequestPacket) ([]byte, error)
	// ywl: 从字节流中反序列化出 ResponsePacket 消息
	ResponseUnpack([]byte) (*requestf.ResponsePacket, error)
	// ywl: 检查有没有收到一个完整的业务包。处理 “粘包” 问题
	ParsePackage([]byte) (int, int)
}
