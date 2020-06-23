package protocol

import (
	"bytes"
	"encoding/binary"
	"github.com/TarsCloud/TarsGo/tars/protocol/codec"
	"github.com/TarsCloud/TarsGo/tars/protocol/res/requestf"
)

var maxPackageLength int = 10485760

// SetMaxPackageLength sets the max length of tars packet 
func SetMaxPackageLength(len int) {
	maxPackageLength = len
}

// ywl: 检查有没有收到一个完整的业务包
func TarsRequest(rev []byte) (int, int) {
	if len(rev) < 4 {
		return 0, PACKAGE_LESS
	}
	iHeaderLen := int(binary.BigEndian.Uint32(rev[0:4]))
	if iHeaderLen < 4 || iHeaderLen > maxPackageLength {
		return 0, PACKAGE_ERROR
	}
	if len(rev) < iHeaderLen {
		return 0, PACKAGE_LESS
	}
	return iHeaderLen, PACKAGE_FULL
}

// ywl: 实现了 model.Protocol 接口
// ywl: 这个类以往的叫 codec ，功能是进行 decode 或是 encode
type TarsProtocol struct {}

// ywl: 加上包头 4 byte 表示业务包的长度
func (p *TarsProtocol) RequestPack(req *requestf.RequestPacket) ([]byte, error) {
	sbuf := bytes.NewBuffer(nil)
	sbuf.Write(make([]byte, 4))
	os := codec.NewBuffer()
	err := req.WriteTo(os)
	if err != nil {
		return nil, err
	}
	bs := os.ToBytes()
	sbuf.Write(bs)
	l := sbuf.Len()
	binary.BigEndian.PutUint32(sbuf.Bytes(), uint32(l))
	return sbuf.Bytes(), nil

}
func (p *TarsProtocol) ResponseUnpack(pkg []byte) (*requestf.ResponsePacket, error) {
	packet := &requestf.ResponsePacket{}
	err := packet.ReadFrom(codec.NewReader(pkg[4:]))
	return packet, err
}

// ywl: 检查有没有收到一个完整的业务包
func (p *TarsProtocol) ParsePackage(rev []byte) (int, int) {
	return TarsRequest(rev)
}
