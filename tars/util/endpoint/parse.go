package endpoint

import (
	"flag"
	"strings"
)

// Parse pares string to struct Endpoint, like tcp -h 10.219.139.142 -p 19386 -t 60000
func Parse(endpoint string) Endpoint {
	//tcp -h 10.219.139.142 -p 19386 -t 60000
	proto := endpoint[0:3]
	// ywl: 是不是传错参数啊？？？ 为什么传的是 proto 进去
	pFlag := flag.NewFlagSet(proto, flag.ContinueOnError)
	var host, bind string
	var port, timeout int
	pFlag.StringVar(&host, "h", "", "host")
	pFlag.IntVar(&port, "p", 0, "port")
	pFlag.IntVar(&timeout, "t", 3000, "timeout")
	pFlag.StringVar(&bind, "b", "", "bind")
	pFlag.Parse(strings.Fields(endpoint)[1:])
	istcp := int32(0)
	if proto == "tcp" {
		istcp = int32(1)
	}
	e := Endpoint{
		Host:    host,
		Port:    int32(port),
		Timeout: int32(timeout),
		Istcp:   istcp,
		Proto:   proto,
		Bind:    bind,
	}
	e.Key = e.String()
	return e
}
