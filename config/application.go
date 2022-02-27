package configure

import (
	"github.com/jinzhu/configor"
)

// OrderConf is configuration for order
type OrderConf struct {
	Name    string `toml:"name" required:"true"`
	Timeout int64  `toml:"timeout" required:"true"`
	Retry   int64  `toml:"retry" required:"true"`
}

// RuntimeOrdersConf is the configuration for applications' all contract in runtime
type RuntimeOrdersConf struct {
	Sqs    AWSSQSConf  `toml:"sqs" required:"true"`
	Redis  RedisConf   `toml:"redis" required:"true"`
	Logs   LogConf     `toml:"logs"  required:"true"`
	Orders []OrderConf `toml:"orders" required:"true"`
}

// LoadAppConf load contract configuration
func LoadAppConf(file string) (conf *RuntimeOrdersConf, err error) {
	// Do not support Auto reload yet
	loadingConf := RuntimeOrdersConf{}
	err = configor.New(&configor.Config{}).Load(&loadingConf, file)
	conf = &loadingConf
	return
}
