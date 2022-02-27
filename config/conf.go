package configure

import (
	"github.com/jinzhu/configor"
)

// AWSSQSConf AWS Simple Queue Service Configuration
type AWSSQSConf struct {
	Region  string `toml:"region" required:"true"`
	Project string `toml:"project" required:"true"`
	App     string `toml:"app" required:"true"`
	Service string `toml:"service" required:"true"`
}

// RedisConf Redis configuration
type RedisConf struct {
	Addr     string `toml:"addr" required:"true"`
	Password string `toml:"password" default:""`
	DB       int    `toml:"db" default:"0"`
}

//HttpConf PriorityQueue http api configuration
type PriorityQueueHttpConf struct {
	Enable                  bool   `toml:"enable" required:"true"`
	Port                    string `toml:"port" default:"8080"`
	Priority_queue_capacity int32  `toml:"priority_queue_capacity"`
}

// LogConf Log configuration
type LogConf struct {
	Level   string      `toml:"console_level" default:"info"`
	Format  string      `toml:"format" default:"txt"`
	Rotate  *RotateHook `toml:"rotate" required:"true"`
	KfkHook *KfkLogHook `toml:"kafka"`
}

// Rotate Log file configuration
type RotateHook struct {
	Link         string `toml:"link" default:"./mola.log"`
	File         string `toml:"file" default:"./mola.log.%Y%m%d%H%M"`
	RotationHors int64  `toml:"rotation" default:"24"`
	KeepRotation uint   `toml:"keep_rotation" deflaut:"7"`
}

// KfkLogHook log hook to push json log into kafka topic
type KfkLogHook struct {
	Level   string   `toml:"level" default:"info"`
	Brokers []string `toml:"brokers"`
	Topic   string   `toml:"topic"`
}

// WConf the workshop configuration
type WConf struct {
	Capacity            int32                 `toml:"capacity"`
	StatePulse          int32                 `toml:"status_plus"`
	Logs                LogConf               `toml:"logs"`
	Redis               RedisConf             `toml:"redis"`
	Sqs                 AWSSQSConf            `toml:"sqs"`
	Priority_queue_http PriorityQueueHttpConf `toml:"priority_queue_http"`
}

var (
	// GlobalConf static Global Configure
	GlobalConf WConf
)

// Init the global configuration
func Init(file string) (conf *WConf, err error) {
	// Do not support Auto reload yet
	err = configor.New(&configor.Config{}).Load(&GlobalConf, file)
	conf = &GlobalConf
	return
}
