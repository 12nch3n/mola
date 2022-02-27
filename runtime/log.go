package runtime

import (
	"os"
	"time"

	fr "github.com/lestrrat-go/file-rotatelogs"
	"github.com/rifflock/lfshook"
	log "github.com/sirupsen/logrus"
	kl "github.com/tracer0tong/kafkalogrus"
	configure "github.com/enriqueChen/mola/config"
)

var (
	// Logger Global logger
	Logger *log.Logger
)

// InitLogger Init a logger and get the referrence of it
func InitLogger(conf *configure.LogConf) (logger *log.Logger, err error) {
	var (
		loglvl log.Level
	)
	logger = log.New()

	Logger = logger
	if loglvl, err = log.ParseLevel(conf.Level); err != nil {
		loglvl = log.InfoLevel
	}
	logger.SetLevel(loglvl)
	switch conf.Format {
	case "json":
		logger.SetFormatter(&log.JSONFormatter{})
	default:
		logger.SetFormatter(&log.TextFormatter{})
	}
	logger.SetReportCaller(true)
	logger.Out = os.Stdout

	if err = addRotateLog(conf.Rotate, logger); err != nil {
		return
	}
	if err = addKfkHook(conf.KfkHook, logger); err != nil {
		return
	}
	return
}

func addRotateLog(conf *configure.RotateHook, logger *log.Logger) (err error) {
	var (
		writer *fr.RotateLogs
	)
	writer, err = fr.New(
		conf.File,
		fr.WithLinkName(conf.Link),
		fr.WithMaxAge(-1),
		fr.WithRotationTime(time.Duration(conf.RotationHors)*time.Hour),
		fr.WithRotationCount(conf.KeepRotation),
	)
	if err != nil {
		return
	}
	logger.AddHook(lfshook.NewHook(
		writer,
		&log.JSONFormatter{},
	))
	return
}

func addKfkHook(conf *configure.KfkLogHook, logger *log.Logger) (err error) {
	if conf == nil {
		return
	}
	var loglvl log.Level
	if loglvl, err = log.ParseLevel(conf.Level); err != nil {
		loglvl = log.InfoLevel
	}
	levels := log.AllLevels[:loglvl+1]
	hook, err := kl.NewKafkaLogrusHook("klh", levels, &log.JSONFormatter{}, conf.Brokers, conf.Topic, true, nil)
	if err == nil {
		log.AddHook(hook)
	}
	return
}
