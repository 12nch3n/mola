package adaptor

import (
	"os"

	"time"

	log "github.com/sirupsen/logrus"
	configure "github.com/enriqueChen/mola/config"
	"github.com/enriqueChen/mola/message"
)

const (
	defaultQTimeout int64 = 30
	defaultQRetry   int64 = 3
)

// DeployApp implements a deloy tool to create infrastructures
func DeployApp(appName string, file string) (router *AppRouter, err error) {
	var (
		conf      *configure.RuntimeOrdersConf
		logger    *log.Logger
		failsQURL *string
		r         *message.RedisConn
	)
	logger = log.New()
	logger.SetLevel(log.DebugLevel)
	logger.SetFormatter(&log.JSONFormatter{})
	logger.Out = os.Stdout
	logger.SetReportCaller(true)
	if conf, err = configure.LoadAppConf(file); err != nil {
		return
	}
	if r, err = message.NewRedisConn(&conf.Redis, logger.WithField("entry", "redis")); err != nil {
		return
	}
	router = &AppRouter{
		appName: appName,
		sqs: &message.AWSSQS{
			Conf:   &conf.Sqs,
			Logger: logger,
		},
		redis: r,
	}
	briefsIndex, briefsQname := router.BriefsQueue()
	if _, err = router.createQ(briefsIndex, briefsQname, defaultQTimeout, defaultQRetry, nil); err != nil {
		return
	}
	failsIndex, failsQname := router.FailuresQueue()
	if failsQURL, err = router.createQ(failsIndex, failsQname, defaultQTimeout, defaultQRetry, nil); err != nil {
		return
	}
	for _, o := range conf.Orders {
		ordersIndex, orderTimeoutIndex, OrderQname := router.OrderQueue(o.Name)
		if _, err = router.createQ(ordersIndex, OrderQname, o.Timeout, o.Retry, failsQURL); err != nil {
			return
		}
		if err = router.redis.Set(orderTimeoutIndex, o.Timeout, time.Duration(0)); err != nil {
			return
		}

	}
	return
}

func (a *AppRouter) createQ(qIndex, qName string, qTimeout, qRetry int64, dlQURL *string) (qURL *string, err error) {
	if qURL, err = a.sqs.CreateQueue(qName, qTimeout, qRetry, dlQURL); err != nil {
		return
	}
	err = a.redis.Set(qIndex, *qURL, time.Duration(0))
	return
}
