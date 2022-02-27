package adaptor

import (
	"encoding/json"
	"fmt"
	"net/http"

	proto "github.com/golang/protobuf/proto"
	any "github.com/golang/protobuf/ptypes/any"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	configure "github.com/enriqueChen/mola/config"
	"github.com/enriqueChen/mola/message"
	pb "github.com/enriqueChen/mola/protobuf"
	"github.com/enriqueChen/mola/runtime"
)

// AppRouter contains App message routers
type AppRouter struct {
	appName         string
	sqs             *message.AWSSQS
	redis           *message.RedisConn
	OrdersTo        map[string]string
	BriefsFrom      string
	FailuresFrom    string
	logger          *logrus.Logger
	HandleBrief     func(contract, orderType, traceID, ret, csmResStr string, duration float64, errMsg string)
	HandleFailedMsg func(contract, orderType, traceID string, conf, body []byte, jsonFormatOrderMessage string)
}

const (
	dftRcvMsgCount   = 10
	dftRcvMsgTimeout = int64(60)
)

// BriefsQueue ...
func (a *AppRouter) BriefsQueue() (rKey, qName string) {
	rKey = fmt.Sprintf(message.RAppPrefix, a.appName, "BriefsQueue")
	qName = fmt.Sprintf(message.QAppPrefix, a.appName, "briefs")
	return
}

// FailuresQueue ...
func (a *AppRouter) FailuresQueue() (rKey, qName string) {
	rKey = fmt.Sprintf(message.RAppPrefix, a.appName, "FailsQueue")
	qName = fmt.Sprintf(message.QAppPrefix, a.appName, "failures")
	return
}

// OrderQueue ...
func (a *AppRouter) OrderQueue(orderName string) (rKey, rTimeoutKey, qName string) {
	return message.OrderIndexes(a.appName, orderName)
}

// LoadRouter load router from configure file
func LoadRouter(appName, file string) (router *AppRouter, l *logrus.Logger, err error) {
	var (
		conf     *configure.RuntimeOrdersConf
		r        *message.RedisConn
		tmpValue interface{}
	)
	if conf, err = configure.LoadAppConf(file); err != nil {
		return
	}
	l, err = runtime.InitLogger(&conf.Logs)
	if err != nil {
		fmt.Printf("Init log from configuration failed: %s", err.Error())
		return
	}
	logger := l.WithField("App", appName)
	if r, err = message.NewRedisConn(&conf.Redis, logger); err != nil {
		return
	}
	router = &AppRouter{
		appName: appName,
		sqs: &message.AWSSQS{
			Conf:   &conf.Sqs,
			Logger: logger.Logger,
		},
		redis:  r,
		logger: logger.Logger,
	}
	briefsIndex, _ := router.BriefsQueue()
	if tmpValue, err = router.redis.Get(briefsIndex); err != nil {
		return
	}
	router.BriefsFrom = tmpValue.(string)
	failsIndex, _ := router.FailuresQueue()
	if tmpValue, err = router.redis.Get(failsIndex); err != nil {
		return
	}
	router.FailuresFrom = tmpValue.(string)
	router.OrdersTo = make(map[string]string, len(conf.Orders))
	for _, o := range conf.Orders {
		ordersIndex, _, _ := router.OrderQueue(o.Name)
		if tmpValue, err = router.redis.Get(ordersIndex); err != nil {
			err = errors.Wrapf(err, "could not get order queue url in redis with key %s", ordersIndex)
			return
		}
		router.OrdersTo[o.Name] = tmpValue.(string)
	}
	return
}

// SendOrder send orders to global queue
func (a *AppRouter) SendOrder(payload runtime.Payload) (err error) {
	var (
		msgStr string
	)
	msgStr, err = a.newOrderMessage(payload)
	err = a.sqs.SendMessage(a.OrdersTo[payload.BoundOrder()], msgStr)
	return
}

func (a *AppRouter) newOrderMessage(payload runtime.Payload) (ret string, err error) {
	var (
		message           *pb.OrderMessage
		buf, conf, body   []byte
		contract, traceID string
	)
	if contract, traceID, conf, body, err = payload.Publish(); err != nil {
		return
	}
	message = &pb.OrderMessage{
		Envelop: &pb.OrderEnvelop{
			App:       a.appName,
			Contract:  contract,
			OrderType: payload.BoundOrder(),
			TraceID:   traceID,
		},
		WorkerConf: &any.Any{Value: conf},
		Body:       &any.Any{Value: body},
	}
	buf, err = proto.Marshal(message)
	ret = string(buf)
	return
}

// ResultHandle receive order process result and failed messages
func (a *AppRouter) ResultHandle(exit <-chan bool) (err error) {
	logFields := logrus.Fields{
		"App":    a.appName,
		"Module": "HandleResults",
	}

	go func() {
		if a.HandleBrief == nil {
			return
		}
		for {
			var briefMap map[string]string
			var err1 error
			if briefMap, err1 = a.sqs.ReceiveMessages(a.BriefsFrom, dftRcvMsgCount, dftRcvMsgTimeout); err1 != nil {
				a.logger.WithFields(logFields).WithField("error", err1.Error()).
					Warnf("get brief messages failed")
				continue
			}
			for recieptHandler, msgBody := range briefMap {
				brief := pb.OrderBrief{}
				if err1 = proto.UnmarshalMerge([]byte(msgBody), &brief); err1 != nil {
					a.logger.WithFields(logFields).WithField("error", err1.Error()).
						Warnf("get unmarshal messages failed")
					a.sqs.DeleteMessage(a.BriefsFrom, recieptHandler)
					continue
				}
				go func(rcp string) {
					e := brief.GetEnvelop()
					a.HandleBrief(e.GetContract(), e.GetOrderType(), e.GetTraceID(),
						brief.GetResult().String(), brief.GetCustomRes(),
						brief.GetDuration(), brief.GetErrorMessage())
					if x := a.sqs.DeleteMessage(a.BriefsFrom, rcp); x != nil {
						a.logger.WithFields(logFields).WithField("error", err1.Error()).
							Warnf("delete brief messages failed")
					}
				}(recieptHandler)
			}
		}
	}()

	go func() {
		if a.HandleFailedMsg == nil {
			return
		}
		for {
			var failuresMap map[string]string
			var err1 error
			if failuresMap, err1 = a.sqs.ReceiveMessages(a.FailuresFrom, dftRcvMsgCount, dftRcvMsgTimeout); err1 != nil {
				a.logger.WithFields(logFields).WithField("error", err1.Error()).
					Warnf("get failures messages failed")
				continue
			}
			for recieptHandler, msgBody := range failuresMap {
				order := pb.OrderMessage{}
				if err1 = proto.UnmarshalMerge([]byte(msgBody), &order); err1 != nil {
					a.logger.WithFields(logFields).WithField("error", err1.Error()).
						Warnf("unmarhsal failures messages failed")
					a.sqs.DeleteMessage(a.FailuresFrom, recieptHandler)
					continue
				}

				go func(rcp string) {
					e := order.GetEnvelop()
					jsonFormatOrderMessage, err1 := json.Marshal(order)
					if err1 != nil {
						a.logger.WithFields(logFields).WithField("error", err1.Error()).
							Warnf("json marhsal failures message failed")
					}

					a.HandleFailedMsg(e.GetContract(), e.GetOrderType(), e.GetTraceID(),
						[]byte(order.GetWorkerConf().String()),
						[]byte(order.GetBody().String()), string(jsonFormatOrderMessage))
					if x := a.sqs.DeleteMessage(a.FailuresFrom, rcp); x != nil {
						a.logger.WithFields(logFields).WithField("error", err1.Error()).
							Warnf("delete failure messages failed")
					}
				}(recieptHandler)
			}
		}
	}()

	<-exit
	return
}

type MessageNumInQueue struct {
	QueueName string
	InFlight  int
	Available int
	Url       string
}

//GetMessageNumInQueue get the number of message available and number of message in flight in all queue
func (a *AppRouter) GetMessageNumInQueue() (jsonString string, err error) {
	var (
		arraym    []MessageNumInQueue
		num       map[string]int
		logFields logrus.Fields
	)
	logFields = logrus.Fields{
		"App":    a.appName,
		"Module": "GetMessageNumInQueue",
	}
	arraym = []MessageNumInQueue{}

	allqueue := make(map[string]string)
	//copy orderqueue to allqueue
	for order, url := range a.OrdersTo {
		allqueue[order] = url
	}

	allqueue["briefs"] = a.BriefsFrom
	allqueue["failures"] = a.FailuresFrom

	for queuename, url := range allqueue {
		num, err = a.sqs.GetMessageNumInQueue(url)
		if err != nil {
			a.logger.WithFields(logFields).WithError(err).WithField("queueName", queuename).Errorf("GetMessageNumInQueue fail")
			continue
		}
		arraym = append(arraym, MessageNumInQueue{
			QueueName: queuename,
			InFlight:  num["ApproximateNumberOfMessagesNotVisible"],
			Available: num["ApproximateNumberOfMessages"],
			Url:       url,
		})
	}

	jsonbyte, err := json.Marshal(arraym)
	if err != nil {
		a.logger.WithFields(logFields).WithError(err).Errorf("marshal to json fail")
		return
	}
	jsonString = string(jsonbyte)

	return
}

//StartMonitorService unused
func (a *AppRouter) StartMonitorService() {
	logFields := logrus.Fields{
		"App":    a.appName,
		"Module": "MoniterService",
	}
	a.logger.WithFields(logFields).Info("starting moniter service")
	myHandler := func(w http.ResponseWriter, r *http.Request) {
		result, _ := a.GetMessageNumInQueue()
		fmt.Fprintf(w, result)
	}
	http.HandleFunc("/", myHandler)
	err := http.ListenAndServe(":23334", nil)
	if err != nil {
		fmt.Println(err)
	}
}
