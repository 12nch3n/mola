package message

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/go-redis/redis"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	configure "github.com/enriqueChen/mola/config"
	pb "github.com/enriqueChen/mola/protobuf"
)

// MRouter implements a message router with application name
type MRouter struct {
	logger        *log.Entry
	redis         *RedisConn
	sqs           *AWSSQS
	appName       string
	orderType     string
	ordersFrom    string
	briefsTo      string
	priorityQueue chan string //bodyString  (bodyString is encoded)
}

const (
	// RAppPrefix redis key prefix
	RAppPrefix = "App:%s:%s:"
	// ROrderPrefix redis key prefix
	ROrderPrefix = "App:%s:Order:%s:%s:"
	// RWorkershopPrefix redis key prefix
	RWorkershopPrefix = "App:%s:Order:%s:Workers:%s-%d"
	// ROrderTracePrefix redis key prefix
	ROrderTracePrefix = "App:%s:Order:%s:Trace:%s"
)

const (
	// QAppPrefix for sqs queue name
	QAppPrefix = "MOLA-%s-%s"
	// QOrderPrefix for sqs queue name
	QOrderPrefix = "MOLA-%s-%s-orders"
	// PriorityReceiptHandle is the fake recieptHandle for messages http rather than message from sqs
	PriorityReceiptHandle = "PRIORITY"
)

// NewRouter produce a new MRouter to server with a configure referrence
func NewRouter(appName, orderType string, conf *configure.WConf, logger *log.Entry) (pTimeout time.Duration, ret *MRouter, err error) {
	ret = &MRouter{
		redis: &RedisConn{
			Conf:   &conf.Redis,
			Logger: logger,
		},
		sqs: &AWSSQS{
			Conf:   &conf.Sqs,
			Logger: logger.Logger,
		},
		appName:       appName,
		orderType:     orderType,
		logger:        logger,
		priorityQueue: make(chan string, conf.Priority_queue_http.Priority_queue_capacity),
	}
	ret.redis.NewClient()
	ret.logger.WithFields(log.Fields{
		"application": ret.appName,
		"orderType":   ret.orderType,
	})
	pTimeout, err = ret.QueueBounding()

	return
}

// BriefQueueIndex get brief queue redis index
func (r *MRouter) BriefQueueIndex() string {
	return fmt.Sprintf(RAppPrefix, r.appName, "BriefsQueue")
}

// OrderQueueIndex get order queue redis index
func (r *MRouter) OrderQueueIndex(key string) string {
	return fmt.Sprintf(ROrderPrefix, r.appName, r.orderType, key)
}

// WorkshopIndex get workshop redix key
func (r *MRouter) WorkshopIndex() string {
	hostname, _ := os.Hostname()
	pid := os.Getpid()
	return fmt.Sprintf(RWorkershopPrefix, r.appName, r.orderType, hostname, pid)
}

// QueueBounding bounding the queues to the router for application
func (r *MRouter) QueueBounding() (pTimeout time.Duration, err error) {
	briefsTo, err := r.redis.Get(r.BriefQueueIndex())
	if err != nil {
		r.logger.WithField("error", err.Error()).
			Errorf("get briefsTo queue URL from redis failed")
		return
	}
	r.briefsTo = briefsTo.(string)
	ordersFrom, err := r.redis.Get(r.OrderQueueIndex("OrdersQueue"))
	if err != nil {
		r.logger.WithField("error", err.Error()).
			Errorf("get ordersFrom queue URL from redis failed")
		return
	}
	r.ordersFrom = ordersFrom.(string)
	timeout, err := r.redis.Get(r.OrderQueueIndex("OrderTimeout"))
	if err != nil {
		r.logger.WithField("error", err.Error()).
			Errorf("get ordersFrom queue URL from redis failed")
		return
	}
	var t int64
	if t, err = strconv.ParseInt(timeout.(string), 10, 64); err != nil {
		r.logger.WithField("error", err.Error()).
			Errorf("convert timeout failed")
	}
	if t <= taskVisiableGap {
		err = errors.Errorf("queue message timeout must be larger than %d", taskVisiableGap)
		return
	}
	pTimeout = time.Duration(t-taskVisiableGap) * time.Second
	return
}

// FetchOrder fetch a message from sqs service
func (r *MRouter) FetchOrder(orderTimeout time.Duration) (orderKey string, orderBody string, err error) {
	var (
		mMap     map[string]string
		vTimeout int64
	)
	vTimeout = int64(orderTimeout.Seconds()) + taskVisiableGap
	//fetch message from priorityQueue first, if no message in priorityQueue, then fetch message from sqs
	select {
	case msg := <-r.priorityQueue: //try to fetch order from priorityQueue
		mMap = make(map[string]string)
		mMap[PriorityReceiptHandle] = msg
	default:
		mMap, err = r.sqs.ReceiveMessages(r.ordersFrom, 1, vTimeout)
		if err != nil {
			r.logger.WithField("error", err.Error()).
				Errorf("Receive message failed")
			errors.Wrap(err, "receive message failed")
			return
		}
	}

	r.logger.Debugf("fetched messages: %v", mMap)
	if len(mMap) == -1 {
		r.logger.Infof("Receive message succeed, but no new messages")
		return
	}
	r.logger.Debugf("fetched messages: %v", mMap)
	for k, v := range mMap {
		orderKey = k
		orderBody = v
	}
	return
}

// CompleteOrder set the order as complete by delete it from order queue
func (r *MRouter) CompleteOrder(orderKey string) (err error) {
	r.logger.WithField("receiptHandle", orderKey).
		Debugf("send delete message to queue to complete the order")
	return r.sqs.DeleteMessage(r.ordersFrom, orderKey)
}

// ReleaseOrderForRetry set the order as complete by delete it from order queue
func (r *MRouter) ReleaseOrderForRetry(orderKey string) (err error) {
	r.logger.WithField("receiptHandle", orderKey).
		Debugf("set the order message visible for all workers")
	return r.sqs.ChangeMessageVisibility(r.ordersFrom, orderKey, taskVisiableGap)
}

// BriefOrder send a brief of a completed order to BriefTo quue
func (r *MRouter) BriefOrder(briefBody string) (err error) {
	r.logger.WithField("briefBody", briefBody).
		Debugf("send brief message to queue")
	return r.sqs.SendMessage(r.briefsTo, briefBody)
}

const (
	taskUniqHours = 12
)

// Duplicated check if task duplicated in latest taskLifeCycle duration and set deduplicate key in redis
func (r *MRouter) Duplicated(workerID, traceID string, timeWindow time.Duration) (duplicated bool, err error) {
	var val interface{}
	traceKey := fmt.Sprintf(ROrderTracePrefix, r.appName, r.orderType, traceID)
	if val, err = r.redis.Get(traceKey); err == nil {
		r.logger.WithField("traceID", traceID).Warnf("duplicated processing with %s", val.(string))
		duplicated = true
		return
	}
	if err == redis.Nil {
		duplicated = false
	}
	hostname, _ := os.Hostname()
	pid := os.Getpid()
	value := fmt.Sprintf("Processing:%s-%d-%s", hostname, pid, workerID)
	err = r.redis.Set(traceKey, value, timeWindow)
	r.logger.WithField("traceID", traceID).Debugf("set task %s with deduplicate key %s in %d second",
		traceID, value, timeWindow/time.Second)
	return
}

// MarkOrderCompleted mark a task completed by update its trace item in redis
func (r *MRouter) MarkOrderCompleted(workerID, traceID string) (err error) {
	traceKey := fmt.Sprintf(ROrderTracePrefix, r.appName, r.orderType, traceID)
	hostname, _ := os.Hostname()
	pid := os.Getpid()
	value := fmt.Sprintf("Completed:%s-%d-%s", hostname, pid, workerID)
	err = r.redis.Set(traceKey, value, taskUniqHours*time.Hour)
	r.logger.WithField("traceID", traceID).Debugf("set task %s with deduplicate key %s in %d second",
		traceID, value, taskUniqHours*time.Hour)
	return
}

// DeleteDuplicateKey delete "deduplicate key" in redis
func (r *MRouter) DeleteDuplicateKey(traceID string) (err error) {
	logFields := log.Fields{
		"application": r.appName,
		"orderType":   r.orderType,
		"traceID":     traceID,
	}

	traceKey := fmt.Sprintf(ROrderTracePrefix, r.appName, r.orderType, traceID)
	res, err := r.redis.Del(traceKey)
	if err != nil {
		r.logger.WithFields(logFields).WithError(err).Errorf("Delete deDuplicated key error:%s", traceKey)
		return
	}
	if res != true {
		r.logger.WithFields(logFields).Debug("did not find deDuplicated key, key has been timeout")
		return
	}
	r.logger.WithFields(logFields).WithField("traceID", traceID).Debugf("Delete deduplicated key :%s", traceKey)
	return
}

// WorkerShopInfo describe workshop status
type WorkerShopInfo struct {
	Capacity int            `json:"capacity"`
	Status   map[string]int `json:"status"`
}

// EnrollWorkshopStatus enroll workershop status in redis
func (r *MRouter) EnrollWorkshopStatus(capa int, status map[string]int, expiredAfter time.Duration) (err error) {

	workerShopKey := r.WorkshopIndex()
	wi := WorkerShopInfo{
		Capacity: capa,
		Status:   status,
	}
	jsonStr, _ := json.Marshal(wi)
	err = r.redis.Set(workerShopKey, jsonStr, expiredAfter)
	if err != nil {
		r.logger.WithField("workshop ID", workerShopKey).WithError(err).Error("set workshop information fail")
	}
	r.logger.WithField("workshop ID", workerShopKey).
		WithField("workshop info", string(jsonStr)).
		Debug("set workshop information")
	return
}

// InsertOrderToPriorityQueue insert a message from http post
func (r *MRouter) InsertOrderToPriorityQueue(s string) (err error) {
	orderMsg := pb.OrderMessage{}
	if err = json.Unmarshal([]byte(s), &orderMsg); err != nil {
		r.logger.WithField("message", s).WithError(err).Errorf("json unmarshal ordermsg fail")
		return
	}

	body, err := proto.Marshal(&orderMsg)
	if err != nil {
		fmt.Println(err)
		r.logger.WithField("message", s).WithError(err).Errorf("proto Marshal ordermsg fail")
		return
	}
	//msg := map[string]string{receiptHandle: string(body)}
	r.priorityQueue <- string(body)
	r.logger.WithField("traceID", orderMsg.Envelop.TraceID).Infof("receive priority message order")
	return
}

// OrderIndexes get order related indexes
func OrderIndexes(app, order string) (rKey, rTimeoutKey, qName string) {
	rKey = fmt.Sprintf(ROrderPrefix, app, order, "OrdersQueue")
	rTimeoutKey = fmt.Sprintf(ROrderPrefix, app, order, "OrderTimeout")
	qName = fmt.Sprintf(QOrderPrefix, app, order)
	return
}
