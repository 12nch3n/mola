package runtime

import (
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/lithammer/shortuuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	pb "github.com/enriqueChen/mola/protobuf"
)

type workerWrapper struct {
	ID       string
	loggy    *log.Entry
	loggyBck *log.Entry

	// Runtime set attributes
	orderKey     string
	orderMessage string
	traceID      string
	contractName string
	worker       RWorker
	ctor         func() RWorker
	appName      string
	orderType    string
	status       WStatus
	exit         chan bool

	// Result attributes
	customResStr string
	orderStatus  OStatus
	orderError   error
	pDuration    time.Duration
}

func newWorkerWrapper(appName string, ctor func() RWorker, logger *log.Entry) (ret *workerWrapper) {
	ret = &workerWrapper{
		ID:        shortuuid.New(),
		appName:   appName,
		ctor:      ctor,
		status:    WStatusAvaliable,
		orderType: ctor().BoundOrder(),
	}
	ret.setLog(logger)
	return
}

func (ww *workerWrapper) setLog(logger *log.Entry) {
	ww.loggy = logger.WithFields(log.Fields{
		"WorkerID": ww.ID,
		"Order":    ww.orderType,
	})
	ww.loggyBck = ww.loggy
}

func (ww *workerWrapper) runtimeWorkerInit(orderKey string, orderBody string) (configure []byte, context []byte, err error) {
	ww.worker = ww.ctor()
	ww.orderKey = orderKey
	ww.orderMessage = orderBody
	ww.status = WStatusUnknown
	ww.orderStatus = OStatusUnknown
	defer func() {
		if err != nil {
			ww.status = WStatusCrushed
		}
	}()
	orderMsg := pb.OrderMessage{}
	err = proto.Unmarshal([]byte(orderBody), &orderMsg)
	if err != nil {
		ww.loggy.WithField("error", err.Error()).Errorf("unmarshal message failed")
		err = errors.Wrap(err, "unmarshal message failed")
		return
	}
	if orderMsg.GetEnvelop().GetOrderType() != ww.orderType {
		err = errors.Errorf("message order type %s mismatched with the worker bounded order: %s",
			orderMsg.GetEnvelop().GetOrderType(),
			ww.orderType)
		ww.loggy.WithField("order", orderMsg.GetEnvelop().GetOrderType()).
			Errorf("message order type %s mismatched with the woorker bounded order: %s",
				orderMsg.GetEnvelop().GetOrderType(),
				ww.orderType)
		return
	}
	ww.traceID = orderMsg.GetEnvelop().GetTraceID()
	ww.contractName = orderMsg.GetEnvelop().GetContract()
	configure = orderMsg.GetWorkerConf().GetValue()
	context = orderMsg.GetBody().GetValue()
	ww.loggyBck = ww.loggy
	ww.loggy = ww.loggy.WithFields(log.Fields{"TraceID": ww.traceID, "Contract": ww.contractName})
	return
}

func (ww *workerWrapper) configuring(configure []byte) (err error) {
	defer func() {
		if err != nil {
			ww.status = WStatusCrushed
		}
	}()
	ww.loggy.Debugf("start configuring")
	if len(configure) < 1 {
		ww.loggy.Infof("WorkerConf is empty")
		return
	}
	err = ww.worker.Config(configure, ww.loggy) //need to decode proto inside the function
	if err != nil {
		ww.loggy.WithField("error", err.Error()).Errorf("set worker configure failed with error")
		err = errors.Wrap(err, "set worker configure failed with error")
	}
	return
}

func (ww *workerWrapper) process(context []byte, timeout time.Duration) (err error) {
	// only process loaded workers, set status to busy
	if ww.status != WStatusLoaded {
		err = errors.New("process failed worker status should be loaded")
		ww.loggy.WithField("WStatus", ww.status).WithField("error", err.Error()).
			Errorf("process failed worker status should be loaded")
		ww.orderError = WarpError(ww.orderError, "worker not loaded for processing")

		return
	}
	ww.status = WStatusBusy

	defer func() {
		// Set worker status from busy to loaded
		if ww.status == WStatusBusy {
			ww.status = WStatusLoaded
		}
		ww.loggy.WithField("WStatus", ww.status.String()).
			WithField("OStatus", ww.orderStatus.String()).
			Infof("order procceed")
	}()

	start := time.Now()
	finished := make(chan bool, 1)
	go func() {
		ww.loggy.Debugf("start processing")
		ww.customResStr, ww.orderStatus, ww.orderError = ww.worker.Process(context, ww.loggy)
		finished <- true
	}()

	for {
		select {
		case <-finished:
			ww.pDuration = time.Since(start)
			err = ww.orderError
			if err != nil {
				ww.orderStatus = OStatusFailed
			}
			return
		case <-ww.exit:
			ww.pDuration = time.Since(start)
			err = errors.New("order processing terminated")
			ww.orderStatus = OStatusUnknown
			ww.status = WStatusCrushed
			err1 := ww.worker.Terminate(ww.loggy)
			if err1 != nil {
				ww.loggy.WithField("error", err1.Error()).
					Warnf("terminate worker on demand")
			}
			ww.loggy.Warnf("order processing terminated")
			return
		case <-time.After(timeout):
			ww.pDuration = time.Since(start)
			err = errors.New("order processing time out")
			ww.orderStatus = OStatusTimeout
			err1 := ww.worker.Terminate(ww.loggy)
			if err1 != nil {
				ww.loggy.WithField("error", err1.Error()).
					Errorf("terminate worker by timeout failed")
			}
			ww.loggy.Warnf("order processing time out")
			return
		}
	}
}

func (ww *workerWrapper) terminate() {
	ww.loggy.Warnf("try to terminate process")
	if ww.status == WStatusBusy {
		ww.exit <- true
	}
}

// reset force reset worker to origin status
func (ww *workerWrapper) reset() {
	ww.loggy = ww.loggyBck
	ww.status = WStatusAvaliable
	ww.orderError = nil
	ww.orderKey = ""
	ww.orderMessage = ""
	ww.traceID = ""
	ww.customResStr = ""
	ww.orderStatus = OStatusUnknown
	ww.pDuration = 0

}

func (ww *workerWrapper) brief() (ret string, err error) {
	ww.loggy.WithField("WStatus", ww.status.String()).WithField("OStatus", ww.orderStatus.String()).
		Debugf("send brief")
	var orderStatus pb.OrderBrief_State
	key, ok := pb.OrderBrief_State_value[ww.orderStatus.String()]
	if !ok {
		ww.loggy.WithField("WStatus", ww.status.String()).WithFields(log.Fields{"OStatus": ww.orderStatus.String(), "orderError": ww.orderError}).
			Warnf("unknown order execution status")
		orderStatus = pb.OrderBrief_UNKNOWN
	}
	orderStatus = pb.OrderBrief_State(key)
	orderType := ww.orderType
	brief := pb.OrderBrief{
		Envelop: &pb.OrderEnvelop{
			App:       ww.appName,
			Contract:  ww.contractName,
			OrderType: orderType,
			TraceID:   ww.traceID,
		},
		Result:   orderStatus,
		Duration: ww.pDuration.Seconds(),
	}
	brief.CustomRes = ww.customResStr
	if ww.orderError != nil {
		brief.ErrorMessage = fmt.Sprintf("%v", ww.orderError)
	}
	retbytes, err := proto.Marshal(&brief)
	if err != nil {
		ww.loggy.WithField("brief error", err.Error()).
			Errorf("brief message Marshal fail")
		return
	}
	ret = string(retbytes)
	return
}
