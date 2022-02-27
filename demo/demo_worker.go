package demo

import (
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"github.com/enriqueChen/mola/runtime"
)

// EWorker implements to hande DemoOrders
type EWorker struct {
	Foo      string
	Bar      string
	DemoWait int64
}

// BoundOrder implements to return the bounding order name to verify the order message
func (dw *EWorker) BoundOrder() string {
	return "DemoOrder1"
}

// Config implements to set worker runtime configurations with WorkConf field in contract
func (dw *EWorker) Config(WorkerConf []byte, logger *logrus.Entry) (err error) {
	var RuntimeConf DemoConf
	RuntimeConf = DemoConf{}
	if err = proto.Unmarshal(WorkerConf, &RuntimeConf); err != nil {
		logger.WithField("error", err.Error()).
			Errorf("unmarshal worker configuration from order message failed")
		return
	}
	logger.WithField("ConfItem1", RuntimeConf.GetConfItem1()).
		WithField("ConfItem2", RuntimeConf.GetConfItem2()).Infof("configure items unpacked")
	dw.DemoWait = RuntimeConf.GetConfItem2()
	return
}

/*Process implements to process the order
 * OStatus Could be UNKNOWN, SUCCEED & FAILED
 * OStatus DUPLICATED & TIMEOUT have handled by framework already. */
func (dw *EWorker) Process(context []byte, logger *logrus.Entry) (customRes string, ret runtime.OStatus, err error) {
	defer func() {
		if err != nil {
			ret = runtime.OStatusFailed
		} else {
			ret = runtime.OStatusSucceed
		}
	}()
	var OrderBody DemoOrder
	if err = proto.Unmarshal(context, &OrderBody); err != nil {
		logger.WithField("error", err.Error()).
			Errorf("unmarshal order body from order message failed")
		return
	}
	dw.Foo = OrderBody.GetFoo()
	dw.Bar = OrderBody.GetBar()
	ret = runtime.OStatusUnknown
	time.Sleep(time.Duration(dw.DemoWait) * time.Second)

	logger.Infof("Task Proceed: Foo:%v, Bar:%v", dw.Foo, dw.Bar)
	return
}

// Terminate implements to terminate the current processing when server going to shutdown
func (dw *EWorker) Terminate(logger *logrus.Entry) (err error) {
	logger.Warnf("worker instance terminate implements for immediately stop")
	return
}

//EWorkerCreator produce EWorker instance who implements the worker interface
func EWorkerCreator() (w runtime.RWorker) {
	w = &EWorker{
		Foo: "Test",
	}
	return
}
