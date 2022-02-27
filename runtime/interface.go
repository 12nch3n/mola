package runtime

import "github.com/sirupsen/logrus"

// OStatus is the type for order process result
type OStatus int8

const (
	// OStatusUnknown order processed but result unknown
	OStatusUnknown OStatus = iota
	// OStatusDuplicated order message duplicated
	OStatusDuplicated
	// OStatusSucceed order processed with result succeed
	OStatusSucceed
	// OStatusFailed order processed with result failed
	OStatusFailed
	// OStatusTimeout order processed with result timeout
	OStatusTimeout
)

func (s OStatus) String() string {
	return [...]string{"UNKNOWN", "DUPLICATED", "SUCCEED", "FAILED", "TIMEOUT"}[s]
}

// WStatus is the type for worker status
type WStatus int8

const (
	// WStatusUnknown worker status unknown
	WStatusUnknown WStatus = 0
	// WStatusBusy worker is busy for processing
	WStatusBusy = 1
	// WStatusAvaliable worker is avaliable for any income orders
	WStatusAvaliable = 2
	// WStatusLoaded worker is loaded by message to run
	WStatusLoaded = 3
	// WStatusCrushed load run time configure or context failed
	WStatusCrushed = 4
	// WStatusStopped worker is stopped
	WStatusStopped = 5
)

func (s WStatus) String() string {
	return [...]string{"UNKNOWN", "BUSY", "AVAILABLE", "LOADED", "CRUSHED"}[s]
}

// RWorker defines the interface to process the orders
type RWorker interface {
	// BoundOrder implements to return the bounding order name to verify the order message
	BoundOrder() string
	//Config implements to set worker runtime configurations with WorkConf field in contract
	Config(WorkerConf []byte, logger *logrus.Entry) error
	/*Process implements to process the order with custom result string and predefined status
	 * OStatus Could be UNKNOWN, SUCCEED & FAILED
	 * OStatus DUPLICATED & TIMEOUT have handled by framework already. */
	Process(context []byte, logger *logrus.Entry) (string, OStatus, error)
	// Terminate implements to terminate the current processing when server going to shutdown	Terminate(logger *logrus.Entry) error
	Terminate(logger *logrus.Entry) (err error)
}

// Payload defines interface to publish orders to message queue
type Payload interface {
	// BoundOrder implements to return the bounding order name to verify the order message
	BoundOrder() string
	// Publish the order body to send it to message queue
	Publish() (contract, traceID string, conf, body []byte, err error)
}
