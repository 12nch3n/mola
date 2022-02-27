package runtime

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/pkg/errors"

	log "github.com/sirupsen/logrus"
	configure "github.com/enriqueChen/mola/config"
	"github.com/enriqueChen/mola/message"
)

// RServer implements the workshop service
type RServer struct {
	appName       string
	capacity      int32
	logger        *log.Entry
	conf          *configure.WConf
	workers       []*workerWrapper
	workerOpLock  sync.Mutex
	messageRouter *message.MRouter
	workerTimeout time.Duration
	ctor          func() RWorker
}

// MaxWaitBeforeExit the max wait time for graceful exit
const MaxWaitBeforeExit = 60 * time.Second

// Register a worker creator into workshop so work shop could create workers on demand
func (s *RServer) Register(ctor func() RWorker) (err error) {
	s.ctor = ctor
	s.workerOpLock.Lock()
	defer s.workerOpLock.Unlock()
	for i := int32(0); i < s.capacity; i++ {
		s.workers = append(s.workers, newWorkerWrapper(s.appName, s.ctor, s.logger))
	}
	return
}

// AppBounding bounding an application for the workshop when service launching
func (s *RServer) AppBounding(appName, orderType string) (err error) {
	s.appName = appName
	s.workerTimeout, s.messageRouter, err = message.NewRouter(appName, orderType, s.conf, s.logger)
	if err != nil {
		s.logger.WithField("Application", appName).WithField("error", err.Error()).
			Errorf("Bounding the message queues and the workshop failed")
	}
	s.logger = s.logger.WithField("Application", appName)
	return
}

func (s *RServer) timeout() time.Duration {
	return s.workerTimeout
}

// NewServer produce a new server which is unbounding application and without ant workers
func NewServer(serverConfigurePath string) (server *RServer, err error) {
	conf, err := configure.Init(serverConfigurePath)
	if err != nil {
		fmt.Printf("load configuration from %s failed: %s", serverConfigurePath, err.Error())
		return
	}
	server = &RServer{}
	server.appName = "UNDEFINED"
	server.capacity = conf.Capacity
	server.conf = conf
	var l *log.Logger
	l, err = InitLogger(&conf.Logs)
	if err != nil {
		fmt.Printf("Init log from configuration failed: %s", err.Error())
		return
	}
	server.logger = l.WithField("instance", "workshop")
	return
}

// Run the workshop server with worker creator for application
// kill command would wait the worker current processing compeleted
// To kill workshop server immediately, please use `kill -9`
func (s *RServer) Run(appName string, workerCreator func() RWorker, exit chan bool) (err error) {
	s.Register(workerCreator)
	err = s.AppBounding(appName, s.workers[0].orderType)
	if err != nil {
		s.logger.WithError(err).Errorf("bounding workshop to application failed")
		return
	}
	wg := &sync.WaitGroup{}
	exitSignals := make(chan bool, len(s.workers))
	for _, worker := range s.workers {
		wg.Add(1)
		go s.workingThread(worker, exitSignals, wg)
	}
	go s.monitorThread()
	//start priorirty queue http server
	if s.conf.Priority_queue_http.Enable {
		go s.StartHTTP(s.conf.Priority_queue_http.Port)
	}

	<-exit
	for i := 0; i < len(s.workers); i++ {
		exitSignals <- true
	}
	time.Sleep(MaxWaitBeforeExit)
	for i := 0; i < len(s.workers); i++ {
		s.workers[i].terminate()
	}
	wg.Wait()
	return
}

// workering without dispather
func (s *RServer) workingThread(aWorker *workerWrapper, exit <-chan bool, waitgroup *sync.WaitGroup) {
	for {
		select {
		case <-exit:
			aWorker.loggy.Warnf("worker exited")
			aWorker.status = WStatusStopped
			waitgroup.Done()
			return
		default:
			{
				aWorker.reset()
				aWorker.loggy.Infof("prepare to load")
				var err1 error
				var orderKey, orderBody string

				// try FetchOrder while queue have no messages
				for orderKey == "" {
					aWorker.loggy.Debugf("try to fetch one new message in queue")
					orderKey, orderBody, err1 = s.messageRouter.FetchOrder(s.timeout())
					if err1 != nil {
						aWorker.loggy.WithField("err", err1.Error()).Warnf("fetch message from queue failed with error")
						continue
					}
				}

				// decode message
				conf, ctx, err1 := aWorker.runtimeWorkerInit(orderKey, orderBody)
				if err1 != nil {
					s.logger.WithField("err", err1.Error()).Warnf("decode message failed")
					continue //just ignore this message
				}
				// message deduplicate
				var duplicated bool
				if duplicated, err1 = s.messageRouter.Duplicated(aWorker.ID, aWorker.traceID, s.timeout()); err1 != nil {
					aWorker.loggy.WithError(err1).Warnf("message deduplication failed")
					aWorker.orderError = WarpError(aWorker.orderError, err1.Error())
				}
				if duplicated {
					aWorker.orderStatus = OStatusDuplicated
					aWorker.orderError = WarpError(aWorker.orderError, "message is duplicated")
				} else {
					err1 = aWorker.configuring(conf)
					if err1 != nil {
						aWorker.loggy.WithError(err1).Errorf("configure error")
						aWorker.orderError = WarpError(aWorker.orderError, err1.Error())
					} else {
						aWorker.status = WStatusLoaded // only WStatusLoaded could be processed by workerWrapper.process()
					}
				}
				// processing
				if err := aWorker.process(ctx, s.timeout()); err != nil {
					aWorker.loggy.WithField("error", err.Error()).Errorf("process error")
					aWorker.orderError = WarpError(aWorker.orderError, "process error")
				}
				// result handling
				switch aWorker.orderStatus {
				case OStatusSucceed:
					// only delete the succeed order messages in queue
					if err1 = s.messageRouter.MarkOrderCompleted(aWorker.ID, aWorker.traceID); err1 != nil {
						aWorker.loggy.Errorf("Mark Order Completed failed")
					}
					if aWorker.orderKey != message.PriorityReceiptHandle { //Priority order doesn't need to delete message in sqs order queue
						s.messageRouter.CompleteOrder(aWorker.orderKey)
					}

				case OStatusFailed:
					// Immediately retry for Failed orders
					if aWorker.orderKey != message.PriorityReceiptHandle { //Priority order doesn't need to release message in sqs order queue
						s.messageRouter.ReleaseOrderForRetry(aWorker.orderKey)
					}
					s.messageRouter.DeleteDuplicateKey(aWorker.traceID)
				default: //the message will wait until timeout and return to queue
					aWorker.loggy.WithField("WStatus", aWorker.status.String()).
						WithField("OStatus", aWorker.orderStatus.String()).
						Warnf("Order process finished with unexpected Status")
				}
				s.workerBriefOrder(aWorker)

			}
		}
	}
}

func (s *RServer) monitorThread() {
	expiredAfter := time.Duration(s.conf.StatePulse+1) * time.Second
	Ticker := time.NewTicker(time.Duration(s.conf.StatePulse) * time.Second)
	for {
		select {
		case <-Ticker.C:
			workerStatus := make(map[string]int, len(s.workers))
			s.workerOpLock.Lock()
			for _, w := range s.workers {
				if c, ok := workerStatus[w.status.String()]; ok {
					workerStatus[w.status.String()] = c + 1
				} else {
					workerStatus[w.status.String()] = 1
				}
			}
			s.workerOpLock.Unlock()
			s.messageRouter.EnrollWorkshopStatus(len(s.workers), workerStatus, expiredAfter)
		}
	}
}

//create and send brief message
func (s *RServer) workerBriefOrder(aWorker *workerWrapper) {
	briefMessage, err3 := aWorker.brief()
	if err3 != nil {
		aWorker.loggy.WithField("brief error", err3.Error()).Errorf("create brief message error")
		return
	}
	err := s.messageRouter.BriefOrder(briefMessage)
	if err != nil {
		s.logger.WithField("brief error", err.Error()).Errorf("send brief message error")
	}
}

func (s *RServer) StartHTTP(port string) {
	logFields := log.Fields{
		"application": s.appName,
	}
	s.logger.WithFields(logFields).Info("starting http api")
	handler_assignment := func(w http.ResponseWriter, re *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		message, err := ioutil.ReadAll(re.Body)
		if err != nil {
			s.logger.WithFields(logFields).WithError(err).Error("read request body fail")

			fmt.Fprintf(w, `{"finished":false,"error":"read request body fail"}`)
			return
		}
		if err := s.messageRouter.InsertOrderToPriorityQueue(string(message)); err != nil {
			s.logger.WithFields(logFields).WithError(err).Error("InsertOrderToPriorityQueue fail")
			fmt.Fprintf(w, `{"finished":false,"error":"InsertOrderToPriorityQueue fail"}`)
			return
		}
		fmt.Fprintf(w, `{"finished":true, "error":""}`)

	}
	handler_heartbeat := func(w http.ResponseWriter, re *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"alive": true}`)
	}
	http.HandleFunc("/direct_assign", handler_assignment)
	http.HandleFunc("/heartbeat", handler_heartbeat)
	err := http.ListenAndServe(":"+port, nil)
	if err != nil {
		s.logger.WithFields(logFields).WithError(err).Error("start http fail")
		fmt.Println(err)
	}
}

func WarpError(e error, message string) (newe error) {
	if e == nil {
		newe = errors.New(message)
	} else {
		newe = errors.Wrap(e, message)
	}
	return
}
