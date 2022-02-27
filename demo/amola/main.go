package main

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/enriqueChen/mola/adaptor"
	"github.com/enriqueChen/mola/demo"
)

type task struct {
	ID       int64
	ConfItem string
	Foo      string
	Bar      string
}

var (
	s1 = rand.NewSource(time.Now().UnixNano())
)

// BoundOrder implements to return the bounding order name to verify the order message
func (t *task) BoundOrder() string {
	return "DemoOrder1"
}

// Publish the order body to send it to message queue
func (t *task) Publish() (contract, traceID string, conf, body []byte, err error) {
	var (
		confPb demo.DemoConf
		bodyPb demo.DemoOrder
	)
	confPb = demo.DemoConf{
		ConfItem1: t.ConfItem,
		ConfItem2: rand.New(s1).Int63n(int64(10)),
	}
	bodyPb = demo.DemoOrder{
		Foo: t.Foo,
		Bar: t.Bar,
	}
	traceID = strconv.FormatInt(t.ID, 10)
	contract = "UNDEFINED"
	if conf, err = proto.Marshal(&confPb); err != nil {
		return
	}
	body, err = proto.Marshal(&bodyPb)
	return
}

func printBrief(contract, orderType, traceID, ret, ctmRes string, duration float64, errMsg string) {
	fmt.Printf("\033[33m Task Processed:\tcontract:%s\torderType:%s\ttraceID:%s\tresult:%s\ncustomStr:%s\tduration:%.2f(s)\nerr:%s\n \033[0m",
		contract, orderType, traceID, ret, ctmRes, duration, errMsg)
}

func printFailure(contract, orderType, traceID string, conf, body []byte, jsonFormatOrderMessage string) {
	fmt.Printf("\033[31m Task Process Failed after auto retry:\tcontract: %s\torderType: %s\ttraceID: %s\nconf:%s\nbody:%s\n orderMessage:%s\n\033[0m",
		contract, orderType, traceID, string(conf), string(body), jsonFormatOrderMessage)

	dirbase := "./"
	outputpath := filepath.Join(dirbase, orderType+"_"+traceID+"_"+time.Now().Format("20060102150405"))
	_ = os.MkdirAll(dirbase, os.ModePerm)
	ioutil.WriteFile(outputpath, []byte(jsonFormatOrderMessage), 0644)

}

func main() {

	appRouter, _, err := adaptor.LoadRouter(os.Args[1], os.Args[2])
	appRouter.HandleBrief = printBrief
	appRouter.HandleFailedMsg = printFailure
	if err != nil {
		fmt.Printf("%s\n", err.Error())
		os.Exit(-1)
	}
	for i := int64(0); i < 10; i++ {
		mTask := &task{
			ID:       rand.New(s1).Int63(),
			ConfItem: fmt.Sprintf("conf_%d", i),
			Foo:      fmt.Sprintf("Foo_%d", i),
			Bar:      fmt.Sprintf("Bar_%d", i),
		}
		fmt.Printf("%v\n", mTask)
		if err := appRouter.SendOrder(mTask); err != nil {
			fmt.Printf("%s\n", err.Error())
		}
	}
	//if need to get message number of queue
	//jsonstring,_:=appRouter.GetMessageNumInQueue()
	//go appRouter.StartMoniterService()
	exit := make(chan bool, 1)
	sigs := make(chan os.Signal, 1)
	go func() {
		sig := <-sigs
		fmt.Println(sig.String())
		exit <- true
	}()

	appRouter.ResultHandle(exit)

}
