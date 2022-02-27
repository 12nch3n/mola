package main

import (
	"fmt"
	"os"

	"github.com/enriqueChen/mola/demo"
	"github.com/enriqueChen/mola/runtime"
)

func main() {
	appName := os.Args[1]
	fName := os.Args[2]
	var s *runtime.RServer
	var err error
	if s, err = runtime.NewServer(fName); err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	exit := make(chan bool, 1)
	sigs := make(chan os.Signal, 1)
	go func() {
		sig := <-sigs
		fmt.Println(sig.String())
		exit <- true
	}()
	err = s.Run(appName, demo.EWorkerCreator, exit)
	fmt.Println(err.Error())
	return
}
