package main

import (
	"bytes"
	"fmt"
	"os"

	"github.com/BurntSushi/toml"
	"github.com/urfave/cli"
	"github.com/enriqueChen/mola/adaptor"
	configure "github.com/enriqueChen/mola/config"
)

func main() {
	app := cli.NewApp()
	app.Name = "gmola"
	app.Version = "0.1.0"
	app.Usage = "deploy message queue topics and redis configurations for application"
	deployFlags := []cli.Flag{
		cli.StringFlag{
			Name:     "app, a",
			Usage:    "Specify the application name for deployment",
			Required: true,
		},
		cli.StringFlag{
			Name:     "config, c",
			Usage:    "Specify the configuration file for deployment",
			Required: true,
		},
	}
	app.Commands = []cli.Command{
		cli.Command{
			Name:     "genconf",
			Aliases:  []string{"g"},
			Usage:    "generate and print example configure file",
			Category: "generate",
			Subcommands: []cli.Command{
				cli.Command{
					Name:    "adaptor",
					Aliases: []string{"a"},
					Action:  demoAdaptorConf,
				},
				cli.Command{
					Name:    "service",
					Aliases: []string{"s"},
					Action:  demoServiceConf,
				},
			},
		},
		cli.Command{
			Name:     "check",
			Aliases:  []string{"c"},
			Usage:    "check the infrastructures for application with APP name & adaptor configure",
			Category: "check",
			Flags:    deployFlags,
			Action:   check,
		},
		cli.Command{
			Name:     "deploy",
			Aliases:  []string{"d"},
			Usage:    "deploy the infrastructures for application with APP name & adaptor configure",
			Category: "deploy",
			Flags:    deployFlags,
			Action:   deploy,
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		fmt.Printf(err.Error())
		os.Exit(1)
	}
}

func demoAdaptorConf(c *cli.Context) (err error) {
	conf := configure.RuntimeOrdersConf{
		Logs: configure.LogConf{
			Level:  "debug",
			Format: "txt",
			Rotate: &configure.RotateHook{
				File:         "./mola.log.%Y%m%d%H%M",
				Link:         "./mola.log",
				RotationHors: 24,
				KeepRotation: 7,
			},
		},
		Sqs: configure.AWSSQSConf{
			Region:  "us-east-1",
			App:     "Test",
			Project: "Mola",
			Service: "Demo",
		},
		Redis: configure.RedisConf{
			Addr:     "redis.test.local:6379",
			Password: "",
			DB:       0,
		},
		Orders: []configure.OrderConf{
			configure.OrderConf{
				Name:    "DemoOrder1",
				Timeout: 180,
				Retry:   3,
			},
		},
	}
	var b bytes.Buffer
	toml.NewEncoder(&b).Encode(conf)
	fmt.Println(b.String())
	return
}

func demoServiceConf(c *cli.Context) (err error) {
	conf := configure.WConf{
		Capacity:   5,
		StatePulse: 30,
		Logs: configure.LogConf{
			Level: "debug",
			Rotate: &configure.RotateHook{
				File:         "./mola.log.%Y%m%d%H%M",
				Link:         "./mola.log",
				RotationHors: 24,
				KeepRotation: 7,
			},
		},
		Sqs: configure.AWSSQSConf{
			Region:  "us-east-1",
			App:     "Test",
			Project: "Mola",
			Service: "Demo",
		},
		Redis: configure.RedisConf{
			Addr:     "redis.test.local:6379",
			Password: "",
			DB:       0,
		},
	}
	var b bytes.Buffer
	toml.NewEncoder(&b).Encode(conf)
	fmt.Println(b.String())
	return
}

func deploy(c *cli.Context) (err error) {
	_, err = adaptor.DeployApp(c.String("app"), c.String("config"))
	return
}

func check(c *cli.Context) (err error) {
	// Check queue url by test loading
	_, _, err = adaptor.LoadRouter(c.String("app"), c.String("config"))
	return
}
