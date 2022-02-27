package utils

import (
	"github.com/sirupsen/logrus"
)

// LogrusProxy ...
type LogrusProxy struct {
	Logger *logrus.Logger
}

// Log is a utility function to comply with the AWS signature
func (proxy *LogrusProxy) Log(args ...interface{}) {
	proxy.Logger.Info(args...)
}
