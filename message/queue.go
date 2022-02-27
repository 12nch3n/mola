package message

import (
	"encoding/base64"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	awsSess "github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	configure "github.com/enriqueChen/mola/config"
	"github.com/enriqueChen/mola/utils"
)

const (
	taskVisiableGap = int64(10)
	rcvWaitSecond   = int64(20)
)

// AWSSQS struct provides the aws simple queue service
type AWSSQS struct {
	Conf   *configure.AWSSQSConf
	Logger *log.Logger
}

func (qs *AWSSQS) newSQSSvc() (ret *sqs.SQS) {
	var (
		session *awsSess.Session
		config  *aws.Config
	)
	config = aws.NewConfig().WithRegion(qs.Conf.Region)
	if qs.Logger != nil {
		config.WithLogger(&utils.LogrusProxy{Logger: qs.Logger})
	}
	session = awsSess.Must(awsSess.NewSession(config))

	ret = sqs.New(session)
	ret.Retryer = &client.DefaultRetryer{
		NumMaxRetries: 3,
	}
	return
}

//CreateQueue Create a queue with specified name with task timeout with it's daed queue for max retried task
func (qs *AWSSQS) CreateQueue(name string, taskTimeout int64, taskRetry int64, dqURL *string) (qURL *string, err error) {
	var (
		svc       *sqs.SQS
		input     *sqs.CreateQueueInput
		output    *sqs.CreateQueueOutput
		dqAttr    *sqs.GetQueueAttributesOutput
		logFields map[string]interface{}
	)

	logFields = log.Fields{
		"function":              "create queue",
		"queue name":            name,
		"dead letter queue URL": dqURL,
	}
	log.WithFields(logFields).Debugf("Create queue")
	svc = qs.newSQSSvc()
	var redrivePolicy *string
	if dqURL != nil {
		dqAttr, err = svc.GetQueueAttributes(&sqs.GetQueueAttributesInput{
			QueueUrl: aws.String(*dqURL),
			AttributeNames: []*string{
				aws.String("All"),
			},
		})
		if err == nil {
			redrivePolicy = aws.String(fmt.Sprintf("{\"deadLetterTargetArn\":\"%s\",\"maxReceiveCount\":%d}", *dqAttr.Attributes["QueueArn"], taskRetry))
		} else {
			qs.Logger.WithFields(logFields).
				Errorf("get Dead letter queue attribute failed")
			err = errors.Wrapf(err, "get Dead letter queue attribute failed")
			return
		}
		qs.Logger.WithFields(logFields).
			WithField("dead letter queue ARN", *dqAttr.Attributes["QueueArn"]).
			Debugf("get Dead letter queue attribute")
	}
	input = &sqs.CreateQueueInput{
		QueueName: aws.String(name),
		Attributes: map[string]*string{
			"VisibilityTimeout":             aws.String(strconv.FormatInt(taskTimeout+taskVisiableGap, 10)),
			"ReceiveMessageWaitTimeSeconds": aws.String("20"),
		},
	}
	if redrivePolicy != nil {
		input.Attributes["RedrivePolicy"] = redrivePolicy
	}
	output, err = svc.CreateQueue(input)
	if err != nil {
		qs.Logger.WithFields(logFields).
			Errorf("create queue failed")
		err = errors.Wrapf(err, "create queue failed")
	}
	qURL = output.QueueUrl
	svc.TagQueue(&sqs.TagQueueInput{
		QueueUrl: qURL,
		Tags: map[string]*string{
			"Project": aws.String(qs.Conf.Project),
			"App":     aws.String(qs.Conf.App),
			"Service": aws.String(qs.Conf.Service),
		},
	})
	qs.Logger.WithFields(logFields).Info("create queue succeed")
	return
}

// DeleteQueue delete specified queue
func (qs *AWSSQS) DeleteQueue(qURL string) (err error) {
	var (
		svc       *sqs.SQS
		qInput    *sqs.DeleteQueueInput
		logFields map[string]interface{}
	)
	logFields = log.Fields{"queue url": qURL}
	svc = qs.newSQSSvc()
	qInput = &sqs.DeleteQueueInput{
		QueueUrl: aws.String(qURL),
	}

	_, err = svc.DeleteQueue(qInput)
	if err != nil {
		err = errors.Wrap(err, "delete queue failed")
		qs.Logger.WithFields(logFields).WithField("error", err.Error()).Errorf("delete queue failed")
	}
	qs.Logger.WithFields(logFields).Warn("delete queue")
	return
}

// SendMessage send a message to a queue
func (qs *AWSSQS) SendMessage(qURL string, mBody string) (err error) {
	var (
		svc       *sqs.SQS
		sMsgInput *sqs.SendMessageInput
		logFields map[string]interface{}
	)
	logFields = log.Fields{"queue": qURL}
	mBodyEncode := aws.String(base64.StdEncoding.EncodeToString([]byte(mBody)))
	sMsgInput = &sqs.SendMessageInput{
		MessageBody: mBodyEncode,
		QueueUrl:    aws.String(qURL),
	}
	qs.Logger.WithFields(logFields).WithField("messageBody", mBody).Debugf("send message")
	svc = qs.newSQSSvc()
	_, err = svc.SendMessage(sMsgInput)
	return
}

// ReceiveMessages receive message from a queue
func (qs *AWSSQS) ReceiveMessages(qURL string, maxMsg int64, vTimeout int64) (mBodys map[string]string, err error) {
	var (
		svc        *sqs.SQS
		rMsgInput  *sqs.ReceiveMessageInput
		rMsgOutput *sqs.ReceiveMessageOutput
		logFields  map[string]interface{}
	)
	logFields = log.Fields{"queue": qURL, "max_number": maxMsg}
	rMsgInput = &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(qURL),
		MaxNumberOfMessages: aws.Int64(maxMsg),
		VisibilityTimeout:   aws.Int64(vTimeout),
		WaitTimeSeconds:     aws.Int64(rcvWaitSecond),
	}
	svc = qs.newSQSSvc()
	rMsgOutput, err = svc.ReceiveMessage(rMsgInput)
	if len(rMsgOutput.Messages) == 0 || err != nil {
		return
	}
	mBodys = make(map[string]string, len(rMsgOutput.Messages))
	for _, msg := range rMsgOutput.Messages {
		var bodyDecode []byte
		if bodyDecode, err = base64.StdEncoding.DecodeString(*msg.Body); err != nil {
			qs.Logger.WithFields(logFields).
				WithField("messageReceiptHandle", *msg.ReceiptHandle).
				WithField("messageBody", *msg.Body).
				WithField("error", err.Error()).
				Errorf("base64 decode message failed")
			return
		}
		mBodys[*msg.ReceiptHandle] = string(bodyDecode)
		qs.Logger.WithFields(logFields).
			WithField("messageReceiptHandle", *msg.ReceiptHandle).
			WithField("messageBody", *msg.Body).
			Debugf("recieve message")
	}
	return
}

// DeleteMessage delete message in queue when task finished
func (qs *AWSSQS) DeleteMessage(qURL string, receiptHandle string) (err error) {
	var (
		svc       *sqs.SQS
		dMsgInput *sqs.DeleteMessageInput
		logFields map[string]interface{}
	)
	logFields = log.Fields{"queue": qURL, "receipt handle": receiptHandle}
	svc = qs.newSQSSvc()
	dMsgInput = &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(qURL),
		ReceiptHandle: aws.String(receiptHandle),
	}
	_, err = svc.DeleteMessage(dMsgInput)
	qs.Logger.WithFields(logFields).
		Debugf("delete message")
	return
}

// ChangeMessageVisibility delete message in queue when task finished
func (qs *AWSSQS) ChangeMessageVisibility(qURL string, receiptHandle string, vTimeout int64) (err error) {
	var (
		svc       *sqs.SQS
		vMsgInput *sqs.ChangeMessageVisibilityInput
		logFields map[string]interface{}
	)
	logFields = log.Fields{"queue": qURL, "receipt handle": receiptHandle, "timout": vTimeout}
	svc = qs.newSQSSvc()
	vMsgInput = &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(qURL),
		ReceiptHandle:     aws.String(receiptHandle),
		VisibilityTimeout: aws.Int64(vTimeout),
	}
	_, err = svc.ChangeMessageVisibility(vMsgInput)
	qs.Logger.WithFields(logFields).
		Debugf("change message visibility timeout")
	return
}

// GetMessageNumInQueue get the num of message available and message in flight  of a queue
func (qs *AWSSQS) GetMessageNumInQueue(qURL string) (ret map[string]int, err error) {
	var (
		svc *sqs.SQS

		logFields map[string]interface{}
	)
	logFields = log.Fields{"queue": qURL}

	svc = qs.newSQSSvc()
	gMsgNumInput := &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(qURL),
		AttributeNames: []*string{
			aws.String("ApproximateNumberOfMessages"),
			aws.String("ApproximateNumberOfMessagesNotVisible"),
		},
	}
	result, err := svc.GetQueueAttributes(gMsgNumInput)
	if err != nil {
		qs.Logger.WithError(err).WithFields(logFields).Errorf("Get messages num of queue fail")
		return
	}
	ret = make(map[string]int)
	for attr, val := range result.Attributes {

		ret[attr], _ = strconv.Atoi(aws.StringValue(val))
	}
	return
}
