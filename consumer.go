package sqs

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type HandlerFunc func(message *Message)

type Message struct {
	Event   	string			`json:"event"`
	Payload 	map[string]interface{}	`json:"payload"`
}

func Consume(queueUrl string, handlerFunc HandlerFunc) {
	go func(url string) {
		var region string
		if os.Getenv("AWS_REGION") != "" {
			region = os.Getenv("AWS_REGION")
		} else {
			region = "us-east-1"
		}

		svc := sqs.New(session.New(), aws.NewConfig().WithRegion(region))

		params := &sqs.ReceiveMessageInput{
			QueueUrl:		aws.String(url),
			AttributeNames:		[]*string{},
			MaxNumberOfMessages: 	aws.Int64(1),
			MessageAttributeNames:	[]*string{ aws.String(".*"), },
			VisibilityTimeout:	aws.Int64(20),
			WaitTimeSeconds:   	aws.Int64(10),
		}

		for {
			response, err := svc.ReceiveMessage(params)

			if err != nil {
				fmt.Println("Encountered error while polling SQS queue; " + err.Error())
				return
			} else {
				if len(response.Messages) > 0 {
					for _, msg := range response.Messages {
						var message *Message
						decoder := json.NewDecoder(strings.NewReader(*msg.Body))
						err := decoder.Decode(&message)
						if err == nil {
							handlerFunc(message)
							svc.DeleteMessage(&sqs.DeleteMessageInput{
								QueueUrl:      aws.String(url),
								ReceiptHandle: msg.ReceiptHandle,
							})
						} else {
							fmt.Println("Encountered error while parsing SQS message; " + err.Error())
						}
					}
				} else {
					fmt.Println("SQS polling returned 0 messages")
				}
			}
		}
	}(queueUrl)
}
