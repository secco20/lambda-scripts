package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sfn"
	_ "github.com/lib/pq"
)

type Response struct {
	Status string `json:"status"`
}

func TriggerStepFunction(ctx context.Context, s3Event events.S3Event) (Response, error) {
	creds := credentials.NewStaticCredentials("AWS_KEY", "AWS_SECRET_KEY", "")
	config := aws.Config{
		Credentials: creds,
		Region:      aws.String("ap-northeast-1"),
	}
	sess, err := session.NewSession(&config)
	svc := sfn.New(sess)
	fmt.Printf("%v\n", s3Event)
	input, err := json.Marshal(s3Event)
	if err != nil {
		fmt.Println("JSON Marshal error:", err)
		return Response{Status: "NG"}, err
	}
	timestr := time.Now().In(time.FixedZone("Asia/Tokyo", 9*60*60)).Format("2006/01/02 15:04:05")
	params := sfn.StartExecutionInput{
		Input:           aws.String(string(input)),
		Name:            aws.String("testrun" + timestr),
		StateMachineArn: aws.String("arn:aws:states:ap-northeast-1:xxxxxxxxxxxx:stateMachine:MyStateMachine"),
	}
	fmt.Println(params)
	resp, err := svc.StartExecution(&params)
	if err != nil {
		fmt.Println("err on sending req to step function")
		return Response{Status: "NG"}, err
	}
	fmt.Println(resp)
	return Response{Status: "OK"}, nil
}

func main() {
	lambda.Start(TriggerStepFunction)
}
