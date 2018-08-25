package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	_ "github.com/lib/pq"
)

type Event struct {
	Datetime string `json:"datetime"`
	Filename string `json:"filename"`
	Status   string `json:"status"`
	Error    string `json:"error"`
}

type Response struct {
	Status string `json:"status"`
}

func OutputJson(ctx context.Context, event Event) (Response, error) {
	creds := credentials.NewStaticCredentials("AWS_KEY", "AWS_SECRET_KEY", "")
	config := aws.Config{
		Credentials: creds,
		Region:      aws.String("ap-northeast-1"),
	}
	sess, err := session.NewSession(&config)
	if err != nil {
		return Response{Status: "NG"}, errors.New("cannot create new session")
	}
	svc := s3.New(sess)
	resp, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String("output_bucketname"),
		Key:    aws.String("log.json"),
	})
	if err != nil {
		return Response{Status: "NG"}, errors.New("cannot get s3 obj")
	}
	logData := new(bytes.Buffer)
	logData.ReadFrom(resp.Body)
	jsonBytes := []byte(logData.String())
	var data []Event
	if err := json.Unmarshal(jsonBytes, &data); err != nil {
		panic("JSON Unmarshal error")
	}
	data = append(data, event)
	dataByte, err := json.Marshal(data)
	if err != nil {
		panic("JSON Marshal error")
	}
	_, err = svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String("output_bucketname"),
		Key:    aws.String("log.json"),
		Body:   bytes.NewReader(dataByte),
	})
	if err != nil {
		panic("JSON Marshal error")
	}
	if event.Status != "OK" {
		err = errors.New("task failed due to:" + event.Error)
	}
	return Response{Status: event.Status}, err
}

func main() {
	lambda.Start(OutputJson)
}
