package main

import (
	"context"
	"encoding/json"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	_ "github.com/lib/pq"
)

type Event struct {
	Tablename  string `json:"tablename"`
	Columnlist string `json:"columnlist"`
	Status     string `json:"status"`
	Error      Error  `json:"error"`
}

type Error struct {
	Error string
	Cause string
}

type Response struct {
	Datetime string `json:"datetime"`
	Filename string `json:"filename"`
	Status   string `json:"status"`
	Error    string `json:"error"`
}

func Fallback(ctx context.Context, event Event) (Response, error) {
	timestr := time.Now().In(time.FixedZone("Asia/Tokyo", 9*60*60)).Format("2006/01/02 15:04:05")
	tableName := event.Tablename
	jsonStr := event.Error.Cause
	jsonByte := []byte(jsonStr)
	var data interface {
	}
	if err := json.Unmarshal(jsonByte, &data); err != nil {
		panic("JSON Unmarshal error")
	}
	return Response{Datetime: timestr, Filename: tableName + ".csv", Status: "NG", Error: "[Aurora] something went wrong: " + data.(map[string]interface{})["errorMessage"].(string)}, nil
}

func main() {
	lambda.Start(Fallback)
}
