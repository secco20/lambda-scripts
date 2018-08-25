package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	_ "github.com/lib/pq"
)

type Event struct {
	TableName  string
	ColumnList string
	Status     string
	Error      string
}

type Response struct {
	Datetime string `json:"datetime"`
	Filename string `json:"filename"`
	Status   string `json:"status"`
	Error    string `json:"error"`
}

func InsertAurora(ctx context.Context, event Event) (Response, error) {
	timestr := time.Now().In(time.FixedZone("Asia/Tokyo", 9*60*60)).Format("2006/01/02 15:04:05")
	tableName := event.TableName
	columnList := event.ColumnList
	if event.Status == "NG" {
		return Response{Datetime: timestr, Filename: tableName + ".csv", Status: "NG", Error: event.Error}, nil
	}
	connStr := "postgres://dbuser:dbpass@dbhost:dbport/dbname"
	errmsg := "-"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		errmsg = "[Aurora] error on DB connection: "
		return Response{Datetime: timestr, Filename: tableName + ".csv", Status: "NG", Error: errmsg + err.Error()}, nil

	}
	tx, err := db.Begin()
	if err != nil {
		errmsg = "[Aurora] error on transaction begin: "
		return Response{Datetime: timestr, Filename: tableName + ".csv", Status: "NG", Error: errmsg + err.Error()}, nil
	}

	queryStr := fmt.Sprintf("INSERT INTO %s SELECT * FROM DBLINK(", tableName)
	queryStr += fmt.Sprintf("'foreign_server', $REDSHIFT$ SELECT * FROM %s $REDSHIFT$) as t1 (%s)", tableName, columnList)
	fmt.Printf("QUERYSTRING: %s\n", queryStr)
	_, err = db.Exec(queryStr)
	if err != nil {
		tx.Rollback()
		errmsg = "[Aurora] error on query: "
		return Response{Datetime: timestr, Filename: tableName + ".csv", Status: "NG", Error: errmsg + err.Error()}, nil
	}
	tx.Commit()
	return Response{Datetime: timestr, Filename: tableName + ".csv", Status: "OK", Error: errmsg}, nil
}

func main() {
	lambda.Start(InsertAurora)
}
