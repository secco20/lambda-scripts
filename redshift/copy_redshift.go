package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	_ "github.com/lib/pq"
)

type Response struct {
	TableName  string `json:"tablename"`
	ColumnList string `json:"columnlist"`
	Status     string `json:"status"`
	Error      string `json:"error"`
}

func CopyRedshift(ctx context.Context, s3Event events.S3Event) (Response, error) {
	var bucketName string
	var objectKey string
	errmsg := ""
	columnList := ""
	for _, record := range s3Event.Records {
		s3 := record.S3
		fmt.Printf("[%s - %s] Bucket = %s, Key = %s \n", record.EventSource, record.EventTime, s3.Bucket.Name, s3.Object.Key)
		bucketName = s3.Bucket.Name
		objectKey = s3.Object.Key
	}
	tableName := strings.TrimRight(objectKey, ".csv")
	connStr := "postgres://dbuser:dbpass@dbhost:dbport/dbname"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		errmsg = "[Redshift] error on DB connection: "
		return Response{TableName: tableName, ColumnList: columnList, Status: "NG", Error: errmsg + err.Error()}, nil
	}
	tx, err := db.Begin()
	if err != nil {
		errmsg = "[Redshift] error on transaction begin: "
		return Response{TableName: tableName, ColumnList: columnList, Status: "NG", Error: errmsg + err.Error()}, nil
	}
	
	accessKey := "AWS_KEY"
	secretKey := "AWS_SECRET_KEY"
	queryStr := fmt.Sprintf("COPY %s ", tableName)
	queryStr += fmt.Sprintf("FROM 's3://%s/%s' ", bucketName, objectKey)
	queryStr += fmt.Sprintf("CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s' ", accessKey, secretKey)
	queryStr += "CSV IGNOREHEADER 1 TIMEFORMAT 'auto' DELIMITER ','; "
	fmt.Printf("QUERYSTRING: %s\n", queryStr)
	_, err = db.Exec(queryStr)
	if err != nil {
		tx.Rollback()
		errmsg = "[Redshift] error on query :"
		return Response{TableName: tableName, ColumnList: columnList, Status: "NG", Error: errmsg + err.Error()}, nil
	}
	tx.Commit()
	queryStr = fmt.Sprintf("SELECT ordinal_position, column_name, data_type FROM information_schema.columns where table_name='%s' order by ordinal_position asc", tableName)
	rows, err := db.Query(queryStr)
	if err != nil {
		fmt.Println("err on select columns")
	}
	index, colName, dataType := 0, "", ""
	for rows.Next() {
		err = rows.Scan(&index, &colName, &dataType)
		if err != nil {
			errmsg = "[Redshift] error on scan data: "
			return Response{TableName: tableName, ColumnList: columnList, Status: "NG", Error: errmsg + err.Error()}, nil
		}
		if index != 1 {
			columnList += ", "
		}
		columnList += fmt.Sprintf("%s %s", colName, dataType)
	}

	return Response{TableName: tableName, ColumnList: columnList, Status: "OK", Error: errmsg}, nil
}

func main() {
	lambda.Start(CopyRedshift)
}
