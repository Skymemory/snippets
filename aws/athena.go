package main

import (
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"log"
	"time"
)

type AthenaClient struct {
	svc *athena.Athena
}

func NewAthena(filename, profile, region string) *AthenaClient {
	return &AthenaClient{
		svc: athena.New(session.Must(session.NewSession()), &aws.Config{
			Credentials: credentials.NewSharedCredentials(filename, profile),
			Region:      aws.String(region),
		}),
	}
}

func (client *AthenaClient) ExecuteSql(sql, database, s3Bucket string) (string, error) {
	var input athena.StartQueryExecutionInput
	input.SetQueryString(sql)
	input.SetQueryExecutionContext(&athena.QueryExecutionContext{
		Database: aws.String(database),
	})
	input.SetResultConfiguration(&athena.ResultConfiguration{
		OutputLocation: aws.String("s3://" + s3Bucket),
	})

	result, err := client.svc.StartQueryExecution(&input)
	if nil != err {
		return "", err
	}

	for {
		queryResultInfo, err := client.svc.GetQueryExecution(&athena.GetQueryExecutionInput{
			QueryExecutionId: aws.String(*result.QueryExecutionId),
		})
		if nil != err {
			return "", err
		}
		state := *queryResultInfo.QueryExecution.Status.State
		if state == "SUCCEEDED" {
			return *result.QueryExecutionId, nil
		} else if state == "FAILED" {
			return "", errors.New(*queryResultInfo.QueryExecution.Status.StateChangeReason)
		}
		log.Println("waiting")
		time.Sleep(5 * time.Second)
	}
}

func (client *AthenaClient) QueryResult(queryId string) ([]map[string]string, error) {
	resultSet, err := client.svc.GetQueryResults(&athena.GetQueryResultsInput{
		QueryExecutionId: aws.String(queryId),
	})

	if nil != err {
		return nil, err
	}

	if nil == resultSet {
		return nil, nil
	}

	var rs []map[string]string

	for i := range resultSet.ResultSet.Rows {
		if i == 0 {
			continue
		}

		temp := make(map[string]string, len(resultSet.ResultSet.Rows[i].Data))
		for j := range resultSet.ResultSet.Rows[i].Data {
			ptr := resultSet.ResultSet.Rows[i].Data[j].VarCharValue
			key := *resultSet.ResultSet.Rows[0].Data[j].VarCharValue
			if nil == ptr {
				temp[key] = "NULL"
			} else {
				temp[key] = *ptr
			}
		}

		rs = append(rs, temp)
	}

	return rs, nil
}
