package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
)

type AthenaClient interface {
	StartQueryExecution(input *athena.StartQueryExecutionInput) (*athena.StartQueryExecutionOutput, error)
	GetQueryExecution(input *athena.GetQueryExecutionInput) (*athena.GetQueryExecutionOutput, error)
	GetQueryResults(input *athena.GetQueryResultsInput) (*athena.GetQueryResultsOutput, error)
}

type Event struct {
	Name       string      `json:"name"`
	Data       string      `json:"data"`
	Status     string      `json:"status"`
	ErroDetail *ErroDetail `json:"erro"`
}

type ErroDetail struct {
	Descricao string `json:"descricao"`
}

func main() {
	lambda.Start(handler)
}

func handler(req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	id := req.PathParameters["id"]

	if id == "" {
		return events.APIGatewayProxyResponse{StatusCode: 400, Body: "id obrigatorio"}, nil
	}

	sess := session.Must(session.NewSession())
	svc := athena.New(sess, aws.NewConfig().WithRegion("us-west-2"))

	athenaResult, err := queryAthena(svc, id)
	if err != nil {
		log.Println("Erro ao consultar Athena: ", err)
		return events.APIGatewayProxyResponse{StatusCode: 500, Body: "Erro ao consultar Athena"}, nil
	}

	res, err := json.Marshal(athenaResult)
	if err != nil {
		log.Println("Erro ao serializar resultado: ", err)
		return events.APIGatewayProxyResponse{StatusCode: 500, Body: "Erro ao serializar resultado"}, nil
	}

	return events.APIGatewayProxyResponse{StatusCode: 200, Body: string(res)}, nil
}

func queryAthena(svc AthenaClient, queryID string) ([]Event, error) {
	queryString := fmt.Sprintf("SELECT * FROM tb_teste WHERE id='%s'", queryID)

	queryExecutionInput := &athena.StartQueryExecutionInput{
		QueryString: aws.String(queryString),
		ResultConfiguration: &athena.ResultConfiguration{
			OutputLocation: aws.String("s3://bucket-teste/query-results/"),
		},
	}

	result, err := svc.StartQueryExecution(queryExecutionInput)
	if err != nil {
		return nil, err
	}

	queryExecutionID := result.QueryExecutionId

	results, err := getQueryResults(svc, queryExecutionID)
	if err != nil {
		return nil, err
	}

	return results, nil
}

func getQueryResults(svc AthenaClient, queryExecutionID *string) ([]Event, error) {
	maxWaitTime := 25
	waitInterval := 1

	for elapsed := 0; elapsed < maxWaitTime; elapsed += waitInterval {

		statusInput := &athena.GetQueryExecutionInput{
			QueryExecutionId: queryExecutionID,
		}

		result, err := svc.GetQueryExecution(statusInput)
		if err != nil {
			return nil, err
		}

		status := *result.QueryExecution.Status.State

		if status == athena.QueryExecutionStateSucceeded {
			return fetchResults(svc, queryExecutionID)
		} else if status == athena.QueryExecutionStateFailed || status == athena.QueryExecutionStateCancelled {
			log.Fatalln("Query com status de falha/cancelada: ", status)
			return nil, err
		}

		time.Sleep(time.Duration(waitInterval) * time.Second)
	}

	return nil, errors.New("query nao concluida dentro dos 25s")
}

func fetchResults(svc AthenaClient, queryExecutionID *string) ([]Event, error) {
	input := &athena.GetQueryResultsInput{
		QueryExecutionId: queryExecutionID,
	}

	results, err := svc.GetQueryResults(input)
	if err != nil {
		return nil, err
	}

	var eventsResult []Event

	for _, row := range results.ResultSet.Rows[1:] {
		evento := *row.Data[0].VarCharValue
		dataHora := *row.Data[1].VarCharValue
		status := *row.Data[2].VarCharValue
		erro := *row.Data[3].VarCharValue

		var txtInfo map[string]interface{}

		if err := json.Unmarshal([]byte(erro), &txtInfo); err != nil {
			return nil, err
		}

		var erroDetail *ErroDetail

		if erros, ok := txtInfo["erros"].([]interface{}); ok {
			for _, e := range erros {
				if erroMap, ok := e.(map[string]interface{}); ok {
					if desc, ok := erroMap["descricao"].(string); ok {
						erroDetail = &ErroDetail{Descricao: desc}
						break
					}
				}
			}
		}

		event := Event{
			Name:       evento,
			Data:       dataHora,
			Status:     status,
			ErroDetail: erroDetail,
		}
		eventsResult = append(eventsResult, event)
	}

	return eventsResult, nil
}
