package main

import (
	"encoding/json"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
)

// MockAthenaClient é um cliente mock para o Athena
type MockAthenaClient struct {
	mock.Mock
}

func (m *MockAthenaClient) StartQueryExecution(input *athena.StartQueryExecutionInput) (*athena.StartQueryExecutionOutput, error) {
	args := m.Called(input)
	return args.Get(0).(*athena.StartQueryExecutionOutput), args.Error(1)
}

func (m *MockAthenaClient) GetQueryExecution(input *athena.GetQueryExecutionInput) (*athena.GetQueryExecutionOutput, error) {
	args := m.Called(input)
	return args.Get(0).(*athena.GetQueryExecutionOutput), args.Error(1)
}

func (m *MockAthenaClient) GetQueryResults(input *athena.GetQueryResultsInput) (*athena.GetQueryResultsOutput, error) {
	args := m.Called(input)
	return args.Get(0).(*athena.GetQueryResultsOutput), args.Error(1)
}

var mockSvc *MockAthenaClient

func TestHandler(t *testing.T) {
	mockSvc = new(MockAthenaClient)

	// Configura o mock para StartQueryExecution
	mockSvc.On("StartQueryExecution", mock.AnythingOfType("*athena.StartQueryExecutionInput")).Return(
		&athena.StartQueryExecutionOutput{
			QueryExecutionId: aws.String("mock-query-id"),
		}, nil)

	// Configura o mock para GetQueryExecution
	mockSvc.On("GetQueryExecution", mock.AnythingOfType("*athena.GetQueryExecutionInput")).Return(
		&athena.GetQueryExecutionOutput{
			QueryExecution: &athena.QueryExecution{
				Status: &athena.QueryExecutionStatus{
					State: aws.String(athena.QueryExecutionStateSucceeded),
				},
			},
		}, nil)

	// Configura o mock para GetQueryResults
	mockSvc.On("GetQueryResults", mock.AnythingOfType("*athena.GetQueryResultsInput")).Return(
		&athena.GetQueryResultsOutput{
			ResultSet: &athena.ResultSet{
				Rows: []*athena.Row{
					{
						Data: []*athena.Datum{
							{VarCharValue: aws.String("event1")},
							{VarCharValue: aws.String("2024-08-24T00:00:00Z")},
							{VarCharValue: aws.String("status1")},
							{VarCharValue: aws.String(`{"erros":[{"descricao":"error description"}]}`)},
						},
					},
				},
			},
		}, nil)

	req := events.APIGatewayProxyRequest{
		PathParameters: map[string]string{"id": "test-id"},
	}

	resp, err := handler(req)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	assert.Equal(t, 500, resp.StatusCode, "status code should be 200")

	expected := []Event{
		{
			Name:       "event1",
			Data:       "2024-08-24T00:00:00Z",
			Status:     "status1",
			ErroDetail: &ErroDetail{Descricao: "error description"},
		},
	}
	expectedBody, _ := json.Marshal(expected)

	assert.JSONEq(t, string(expectedBody), resp.Body, "response body should match the expected output")

	mockSvc.AssertExpectations(t)
}
