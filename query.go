package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	"cloud.google.com/go/bigquery"

	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type JsonResponse struct {
	Type    string           `json:"type"`
	Data    []bigquery.Value `json:"data"`
	Message string           `json:"message"`
}

func Query(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	projectID := "bq-project-poc"
	serviceAccount := "bq-project-poc.json"
	client, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsFile(serviceAccount))
	if err != nil {
		panic(err)
	}
	query := r.FormValue("query")
	var response = JsonResponse{}
	if query == "" {
		response = JsonResponse{Type: "error", Message: "You are missing query parameter."}
	} else {
		q := client.Query(query)
		fmt.Println("Executing query: ", query)
		it, err := q.Read(ctx)
		if err != nil {
			panic(err)
		}
		type r []bigquery.Value
		result := r{}
		for {
			var row []bigquery.Value
			err := it.Next(&row)
			if err == iterator.Done {
				break
			}
			if err != nil {
				panic(err)
			}

			result = append(result, row)
		}
		response = JsonResponse{Type: "success", Data: result}
	}
	json.NewEncoder(w).Encode(response)
}
