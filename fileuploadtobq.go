package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"cloud.google.com/go/bigquery"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
)

// FileUploadToBQ demonstrates loading data into a BigQuery table using a CSV file on the local filesystem.
func FileUploadToBQ(w http.ResponseWriter, r *http.Request) {
	datasetID := r.FormValue("dataset_name")
	tableID := r.FormValue("table_name")
	filePath := r.FormValue("filepath")
	loadType := r.FormValue("loadtype")
	deLimiter := r.FormValue("delimiter")

	var response = JsonResponse{}

	projectID := "bq-project-poc"
	serviceAccount := "bq-project-poc.json"

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsFile(serviceAccount))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	if datasetID == "" || tableID == "" || filePath == "" || loadType == "" || deLimiter == "" {
		response = JsonResponse{Type: "error", Message: "You are missing datasetID or tableD or filePath or loadType or deLimiter parameters"}
	} else {
		f, err := os.Open(filePath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error opening file: %v\n", err)
		}
		source := bigquery.NewReaderSource(f)
		source.AutoDetect = true   // Allow BigQuery to determine schema.
		source.SkipLeadingRows = 1 // CSV has a single header line.
		source.FieldDelimiter = deLimiter

		loader := client.Dataset(datasetID).Table(tableID).LoaderFrom(source)
		loader.CreateDisposition = bigquery.CreateIfNeeded

		if strings.ToLower(loadType) == "replace" {
			loadType = "replace"
			loader.WriteDisposition = bigquery.WriteTruncate
		} else if strings.ToLower(loadType) == "append" {
			loadType = "append"
			loader.WriteDisposition = bigquery.WriteAppend
		} else {
			loadType = "replace"
			loader.WriteDisposition = bigquery.WriteTruncate
		}

		fmt.Println("Uploading file to BO: ", filePath)

		job, err := loader.Run(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error in job loading: %v\n", err)
		}
		status, err := job.Wait(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error during job wait: %v\n", err)
		}
		if status.Err() != nil {
			fmt.Fprintf(os.Stderr, "error in job status: %v\n", err)
		}

		response = JsonResponse{Type: "success", Message: "The file has been uploaded to Big Query successfully!"}
	}

	json.NewEncoder(w).Encode(response)

}
