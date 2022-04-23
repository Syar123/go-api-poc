package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/spanner"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
)

// FileUploadToBQSpanner demonstrates loading data into a BigQuery table using a CSV file from local filesystem and inserting/updating row entry in spanner DB.
func FileUploadToBQSpanner(w http.ResponseWriter, r *http.Request) {
	datasetID := r.FormValue("dataset_name")
	tableID := r.FormValue("table_name")
	filePath := r.FormValue("filepath")
	loadType := r.FormValue("loadtype")
	deLimiter := r.FormValue("delimiter")

	var response = JsonResponse{}

	projectID := "bq-project-poc"
	serviceAccount := "bq-project-poc.json"
	spannerDB := "spanner-test-db"
	spannerMYSQL := "spanner-test-mysql"

	db := "projects/" + projectID + "/instances/" + spannerDB + "/databases/" + spannerMYSQL
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsFile(serviceAccount))
	if err != nil {
		panic(err)
	}
	defer client.Close()
	dbclient, err := spanner.NewClient(ctx, db, option.WithCredentialsFile(serviceAccount))
	if err != nil {
		panic(err)
	}
	defer dbclient.Close()

	if datasetID == "" || tableID == "" || filePath == "" || loadType == "" || deLimiter == "" {
		response = JsonResponse{Type: "error", Message: "You are missing datasetID or tableID or filepath or loadtype or delimiter parameters"}
	} else {
		f, err := os.Open(filePath)
		if err != nil {
			panic(err)
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

		fmt.Println("Uploading file to BQ: ", filePath)

		job, err := loader.Run(ctx)
		if err != nil {
			println("error in job loading")
			panic(err)
		}
		status, err := job.Wait(ctx)
		if err != nil {
			println("error during job wait")
			panic(err)
		}
		if status.Err() != nil {
			println("error in job status")
			panic(err)
		}

		response = JsonResponse{Type: "success", Message: "The file has been uploaded to Big Query successfully!"}
	}

	// inserting/updating table_details_1 table in spanner db using mutations
	tableDetailsColumns := []string{"table_name", "description", "dataset", "record_type", "load_type", "delimiiter", "created_datetime", "last_modified _datetime"}
	_, err = dbclient.Apply(ctx, []*spanner.Mutation{
		spanner.InsertOrUpdate("table_details_1", tableDetailsColumns,
			[]interface{}{tableID, datasetID, "CSV", loadType, "test dataset", spanner.CommitTimestamp, spanner.CommitTimestamp}),
	})
	if err != nil {
		log.Panicln(err)
	}

	// inserting/updating table_details table in spanner db using dml
	// created_datetime & last_modified_datetime needs to be inserted when new table entry is created
	// last_modified_datetime needs to be updated when table entry already exists

	_, err = dbclient.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		row, err := txn.ReadRow(ctx, "table_details", spanner.Key{tableID}, []string{"table_name"})

		fmt.Println("row: ", &row)
		if err != nil {
			stmt := spanner.Statement{
				SQL: `INSERT INTO table_details (table_name, dataset, record_type, load_type, delimiter, description, created_datetime,last_modified_datetime)
		VALUES (@tableID, @datasetID, 'CSV', @loadType, @deLimiter,'test dataset',@created datetime,@last_modified_datetime)`,
				Params: map[string]interface{}{
					"tableID":                tableID,
					"datasetID":              datasetID,
					"loadType":               loadType,
					"deLimiter":              deLimiter,
					"created datetime":       time.Now(),
					"last_modified_datetime": time.Now(),
				},
			}
			rowCount, err := txn.Update(ctx, stmt)
			if err != nil {
				return err
			}
			fmt.Println(tableID, "%d record(s) inserted. \n", rowCount)
			return nil
		}

		stmt := spanner.Statement{
			SQL: `UPDATE table_ details set last_modified_datetime = @last_modified_datetime where table_name = @tableID`,
			Params: map[string]interface{}{
				"tableID":                 tableID,
				"last, modified_datetime": time.Now(),
			},
		}
		rowCount, err := txn.Update(ctx, stmt)
		if err != nil {
			return err
		}
		fmt.Println(tableID, "%d record(s) updated. \n", rowCount)
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	json.NewEncoder(w).Encode(response)

}
