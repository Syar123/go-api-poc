package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"golang.org/x/net/context"
	"google.golang.org/api/option"
	storage "google.golang.org/api/storage/v1"
)

func FileUploadToGCS(w http.ResponseWriter, r *http.Request) {
	gcsBucket := "gcs-bucket-poc"
	serviceAccount := "bq-project-poc.json"
	ctx := context.Background()
	client, err := storage.NewService(ctx, option.WithCredentialsFile(serviceAccount))
	if err != nil {
		log.Fatal(err)
	}
	filePath := r.FormValue("filepath")
	fileName := r.FormValue("filename")

	var response = JsonResponse{}

	if filePath == "" || fileName == "" {
		response = JsonResponse{Type: "error", Message: "You are missing filepath or filename  parameter."}
	} else {
		file, err := os.Open(filePath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error opening file: %v\n", err)
		}
		defer file.Close()
		object := &storage.Object{
			Name:         "raw/" + fileName,
			CacheControl: "public, max-age=31536000",
		}
		_, err = client.Objects.Insert(gcsBucket, object).Media(file).Do()
		if err != nil {
			fmt.Fprintf(os.Stderr, "gcs file insert error: %v\n", err)
		}
		response = JsonResponse{Type: "success", Message: "The file has been uploaded to GCS successfully!"}
	}
	json.NewEncoder(w).Encode(response)

}
