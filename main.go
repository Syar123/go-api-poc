package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"
)

func main() {
	// Init the chi router
	router := chi.NewRouter()
	// Route handles & endpoints

	// Query results from Big Query
	router.Get("/query/", Query)

	// Upload Local File to GCS
	router.Post("/gcs/", FileUploadToGCS)

	// Upload Local CSV File to BQ
	router.Post("/bq/", FileUploadToBQ)

	// Upload Local CSV File to BQ and update table_details table in spanner DB
	router.Post("/bq/", FileUploadToBQSpanner)

	// Upload File from GCS to BQ
	router.Post("/gcstobq/", FileUploadFromGCSToBQ)

	// Upload File to BQ and GCS Asynchronously
	router.Post("/gcsandbq/", FileUploadToGCSBQ)

	// serve the app
	fmt.Println("Server at 8080")
	log.Fatal(http.ListenAndServe(":8080", router))

}
