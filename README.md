# go-api-poc

Building an api to be able to connect to google cloud from your local system using go-lang (using service account for auth)

## To install gorilla/mux router
Go to terminal/cmd-promt (your-project-location) and run below commands

go mod init example.com/m

go get github.com/gorilla/mux

go get github.com/lib/pq

## To install go-chi router

go mod init example.com/m

go get -u github.com/go-chi/chi/v5

## To install additional go packages

go get 

"cloud.google.com/go/bigquery"

"cloud.google.com/go/spanner"

"golang.org/x/net/context"

"google.golang.org/api/iterator"

"google.golang.org/api/option"

## Verify your application using postman

#### To run your program

cd into your project location and run below command

go run main.go query.go fileuploadtogcs.go fileuploadtobq.go fileuploadtobqwithspanner.go

#### Query API 

using GET method URL ("http://localhost:8080/query/")

provide query parameter (key: query; value: select * from bigquery-public-data.covid19_open_data limit 10)

#### File Upload TO GCS API

using POST method URL ("http://localhost:8080/gcs/")

provide below parameters (key: value) 

filepath : "your local file path (eg: /users/syar/documents/localtest.json)"

filename : "your desired GCS file name (eg: poc-test)"

your gcs file path gs://gcs-bucket-poc/raw/poc-test 

#### File Upload TO BQ API

using POST method URL ("http://localhost:8080/bq/")

projectID : "bq-project-poc"

recordtype : CSV

provide below parameters (key: value) 

dataset_name : "poc_dataset"

table_name : "poc_table"

filepath : "your local file path (eg: /users/syar/documents/localtest.json)"

loadtype : "replace" or "append"

delimiter : eg : "~"

#### File Upload TO BQ and Update Spanner DB API

using POST method URL ("http://localhost:8080/bqspanner/")

Spanner DB Instance : "spanner-test-db"
	
Spanner Database MYSQL : "spanner-test-mysql"

provide parameters as above (key: value) 


