package main

import (
	"fmt"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/pennsieve/packages-service/service/handler"
	"github.com/pennsieve/pennsieve-go-core/pkg/queries/pgdb"
	"github.com/sirupsen/logrus"
)

func init() {
	db, err := pgdb.ConnectRDS()
	if err != nil {
		panic(fmt.Sprintf("unable to connect to RDS database: %s", err))
	}
	logrus.Info("connected to RDS database")
	handler.PennsieveDB = db
}

func main() {
	lambda.Start(handler.PackagesServiceHandler)
}
