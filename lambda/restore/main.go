package main

import (
	"fmt"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/pennsieve/packages-service/restore/handler"
	"github.com/pennsieve/pennsieve-go-core/pkg/queries/pgdb"
	log "github.com/sirupsen/logrus"
	"os"
)

func init() {
	log.SetFormatter(&log.JSONFormatter{})
	if level, ok := os.LookupEnv("LOG_LEVEL"); !ok {
		log.SetLevel(log.InfoLevel)
	} else {
		if ll, err := log.ParseLevel(level); err == nil {
			log.SetLevel(ll)
		} else {
			log.SetLevel(log.InfoLevel)
			log.Warnf("could not set log level to %q: %v", level, err)
		}

	}
	// Open DB connection pool here so that it can be reused if lambda handles more than one request
	db, err := pgdb.ConnectRDS()
	if err != nil {
		panic(fmt.Sprintf("unable to connect to RDS database: %s", err))
	}
	log.Info("connected to RDS database")
	handler.PennsieveDB = db
}

func main() {
	lambda.Start(handler.RestorePackagesHandler)
}
