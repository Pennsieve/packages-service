package store

import (
	"database/sql"
	"fmt"
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
}

type PostgresConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	DBName   string
	SSLMode  string
}

func (c *PostgresConfig) String() string {
	port := c.Port
	if port == "" {
		port = "5432"
	}
	noSSLConfig := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s",
		c.Host, port, c.User, c.Password, c.DBName)
	if c.SSLMode == "" {
		return noSSLConfig
	}
	return fmt.Sprintf("%s sslmode=%s", noSSLConfig, c.SSLMode)
}

func (c *PostgresConfig) LogString() string {
	return fmt.Sprintf("host=%s port=%s user=%s password=**** dbname=%s sslmode=%s",
		c.Host, c.Port, c.User, c.DBName, c.SSLMode)
}

func (c *PostgresConfig) Open() (*sql.DB, error) {
	return sql.Open("postgres", c.String())
}

func (c *PostgresConfig) OpenAtSchema(schema string) (*sql.DB, error) {
	// Setting search_path in the connection string is a lib/pq driver extension.
	// Might not be available with other drivers.
	connStr := fmt.Sprintf("%s search_path=%s", c, schema)
	return sql.Open("postgres", connStr)
}

func PostgresConfigFromEnv() *PostgresConfig {
	return &PostgresConfig{
		Host:     os.Getenv("POSTGRES_HOST"),
		Port:     os.Getenv("POSTGRES_PORT"),
		User:     os.Getenv("POSTGRES_USER"),
		Password: os.Getenv("POSTGRES_PASSWORD"),
		DBName:   os.Getenv("PENNSIEVE_DB"),
		SSLMode:  os.Getenv("POSTGRES_SSL_MODE"),
	}
}
