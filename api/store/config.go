package store

import (
	"database/sql"
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"strings"
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

type PostgresOption struct {
	Name  string
	Value string
}

func (o *PostgresOption) String() string {
	return fmt.Sprintf("%s=%s", o.Name, o.Value)
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

func (c *PostgresConfig) Open(additionalOptions ...PostgresOption) (*sql.DB, error) {
	var b strings.Builder
	b.WriteString(c.String())
	for _, o := range additionalOptions {
		b.WriteString(" ")
		b.WriteString(o.String())
	}
	return sql.Open("postgres", b.String())
}

func PostgresConfigFromEnv() *PostgresConfig {
	return &PostgresConfig{
		Host:     getEnvOrDefault("POSTGRES_HOST", "localhost"),
		Port:     getEnvOrDefault("POSTGRES_PORT", "5432"),
		User:     getEnvOrDefault("POSTGRES_USER", "postgres"),
		Password: getEnvOrDefault("POSTGRES_PASSWORD", "password"),
		DBName:   getEnvOrDefault("PENNSIEVE_DB", "postgres"),
		SSLMode:  getEnvOrDefault("POSTGRES_SSL_MODE", "disable"),
	}
}

func getEnvOrDefault(varName string, defaultValue string) string {
	if value, set := os.LookupEnv(varName); set {
		return value
	}
	return defaultValue
}
