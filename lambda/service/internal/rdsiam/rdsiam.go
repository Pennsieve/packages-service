// Package rdsiam opens Postgres connection pools that authenticate to an RDS
// Proxy with IAM database authentication.
//
// An IAM auth token is valid for 15 minutes and is only checked when a
// connection is opened. A Lambda execution environment lives far longer than
// that, and its pooled connections get closed (for example by the proxy's idle
// client timeout), so a pool whose DSN holds a token built at cold start
// eventually fails every new connection with "IAM authentication failed". The
// Connector here builds a fresh token for each new connection instead.
package rdsiam

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/lib/pq"
)

// tokenBuilder matches auth.BuildAuthToken; a variable so tests can stub it.
type tokenBuilder func(ctx context.Context, endpoint, region, dbUser string, creds aws.CredentialsProvider, optFns ...func(options *auth.BuildAuthTokenOptions)) (string, error)

// Connector is a driver.Connector for lib/pq that authenticates each new
// connection with a newly built IAM auth token.
type Connector struct {
	host       string
	port       int
	user       string
	dbName     string
	region     string
	creds      aws.CredentialsProvider
	buildToken tokenBuilder
}

// NewConnector returns a Connector for user on dbName at host:port. creds
// should refresh itself (e.g. aws.Config.Credentials), since it is used for
// every new connection.
func NewConnector(host string, port int, user, dbName, region string, creds aws.CredentialsProvider) *Connector {
	return &Connector{
		host:       host,
		port:       port,
		user:       user,
		dbName:     dbName,
		region:     region,
		creds:      creds,
		buildToken: auth.BuildAuthToken,
	}
}

// Connect builds a fresh IAM auth token and opens a connection with it.
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	endpoint := fmt.Sprintf("%s:%d", c.host, c.port)
	token, err := c.buildToken(ctx, endpoint, c.region, c.user, c.creds)
	if err != nil {
		return nil, fmt.Errorf("error building RDS auth token for %s: %w", c.dbName, err)
	}
	pqConnector, err := pq.NewConnector(c.dsn(token))
	if err != nil {
		return nil, fmt.Errorf("error configuring connection to %s: %w", c.dbName, err)
	}
	return pqConnector.Connect(ctx)
}

// Driver implements driver.Connector.
func (c *Connector) Driver() driver.Driver {
	return &pq.Driver{}
}

func (c *Connector) dsn(token string) string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s",
		quote(c.host), c.port, quote(c.user), quote(token), quote(c.dbName))
}

// quote quotes a lib/pq key/value connection string value.
func quote(v string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(v) + "'"
}

// Open returns a pool that uses connector for every new connection and checks
// that a first connection can be made.
func Open(ctx context.Context, connector *Connector) (*sql.DB, error) {
	db := sql.OpenDB(connector)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("error pinging %s: %w", connector.dbName, err)
	}
	return db, nil
}
