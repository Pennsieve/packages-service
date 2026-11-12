package rdsiam

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDSN_QuotesValues(t *testing.T) {
	c := NewConnector("proxy.example.com", 5432, "prod_rds_proxy_user", "discover_postgres", "us-east-1", nil)
	token := `proxy.example.com:5432/?Action=connect&DBUser=prod_rds_proxy_user&X-Amz-Signature=ab'c\d`

	dsn := c.dsn(token)

	// lib/pq must accept the quoted DSN.
	_, err := pq.NewConnector(dsn)
	require.NoError(t, err)
	assert.Contains(t, dsn, `password='proxy.example.com:5432/?Action=connect&DBUser=prod_rds_proxy_user&X-Amz-Signature=ab\'c\\d'`)
	assert.Contains(t, dsn, `dbname='discover_postgres'`)
	assert.Contains(t, dsn, `port=5432`)
}

func TestConnect_BuildsATokenForEveryConnection(t *testing.T) {
	// A port nothing listens on: each Connect fails after the token is built.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())

	c := NewConnector("127.0.0.1", port, "dev_rds_proxy_user", "discover_postgres", "us-east-1", aws.AnonymousCredentials{})
	var endpoints []string
	calls := 0
	c.buildToken = func(_ context.Context, endpoint, region, dbUser string, _ aws.CredentialsProvider, _ ...func(*auth.BuildAuthTokenOptions)) (string, error) {
		calls++
		endpoints = append(endpoints, endpoint)
		assert.Equal(t, "us-east-1", region)
		assert.Equal(t, "dev_rds_proxy_user", dbUser)
		return fmt.Sprintf("token-%d", calls), nil
	}

	for i := 0; i < 3; i++ {
		_, err := c.Connect(context.Background())
		assert.Error(t, err, "no server is listening")
	}
	assert.Equal(t, 3, calls, "each new connection must get its own token")
	assert.Equal(t, fmt.Sprintf("127.0.0.1:%d", port), endpoints[0])
}

func TestConnect_TokenError(t *testing.T) {
	c := NewConnector("127.0.0.1", 5432, "u", "discover_postgres", "us-east-1", aws.AnonymousCredentials{})
	c.buildToken = func(context.Context, string, string, string, aws.CredentialsProvider, ...func(*auth.BuildAuthTokenOptions)) (string, error) {
		return "", fmt.Errorf("no credentials")
	}
	_, err := c.Connect(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "error building RDS auth token for discover_postgres")
}
