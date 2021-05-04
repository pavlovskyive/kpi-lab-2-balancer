package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBalancer(t *testing.T) {
	assert := assert.New(t)

	mockedServersPool := []*server{
		{
			host:      "server1:8080",
			isHealthy: true,
		},
		{
			host:      "server2:8080",
			isHealthy: true,
		},
		{
			host:      "server3:8080",
			isHealthy: true,
		},
	}
	// ----
	mockedServersPool[0].traffic = 50
	mockedServersPool[1].traffic = 20
	server, err := balance(mockedServersPool)

	assert.Nil(err)
	assert.Equal(server.host, mockedServersPool[2].host)
	// ----
	mockedServersPool[2].traffic = 40
	server, err = balance(mockedServersPool)
	// Now server with index "1" has least traffic

	assert.Nil(err)
	assert.Equal(server.host, mockedServersPool[1].host)
	// ----
	mockedServersPool[1].traffic = 70
	mockedServersPool[2].isHealthy = false
	server, err = balance(mockedServersPool)
	// Now server with least traffic is down, so server with least traffic that is healthy is with index "0"

	assert.Nil(err)
	assert.Equal(server.host, mockedServersPool[0].host)
}

func TestBalancerError(t *testing.T) {
	assert := assert.New(t)

	mockedServersPool := []*server{
		{
			host:      "server1:8080",
			isHealthy: false,
		},
		{
			host:      "server2:8080",
			isHealthy: false,
		},
		{
			host:      "server3:8080",
			isHealthy: false,
		},
	}
	// ----
	mockedServersPool[0].traffic = 50
	mockedServersPool[1].traffic = 20
	_, err := balance(mockedServersPool)

	assert.NotNil(err)
}
