package integration

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const baseAddress = "http://localhost:8090"

var client = http.Client{
	Timeout: 3 * time.Second,
}

func TestBalancer(t *testing.T) {
	assert := assert.New(t)

	serverPool := []string{
		"server1:8080",
		"server2:8080",
		"server3:8080",
	}

	authors := make(chan string, 10)
	for i := 0; i < 10; i++ {
		go func() {
			resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
			if err != nil {
				assert.Fail(err.Error())
			}
			respServer := resp.Header.Get("Lb-from")
			authors <- respServer
		}()
		time.Sleep(time.Duration(20) * time.Millisecond)
	}
	for i := 0; i < 10; i++ {
		auth := <-authors
		assert.Equal(auth, serverPool[i%3])
	}
}

func BenchmarkBalancer(b *testing.B) {
	assert := assert.New(b)

	for i := 0; i < b.N; i++ {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
		assert.Nil(err)
		assert.Equal(resp.StatusCode, http.StatusOK)
	}
}
