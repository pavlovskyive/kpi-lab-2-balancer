package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/pavlovskyive/kpi-lab-2-balancer/httptools"
	"github.com/pavlovskyive/kpi-lab-2-balancer/signal"
)

var (
	port       = flag.Int("port", 8090, "load balancer port")
	timeoutSec = flag.Int("timeout-sec", 3, "request timeout time in seconds")
	https      = flag.Bool("https", false, "whether backends support HTTPs")

	traceEnabled = flag.Bool("trace", false, "whether to include tracing information into responses")
)

var (
	timeout     = time.Duration(*timeoutSec) * time.Second
	serversPool = []*server{
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
)

type server struct {
	host      string
	isHealthy bool
	traffic   int
}

func scheme() string {
	if *https {
		return "https"
	}
	return "http"
}

func health(dst string) bool {
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s://%s/health", scheme(), dst), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	if resp.StatusCode != http.StatusOK {
		return false
	}
	return true
}

func forward(dst *server, rw http.ResponseWriter, r *http.Request) error {
	ctx, _ := context.WithTimeout(r.Context(), timeout)
	fwdRequest := r.Clone(ctx)
	fwdRequest.RequestURI = ""
	fwdRequest.URL.Host = dst.host
	fwdRequest.URL.Scheme = scheme()
	fwdRequest.Host = dst.host

	resp, err := http.DefaultClient.Do(fwdRequest)
	if err == nil {
		for k, values := range resp.Header {
			for _, value := range values {
				rw.Header().Add(k, value)
			}
		}
		if *traceEnabled {
			rw.Header().Set("lb-from", dst.host)
		}
		log.Println("fwd", resp.StatusCode, resp.Request.URL, "bytes:", resp.ContentLength)
		dst.traffic += int(resp.ContentLength)
		rw.WriteHeader(resp.StatusCode)
		defer resp.Body.Close()
		_, err := io.Copy(rw, resp.Body)
		if err != nil {
			log.Printf("Failed to write response: %s", err)
		}
		return nil
	} else {
		log.Printf("Failed to get response from %s: %s", dst.host, err)
		rw.WriteHeader(http.StatusServiceUnavailable)
		return err
	}
}

func balance(servers []*server) (*server, error) {
	var healthyServers []*server

	for _, server := range servers {
		if server.isHealthy {
			healthyServers = append(healthyServers, server)
		}
	}

	if len(healthyServers) == 0 {
		return nil, errors.New("no healthy servers at moment")
	}

	optimalServer := healthyServers[0]

	for i, server := range healthyServers {
		if i == 0 || server.traffic < optimalServer.traffic {
			optimalServer = server
		}
	}

	return optimalServer, nil

}

func main() {
	flag.Parse()

	// TODO: Використовуйте дані про стан сервреа, щоб підтримувати список тих серверів, яким можна відправляти ззапит.
	for _, server := range serversPool {
		server := server
		go func() {
			for range time.Tick(10 * time.Second) {
				server.isHealthy = health(server.host)

				serverStatus := ""
				if server.isHealthy {
					serverStatus = "healthy"
				} else {
					serverStatus = "down"
				}

				log.Println("server:", server.host, "status:", serverStatus, "traffic:", server.traffic)
			}
		}()
	}

	frontend := httptools.CreateServer(*port, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		// TODO: Рееалізуйте свій алгоритм балансувальника.
		optimalServer, err := balance(serversPool)

		if err != nil {
			log.Printf("503: no availible servers")
			rw.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		forward(optimalServer, rw, r)
	}))

	log.Println("Starting load balancer...")
	log.Printf("Tracing support enabled: %t", *traceEnabled)
	frontend.Start()
	signal.WaitForTerminationSignal()
}
