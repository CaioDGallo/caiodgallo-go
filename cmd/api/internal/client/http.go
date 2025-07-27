package client

import (
	"net"
	"net/http"
	"time"
)

func NewHTTPClient() *http.Client {
	transport := &http.Transport{
		MaxIdleConns:        50,
		MaxIdleConnsPerHost: 20,
		MaxConnsPerHost:     40,
		IdleConnTimeout:     90 * time.Second,

		ResponseHeaderTimeout: 10 * time.Second,

		ExpectContinueTimeout: 0,

		DisableCompression: true,
		DisableKeepAlives:  false,

		DialContext: (&net.Dialer{
			Timeout:   7500 * time.Millisecond,
			KeepAlive: 30 * time.Second,
			DualStack: false,
		}).DialContext,

		ForceAttemptHTTP2: false,

		WriteBufferSize: 4096,
		ReadBufferSize:  4096,
	}

	return &http.Client{
		Transport: transport,
		Timeout:   15 * time.Second,

		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}