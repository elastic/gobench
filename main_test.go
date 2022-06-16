// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package main

import (
	"bufio"
	"flag"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_parseExtraMetrics(t *testing.T) {
	f, err := os.Open("testdata/benchmark-result.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	type args struct {
		line string
	}
	expected := []map[string]float64{
		{
			"error_responses_sec": 0,
			"errors_sec":          320.7,
			"events_sec":          15988,
			"metrics_sec":         735.5,
			"spans_sec":           10546,
			"txs_sec":             4386},
		{
			"error_responses_sec": 0,
			"errors_sec":          293.8,
			"events_sec":          12066,
			"metrics_sec":         716.6,
			"spans_sec":           6361,
			"txs_sec":             4695},
		{
			"error_responses_sec": 0,
			"errors_sec":          132.6,
			"events_sec":          12928,
			"metrics_sec":         3899,
			"spans_sec":           7512,
			"txs_sec":             1385},
		{
			"error_responses_sec": 0,
			"errors_sec":          503.9,
			"events_sec":          14116,
			"metrics_sec":         1037,
			"spans_sec":           8303,
			"txs_sec":             4272},
		nil, // Second to last entry is ignored.
		nil, // Last entry is ignored.
	}
	scanner := bufio.NewScanner(f)
	for i := 0; scanner.Scan(); i++ {
		result := parseExtraMetrics(scanner.Text())
		if len(expected) <= i {
			t.Errorf("expected entry not found for index %d", i)
			return
		}
		assert.Equal(t, expected[i], result)
	}
}

func Test_readInputConfig(t *testing.T) {
	t.Run(" override full config success", func(t *testing.T) {
		host := "https://top-host:9200"
		index := "bench"
		username := "elastic"
		pass := "t0pSeCr3t"
		tlsSkipVerify := "true"
		tlsSkipVerifyBool, _ := strconv.ParseBool(tlsSkipVerify)
		requestTimeout := "100"
		requestTimeoutInt, _ := strconv.Atoi(requestTimeout)

		os.Args = []string{
			"cmd",
			"-es", host,
			"-index", index,
			"-es-username", username,
			"-es-password", pass,
			"-request-timeout", requestTimeout,
			"-tls-verify", tlsSkipVerify,
		}

		//flags are now reset
		defer func() { flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError) }()

		var cfg elasticsearchConfig
		readInputConfig(&cfg)
		assert.Equal(t, host, cfg.host)
		assert.Equal(t, index, cfg.index)
		assert.Equal(t, username, cfg.user)
		assert.Equal(t, pass, cfg.pass)
		assert.Equal(t, tlsSkipVerifyBool, cfg.shouldSkipTlsVerify)
		assert.Equal(t, requestTimeoutInt, cfg.httpTimeoutSeconds)

	})

	t.Run(" override partial config success", func(t *testing.T) {
		host := "https://top-host:9200"
		index := "bench"
		username := "elastic"
		pass := "t0pSeCr3t"

		os.Args = []string{
			"cmd",
			"-es", host,
			"-index", index,
			"-es-username", username,
			"-es-password", pass,
		}

		//flags are now reset
		defer func() { flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError) }()
		var cfg elasticsearchConfig
		readInputConfig(&cfg)
		assert.Equal(t, host, cfg.host)
		assert.Equal(t, index, cfg.index)
		assert.Equal(t, username, cfg.user)
		assert.Equal(t, pass, cfg.pass)
		assert.Equal(t, false, cfg.shouldSkipTlsVerify)
		assert.Equal(t, 600, cfg.httpTimeoutSeconds)

	})
}

func Test_getEsVersion(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Write([]byte(`{"version" : {"number" : "7.11.1"}}`))
		}))
		t.Cleanup(srv.Close)
		v, err := getEsVersion(elasticsearchConfig{host: srv.URL, user: "", pass: ""})
		require.NoError(t, err)
		require.NotNil(t, v)
		assert.Equal(t, "7.11.1", v.String())
	})
	t.Run("success-auth", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			user, password, ok := r.BasicAuth()
			require.True(t, ok)
			assert.Equal(t, "myuser", user)
			assert.Equal(t, "mypassword", password)
			w.Write([]byte(`{"version" : {"number" : "7.11.1"}}`))
		}))
		t.Cleanup(srv.Close)
		v, err := getEsVersion(elasticsearchConfig{host: srv.URL, user: "myuser", pass: "mypassword"})
		require.NoError(t, err)
		require.NotNil(t, v)
		assert.Equal(t, "7.11.1", v.String())
	})
	t.Run("fail-401", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(401)
			w.Write([]byte(`{"error":{"root_cause":[{"type":"security_exception","reason":"missing authentication credentials for REST request [/]","header":{"WWW-Authenticate":["Basic realm=\"security\" charset=\"UTF-8\"","Bearer realm=\"security\"","ApiKey"]}}],"type":"security_exception","reason":"missing authentication credentials for REST request [/]","header":{"WWW-Authenticate":["Basic realm=\"security\" charset=\"UTF-8\"","Bearer realm=\"security\"","ApiKey"]}},"status":401}`))
		}))
		t.Cleanup(srv.Close)
		v, err := getEsVersion(elasticsearchConfig{host: srv.URL, user: "", pass: ""})
		assert.EqualError(t, err, "received unexpected 401 status code")
		assert.Nil(t, v)
	})
}
