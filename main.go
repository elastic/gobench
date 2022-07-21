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
	"bytes"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"go/build"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/blang/semver"
	"github.com/kr/pretty"
	"github.com/pkg/errors"
	"golang.org/x/tools/benchmark/parse"
	"golang.org/x/tools/go/vcs"
)

var (
	tagsFlag = flag.String(
		"tag", "",
		"comma-separated list of key=value pairs to add to each document",
	)

	verboseFlag = flag.Bool("v", false, "Be verbose")
)

type esError struct {
	Type   string `json:"type"`
	Reason string `json:"reason"`
}

func (e *esError) Error() string {
	return e.Reason
}

/*
	The timer remains running after Get, Head, Post, or Do return and will interrupt reading of the Response.Body.
	That's why it's this big. It's specified in the first place because the DefaultClient of the http package does not timeout. Never.
*/
var httpTimeoutSeconds = 600

func getDefaultClient(timeoutSeconds int) *http.Client {
	return &http.Client{
		Timeout: time.Second * time.Duration(timeoutSeconds)}
}

func getSecureClient(timeoutSeconds int) *http.Client {
	customTransport := &(*http.DefaultTransport.(*http.Transport)) // make shallow copy
	customTransport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}

	cl := &http.Client{
		Timeout:   time.Second * time.Duration(timeoutSeconds),
		Transport: customTransport,
	}

	return cl
}

const (
	exceptionResourceAlreadyExists = "resource_already_exists_exception"
)

type elasticsearchConfig struct {
	host                string
	user                string
	pass                string
	index               string
	shouldSkipTlsVerify bool
	httpTimeoutSeconds  int
}

func getHttpClient(skipTlsVerify bool, timeoutSeconds int) *http.Client {
	if skipTlsVerify {
		return getSecureClient(timeoutSeconds)
	}
	return getDefaultClient(timeoutSeconds)
}

type benchmark struct {
	parse.Benchmark
	extra map[string]float64
}

type fieldProperties map[string]interface{}

const (
	fieldExecutedAt        = "executed_at"
	fieldName              = "name"
	fieldIterations        = "iterations"
	fieldPkg               = "pkg"
	fieldHostname          = "hostname"
	fieldGoVersion         = "go_version"
	fieldOSVersion         = "os_version"
	fieldGOOS              = "goos"
	fieldGOARCH            = "goarch"
	fieldNSPerOp           = "ns_per_op"
	fieldMBPerS            = "mb_per_s"
	fieldAllocedBytesPerOp = "alloced_bytes_per_op"
	fieldAllocsPerOp       = "allocs_per_op"

	fieldGit              = "git"
	fieldGitCommit        = "commit"
	fieldGitSubject       = "subject"
	fieldGitCommitter     = "committer"
	fieldGitCommitterDate = "date"

	fieldExtraMetrics = "extra_metrics"
)

var (
	esFieldProperties = map[string]fieldProperties{
		fieldExecutedAt:        {"type": "date"},
		fieldName:              {"type": "keyword"},
		fieldIterations:        {"type": "long"},
		fieldPkg:               {"type": "keyword"},
		fieldHostname:          {"type": "keyword"},
		fieldGoVersion:         {"type": "keyword"},
		fieldOSVersion:         {"type": "keyword"},
		fieldGOOS:              {"type": "keyword"},
		fieldGOARCH:            {"type": "keyword"},
		fieldNSPerOp:           {"type": "double"},
		fieldMBPerS:            {"type": "double"},
		fieldAllocedBytesPerOp: {"type": "long"},
		fieldAllocsPerOp:       {"type": "long"},
		fieldGit: {
			"properties": map[string]fieldProperties{
				fieldGitCommit:  {"type": "text"},
				fieldGitSubject: {"type": "text"},
				fieldGitCommitter: {
					"properties": map[string]fieldProperties{
						fieldGitCommitterDate: {"type": "date"},
					},
				},
			},
		},
	}
	esExtraMetricsDynamicTemplate = map[string]interface{}{
		fieldExtraMetrics: map[string]interface{}{
			"path_match": "extra_metrics.*",
			"mapping": map[string]string{
				"type": "float",
			},
		},
	}
)

func readInputConfig(cfg *elasticsearchConfig) {
	flag.StringVar(&cfg.host,
		"es", "",
		`Elasticsearch URL into which the benchmark data should be indexed, e.g. http://localhost:9200`,
	)
	flag.StringVar(&cfg.index,
		"index", "gobench",
		"Elasticsearch index into which the benchmarks should be stored.",
	)
	flag.StringVar(&cfg.user, "es-username", "",
		"Elasticsearch username used for authentication.",
	)
	flag.StringVar(&cfg.pass, "es-password", "",
		"Elasticsearch password used for authentication.",
	)
	flag.IntVar(&cfg.httpTimeoutSeconds, "request-timeout", httpTimeoutSeconds,
		"Http timeout threshold in seconds.",
	)
	flag.BoolVar(&cfg.shouldSkipTlsVerify, "tls-verify", false,
		"Should skip TLS verification.",
	)
	flag.Parse()
}

func main() {
	var esConfig elasticsearchConfig
	readInputConfig(&esConfig)

	tags := make(map[string]string)
	for _, field := range strings.Split(*tagsFlag, ",") {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		i := strings.IndexRune(field, '=')
		if i == -1 {
			fmt.Fprintf(
				os.Stderr,
				"invalid key-value pair %q in -tags: missing '='\n",
				field,
			)
			os.Exit(2)
		}
		key, value := field[:i], field[i+1:]
		tags[strings.TrimSpace(key)] = strings.TrimSpace(value)
	}

	var output io.Writer
	var buf bytes.Buffer
	var esURL *url.URL
	if esConfig.host != "" {
		url, err := url.Parse(esConfig.host)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid Elasticsearch URL %q: %s\n", esConfig.host, err)
			os.Exit(2)
		}
		esURL = url
		output = &buf
		if *verboseFlag {
			output = io.MultiWriter(output, os.Stdout)
		}
	} else {
		output = os.Stdout
	}
	encoder := json.NewEncoder(output)

	if esURL != nil {
		if err := createMapping(esConfig); err != nil {
			log.Fatalf("error creating/updating mapping: %s", err)
		}
	}

	var pkg, goos, goarch string
	timestamp := time.Now().UTC()
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "pkg:"):
			pkg = strings.TrimSpace(line[len("pkg:"):])
		case strings.HasPrefix(line, "goos:"):
			goos = strings.TrimSpace(line[len("goos:"):])
		case strings.HasPrefix(line, "goarch:"):
			goarch = strings.TrimSpace(line[len("goarch:"):])
		default:
			if b, err := parse.ParseLine(line); err == nil {
				result := benchmark{Benchmark: *b}
				result.extra = parseExtraMetrics(line)
				encodeIndexOp(
					encoder, result,
					pkg, goos, goarch,
					tags, timestamp,
					esConfig,
				)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	if esURL == nil {
		// Encoded to stdout.
		return
	}

	bulkURL := *esURL
	bulkURL.Path += "/_bulk"
	req, err := http.NewRequest(http.MethodPost, bulkURL.String(), &buf)
	if esConfig.user != "" && esConfig.pass != "" {
		req.SetBasicAuth(esConfig.user, esConfig.pass)
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	resp, err := getHttpClient(esConfig.shouldSkipTlsVerify, esConfig.httpTimeoutSeconds).Do(req)
	var respbod map[string]interface{}
	jsonErr2 := json.NewDecoder(resp.Body).Decode(&respbod)
	if jsonErr2 != nil {
		log.Fatalf("error jsoninfs: %s", respbod)
	}
	pretty.Println(respbod)
	pretty.Println(resp.ContentLength)
	pretty.Println(resp.StatusCode)

	var result map[string]interface{}
	jsonErr := json.NewDecoder(resp.Body).Decode(&result)
	if jsonErr != nil {
		log.Fatalf("error jsoninfs: %s", jsonErr)
	}
	pretty.Println(result)

	if err != nil {
		log.Fatalf("error executing bulk updates: %s", err)
	}
	if err := handleResponse(resp); err != nil {
		log.Fatalf("error executing bulk updates: %s", err)
	}
}

func createMapping(cfg elasticsearchConfig) error {
	// Versions of Elasticsearch prior to 7.0.0 require type names.
	esVersion, err := getEsVersion(cfg)
	if err != nil {
		return err
	}
	includeTypeName := esVersion.LT(semver.MustParse("7.0.0"))

	var body bytes.Buffer
	properties := map[string]interface{}{
		"properties":        esFieldProperties,
		"dynamic_templates": []interface{}{esExtraMetricsDynamicTemplate},
	}
	if includeTypeName {
		properties = map[string]interface{}{"_doc": properties}
	}
	if err := json.NewEncoder(&body).Encode(map[string]interface{}{"mappings": properties}); err != nil {
		return err
	}

	mappingURL := cfg.host + "/" + cfg.index
	req, err := http.NewRequest(http.MethodPut, mappingURL, &body)
	if err != nil {
		return err
	}
	if cfg.user != "" && cfg.pass != "" {
		req.SetBasicAuth(cfg.user, cfg.pass)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := getHttpClient(cfg.shouldSkipTlsVerify, cfg.httpTimeoutSeconds).Do(req)
	if err != nil {
		return err
	}
	if err := handleResponse(resp); err != nil {
		esErr, ok := err.(*esError)
		if ok && esErr.Type == exceptionResourceAlreadyExists {
			if *verboseFlag {
				log.Printf("index %q already exists", cfg.index)
			}
			return nil
		}
		return err
	}
	return nil
}

func getEsVersion(cfg elasticsearchConfig) (*semver.Version, error) {
	req, err := http.NewRequest("GET", cfg.host, nil)
	if err != nil {
		return nil, err
	}
	if cfg.user != "" || cfg.pass != "" {
		req.SetBasicAuth(cfg.user, cfg.pass)
	}

	resp, err := getHttpClient(cfg.shouldSkipTlsVerify, cfg.httpTimeoutSeconds).Do(req)
	if err != nil {
		return nil, err
	}
	var esVersion struct {
		Version struct {
			Number string
		} `json:"version"`
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("received unexpected %d status code", resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(&esVersion); err != nil {
		return nil, err
	}
	return semver.New(esVersion.Version.Number)
}

func encodeIndexOp(
	encoder *json.Encoder,
	b benchmark,
	pkg, goos, goarch string,
	tags map[string]string,
	timestamp time.Time,
	cfg elasticsearchConfig,
) {
	doc := map[string]interface{}{
		fieldExecutedAt: timestamp,
		fieldName:       b.Name,
		fieldIterations: b.N,
		fieldPkg:        pkg,
		fieldGoVersion:  runtime.Version(),
		fieldGOOS:       goos,
		fieldGOARCH:     goarch,
	}
	if b.Measured&parse.NsPerOp != 0 {
		doc[fieldNSPerOp] = b.NsPerOp
	}
	if b.Measured&parse.MBPerS != 0 {
		doc[fieldMBPerS] = b.MBPerS
	}
	if b.Measured&parse.AllocedBytesPerOp != 0 {
		doc[fieldAllocedBytesPerOp] = b.AllocedBytesPerOp
	}
	if b.Measured&parse.AllocsPerOp != 0 {
		doc[fieldAllocsPerOp] = b.AllocsPerOp
	}
	if len(b.extra) > 0 {
		apmbench := b.extra
		doc[fieldExtraMetrics] = apmbench
	}

	addHost(doc)
	addVCS(pkg, doc)
	for key, value := range tags {
		doc[key] = value
	}

	// Versions of Elasticsearch >= 8.0.0 require no _type field
	esVersion, err := getEsVersion(cfg)
	if err != nil {
		log.Fatal(err)
	}
	includeTypDoc := esVersion.LT(semver.MustParse("8.0.0"))

	type Index struct {
		Index string `json:"_index"`
		Type  string `json:"_type,omitempty"`
	}
	indexAction := struct {
		Index Index `json:"index"`
	}{Index: Index{
		Index: cfg.index,
	}}
	if includeTypDoc {
		indexAction.Index.Type = "_doc"
	}

	if err := encoder.Encode(indexAction); err != nil {
		log.Fatal(err)
	}
	if err := encoder.Encode(doc); err != nil {
		log.Fatal(err)
	}
	if newLineErr := encoder.Encode("\n"); newLineErr != nil {
		log.Fatal(newLineErr)
	}
}

func handleResponse(resp *http.Response) error {
	defer resp.Body.Close()
	if !*verboseFlag && resp.StatusCode == http.StatusOK {
		return nil
	}
	result := make(map[string]interface{})
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		log.Fatal(err)
	}
	if resp.StatusCode == http.StatusOK {
		pretty.Println(result)
		return nil
	}
	errorObj, ok := result["error"].(map[string]interface{})
	if !ok {
		return errors.Errorf("%s", resp.Status)
	}
	return &esError{
		Type:   errorObj["type"].(string),
		Reason: errorObj["reason"].(string),
	}
}

func addHost(doc map[string]interface{}) {
	if hostname, err := os.Hostname(); err == nil {
		doc[fieldHostname] = hostname
	}
	switch runtime.GOOS {
	case "linux":
		if output, err := exec.Command("uname", "-r").Output(); err == nil {
			doc[fieldOSVersion] = strings.TrimSpace(string(output))
		}
	}
}

func addVCS(pkgpath string, doc map[string]interface{}) {
	pkg, err := build.Import(pkgpath, "", build.FindOnly)
	if err != nil {
		return
	}
	vcsCmd, _, err := vcs.FromDir(pkg.Dir, pkg.SrcRoot)
	if err != nil {
		return
	}

	switch vcsCmd.Cmd {
	case "git":
		cmd := exec.Command("git", "log", "-1", "--format=%H %ct %s")
		cmd.Dir = pkg.Dir
		output, err := cmd.CombinedOutput()
		if err != nil {
			return
		}
		fields := strings.SplitN(strings.TrimSpace(string(output)), " ", 3)
		if len(fields) == 3 {
			gitFields := map[string]interface{}{
				fieldGitCommit:  fields[0],
				fieldGitSubject: fields[2],
			}
			unixSec, err := strconv.ParseInt(fields[1], 10, 64)
			if err == nil {
				committerDate := time.Unix(unixSec, 0).UTC()
				gitFields[fieldGitCommitter] = map[string]interface{}{
					fieldGitCommitterDate: committerDate,
				}
			}
			doc[fieldGit] = gitFields
		}
	}
}

func parseExtraMetrics(line string) map[string]float64 {
	entries := strings.Split(line, "\t")
	// If the result has less than 3 columns, it doesn't contain
	// extra metrics to be reported.
	if len(entries) < 3 {
		return nil
	}

	result := make(map[string]float64)
	// Ignore the first three entries since they're fixed to be the benchmark,
	// name, iterations and ns/op.
	for _, entry := range entries[3:] {
		parts := strings.Split(strings.TrimSpace(entry), " ")
		if len(parts) < 2 {
			continue
		}

		key := strings.TrimSpace(parts[1])
		value, err := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
		if err != nil {
			continue
		}
		switch key {
		case "ns/op", "MB/s", "B/op", "allocs/op":
			// Ignore the native benchmark fields
			continue
		default:
			escapedKey := strings.ReplaceAll(key, "/", "_")
			result[escapedKey] = value
		}
	}
	if len(result) > 0 {
		return result
	}
	return nil
}
