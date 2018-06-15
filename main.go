package main

import (
	"bufio"
	"bytes"
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

	esFlag = flag.String(
		"es", "",
		`
Elasticsearch URL into which the benchmark data should be indexed, e.g. http://localhost:9200`[1:],
	)

	esIndexFlag = flag.String(
		"index", "gobench",
		"Elasticsearch index into which the benchmarks should be stored.",
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

const (
	exceptionResourceAlreadyExists = "resource_already_exists_exception"
)

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
)

func main() {
	flag.Parse()

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
	if *esFlag != "" {
		url, err := url.Parse(*esFlag)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid Elasticsearch URL %q: %s\n", *esFlag, err)
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
		if err := createMapping(esURL); err != nil {
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
				encodeIndexOp(
					encoder, b,
					pkg, goos, goarch,
					tags, timestamp,
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
	resp, err := http.Post(bulkURL.String(), "application/x-ndjson", &buf)
	if err != nil {
		log.Fatalf("error executing bulk updates: %s", err)
	}
	if err := handleResponse(resp); err != nil {
		log.Fatalf("error executing bulk updates: %s", err)
	}
}

func createMapping(esURL *url.URL) error {
	var body bytes.Buffer
	var mappings struct {
		Mappings struct {
			Doc struct {
				Properties map[string]fieldProperties `json:"properties"`
			} `json:"_doc"`
		} `json:"mappings"`
	}
	mappings.Mappings.Doc.Properties = esFieldProperties
	if err := json.NewEncoder(&body).Encode(&mappings); err != nil {
		return err
	}

	mappingURL := *esURL
	mappingURL.Path += "/" + *esIndexFlag
	req, err := http.NewRequest(http.MethodPut, mappingURL.String(), &body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	if err := handleResponse(resp); err != nil {
		esErr, ok := err.(*esError)
		if ok && esErr.Type == exceptionResourceAlreadyExists {
			if *verboseFlag {
				log.Printf("index %q already exists", *esIndexFlag)
			}
			return nil
		}
		return err
	}
	return nil
}

func encodeIndexOp(
	encoder *json.Encoder,
	b *parse.Benchmark,
	pkg, goos, goarch string,
	tags map[string]string,
	timestamp time.Time,
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

	addHost(doc)
	addVCS(pkg, doc)
	for key, value := range tags {
		doc[key] = value
	}

	type Index struct {
		Index string `json:"_index"`
		Type  string `json:"_type"`
	}
	indexAction := struct {
		Index Index `json:"index"`
	}{Index: Index{
		Index: *esIndexFlag,
		Type:  "_doc",
	}}

	if err := encoder.Encode(indexAction); err != nil {
		log.Fatal(err)
	}
	if err := encoder.Encode(doc); err != nil {
		log.Fatal(err)
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
