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
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
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
