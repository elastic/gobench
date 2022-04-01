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

func Test_parseAPMBenchmark(t *testing.T) {
	f, err := os.Open("testdata/benchmark-result.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	type args struct {
		line string
	}
	expected := []apmBenchmark{
		{
			Errors:  320.7,
			Events:  15988,
			Metrics: 735.5,
			Spans:   10546,
			TXs:     4386,
		},
		{
			Errors:  293.8,
			Events:  12066,
			Metrics: 716.6,
			Spans:   6361,
			TXs:     4695},
		{
			Errors:  132.6,
			Events:  12928,
			Metrics: 3899,
			Spans:   7512,
			TXs:     1385,
		},
		{
			Errors:  503.9,
			Events:  14116,
			Metrics: 1037,
			Spans:   8303,
			TXs:     4272,
		},
		{}, // Last entry is ignored.
	}
	scanner := bufio.NewScanner(f)
	for i := 0; scanner.Scan(); i++ {
		result := parseAPMBenchmark(scanner.Text())
		if len(expected) <= i {
			t.Errorf("expected entry not found for index %d", i)
			return
		}
		assert.Equal(t, expected[i], result)
	}
}
