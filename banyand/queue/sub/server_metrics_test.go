// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
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

package sub

import (
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/meter"
)

type fakeCounter struct {
	lastLabels []string
	lastDelta  float64
}

func (f *fakeCounter) Inc(delta float64, labelValues ...string) {
	f.lastDelta = delta
	f.lastLabels = append([]string(nil), labelValues...)
}

func (*fakeCounter) Delete(_ ...string) bool {
	return true
}

type fakeGauge struct{}

func (*fakeGauge) Set(_ float64, _ ...string) {}
func (*fakeGauge) Add(_ float64, _ ...string) {}
func (*fakeGauge) Delete(_ ...string) bool    { return true }

type fakeHistogram struct{}

func (*fakeHistogram) Observe(_ float64, _ ...string) {}
func (*fakeHistogram) Delete(_ ...string) bool        { return true }

type fakeFactory struct {
	counters map[string]*fakeCounter
}

func newFakeFactory() *fakeFactory {
	return &fakeFactory{
		counters: make(map[string]*fakeCounter),
	}
}

func (f *fakeFactory) NewCounter(name string, _ ...string) meter.Counter {
	c := &fakeCounter{}
	f.counters[name] = c
	return c
}

func (*fakeFactory) NewGauge(_ string, _ ...string) meter.Gauge {
	return &fakeGauge{}
}

func (*fakeFactory) NewHistogram(_ string, _ meter.Buckets, _ ...string) meter.Histogram {
	return &fakeHistogram{}
}

func (*fakeFactory) Close() {}

func TestUpdateChunkOrderMetricsUsesTopicLabel(t *testing.T) {
	ff := newFakeFactory()
	m := newMetrics(ff)
	s := &server{
		metrics: m,
	}

	const topic = "test-topic"
	s.updateChunkOrderMetrics("out_of_order_received", topic)

	c, ok := ff.counters["out_of_order_chunks_received"]
	if !ok {
		t.Fatalf("counter out_of_order_chunks_received not created")
	}
	if len(c.lastLabels) != 1 || c.lastLabels[0] != topic {
		t.Fatalf("expected label %q, got %v", topic, c.lastLabels)
	}
	if c.lastDelta != 1 {
		t.Fatalf("expected delta 1, got %v", c.lastDelta)
	}
}
