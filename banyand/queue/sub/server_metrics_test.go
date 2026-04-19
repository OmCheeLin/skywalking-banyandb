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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
)

// fakeCounter records the last Inc call for assertion.
type fakeCounter struct {
	lastLabels []string
	total      float64
	lastDelta  float64
}

func (f *fakeCounter) Inc(delta float64, labelValues ...string) {
	f.lastDelta = delta
	f.total += delta
	f.lastLabels = append([]string(nil), labelValues...)
}

func (*fakeCounter) Delete(_ ...string) bool {
	return true
}

// fakeGauge records cumulative Add calls and individual Set calls.
type fakeGauge struct {
	value float64
}

func (g *fakeGauge) Set(v float64, _ ...string) { g.value = v }
func (g *fakeGauge) Add(delta float64, _ ...string) {
	g.value += delta
}
func (*fakeGauge) Delete(_ ...string) bool { return true }

// fakeHistogram records whether Observe was called and the last observed value.
type fakeHistogram struct {
	observed    float64
	observeCall int
}

func (h *fakeHistogram) Observe(v float64, _ ...string) {
	h.observed = v
	h.observeCall++
}
func (*fakeHistogram) Delete(_ ...string) bool { return true }

// fakeFactory creates and indexes all fake instruments by name.
type fakeFactory struct {
	counters   map[string]*fakeCounter
	gauges     map[string]*fakeGauge
	histograms map[string]*fakeHistogram
}

func newFakeFactory() *fakeFactory {
	return &fakeFactory{
		counters:   make(map[string]*fakeCounter),
		gauges:     make(map[string]*fakeGauge),
		histograms: make(map[string]*fakeHistogram),
	}
}

func (f *fakeFactory) NewCounter(name string, _ ...string) meter.Counter {
	c := &fakeCounter{}
	f.counters[name] = c
	return c
}

func (f *fakeFactory) NewGauge(name string, _ ...string) meter.Gauge {
	g := &fakeGauge{}
	f.gauges[name] = g
	return g
}

func (f *fakeFactory) NewHistogram(name string, _ meter.Buckets, _ ...string) meter.Histogram {
	h := &fakeHistogram{}
	f.histograms[name] = h
	return h
}

func (*fakeFactory) Close() {}

// TestUpdateChunkOrderMetricsUsesTopicLabel verifies that 
// chunk ordering metrics use the low-cardinality "topic" label.
func TestUpdateChunkOrderMetricsUsesTopicLabel(t *testing.T) {
	ff := newFakeFactory()
	m := newMetrics(ff)
	s := &server{
		metrics: m,
	}

	const topic = "test-topic"
	s.updateChunkOrderMetrics("out_of_order_received", topic)

	c, ok := ff.counters["out_of_order_chunks_received"]
	require.True(t, ok, "counter out_of_order_chunks_received not created")
	assert.Equal(t, []string{topic}, c.lastLabels)
	assert.Equal(t, float64(1), c.lastDelta)
}

// TestActiveSyncSessionsGaugeLifecycle verifies that activeSyncSessions is
// incremented when a new session starts and decremented when it completes.
func TestActiveSyncSessionsGaugeLifecycle(t *testing.T) {
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "warn"}))

	ff := newFakeFactory()
	s := &server{
		log:                 logger.GetLogger("test-lifecycle"),
		chunkedSyncHandlers: make(map[bus.Topic]queue.ChunkedSyncHandler),
		metrics:             newMetrics(ff),
	}

	const topic = "banyandb/stream"

	g := ff.gauges["chunked_sync_active_sessions"]
	require.NotNil(t, g)

	// Simulate session start path: metadata present → gauge +1.
	session := &syncSession{
		sessionID:     "sess-1",
		startTime:     time.Now(),
		partsProgress: make(map[int]*partProgress),
		metadata: &clusterv1.SyncMetadata{
			Topic:      topic,
			TotalParts: 1,
		},
	}
	s.metrics.activeSyncSessions.Add(1, topic)
	assert.Equal(t, float64(1), g.value, "gauge should be 1 after session start")

	// Simulate completion: handleCompletion decrements the gauge.
	// Inject a completed partCtx so handleCompletion doesn't crash on nil Handler.
	session.partCtx = &queue.ChunkedSyncPartContext{}
	session.partsProgress[0] = &partProgress{completed: true, receivedBytes: 64}
	session.totalReceived = 64

	mockStream := &MockSyncPartStream{}
	req := &clusterv1.SyncPartRequest{SessionId: "sess-1"}

	completionErr := s.handleCompletion(mockStream, session, req)
	require.NoError(t, completionErr)

	assert.Equal(t, float64(0), g.value, "gauge should be 0 after session completes")
}

// TestChunkedSyncFailedPartsMetric verifies that chunkedSyncFailedParts is
// incremented with the correct reason label when parts are not successful.
func TestChunkedSyncFailedPartsMetric(t *testing.T) {
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "warn"}))

	tests := []struct {
		name           string
		partsProgress  map[int]*partProgress
		errorMsg       string
		expectedReason string
		expectedCount  float64
	}{
		{
			name: "incomplete_part_reason",
			partsProgress: map[int]*partProgress{
				0: {completed: false, receivedBytes: 0},
			},
			errorMsg:       "",
			expectedReason: "incomplete",
			expectedCount:  1,
		},
		{
			name: "error_part_reason",
			partsProgress: map[int]*partProgress{
				0: {completed: false, receivedBytes: 0},
			},
			errorMsg:       "disk full",
			expectedReason: "error",
			expectedCount:  1,
		},
		{
			name: "multiple_failed_parts",
			partsProgress: map[int]*partProgress{
				0: {completed: false, receivedBytes: 0},
				1: {completed: false, receivedBytes: 0},
			},
			errorMsg:       "",
			expectedReason: "incomplete",
			expectedCount:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ff := newFakeFactory()
			s := &server{
				log:                 logger.GetLogger("test-failed-parts"),
				chunkedSyncHandlers: make(map[bus.Topic]queue.ChunkedSyncHandler),
				metrics:             newMetrics(ff),
			}

			const topic = "banyandb/stream"
			session := &syncSession{
				sessionID:     "sess-fail",
				startTime:     time.Now(),
				partsProgress: tt.partsProgress,
				errorMsg:      tt.errorMsg,
				partCtx:       &queue.ChunkedSyncPartContext{},
				metadata: &clusterv1.SyncMetadata{
					Topic:      topic,
					TotalParts: uint32(len(tt.partsProgress)),
				},
			}
			s.metrics.activeSyncSessions.Add(1, topic)

			mockStream := &MockSyncPartStream{}
			req := &clusterv1.SyncPartRequest{SessionId: "sess-fail"}

			completionErr := s.handleCompletion(mockStream, session, req)
			require.NoError(t, completionErr)

			c, ok := ff.counters["chunked_sync_failed_parts_total"]
			require.True(t, ok, "counter chunked_sync_failed_parts_total should exist")
			assert.Equal(t, tt.expectedCount, c.total,
				"failed_parts_total should equal number of failed parts")
			assert.Equal(t, tt.expectedReason, c.lastLabels[1],
				"reason label should be %q", tt.expectedReason)
		})
	}
}

// TestChunkedSyncBytesAndDuration verifies that on successful session completion
// chunkedSyncTotalBytes and chunkedSyncDurationSecs are recorded.
func TestChunkedSyncBytesAndDuration(t *testing.T) {
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "warn"}))

	ff := newFakeFactory()
	s := &server{
		log:                 logger.GetLogger("test-bytes-duration"),
		chunkedSyncHandlers: make(map[bus.Topic]queue.ChunkedSyncHandler),
		metrics:             newMetrics(ff),
	}

	const topic = "banyandb/measure"
	const payloadBytes uint64 = 1024
	s.metrics.activeSyncSessions.Add(1, topic)

	session := &syncSession{
		sessionID:     "sess-ok",
		startTime:     time.Now().Add(-50 * time.Millisecond),
		totalReceived: payloadBytes,
		partsProgress: map[int]*partProgress{
			0: {completed: true, receivedBytes: uint32(payloadBytes)},
		},
		partCtx: &queue.ChunkedSyncPartContext{},
		metadata: &clusterv1.SyncMetadata{
			Topic:      topic,
			TotalParts: 1,
		},
	}

	mockStream := &MockSyncPartStream{}
	req := &clusterv1.SyncPartRequest{SessionId: "sess-ok"}

	completionErr := s.handleCompletion(mockStream, session, req)
	require.NoError(t, completionErr)

	bytesCounter, ok := ff.counters["chunked_sync_total_bytes_received"]
	require.True(t, ok, "counter chunked_sync_total_bytes_received should exist")
	assert.Equal(t, float64(payloadBytes), bytesCounter.total,
		"total_bytes_received should equal payload size")

	durationHist, ok := ff.histograms["chunked_sync_duration_seconds"]
	require.True(t, ok, "histogram chunked_sync_duration_seconds should exist")
	assert.Equal(t, 1, durationHist.observeCall, "duration histogram should be observed once")
	assert.Greater(t, durationHist.observed, float64(0), "observed duration should be positive")
}
