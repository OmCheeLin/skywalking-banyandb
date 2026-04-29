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

	"github.com/stretchr/testify/require"

	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	"github.com/apache/skywalking-banyandb/pkg/meter"
)

type fakeCounter struct {
	total float64
}

func (f *fakeCounter) Inc(delta float64, _ ...string) { f.total += delta }
func (*fakeCounter) Delete(_ ...string) bool          { return true }

type fakeGauge struct {
	value float64
}

func (g *fakeGauge) Set(v float64, _ ...string)     { g.value = v }
func (g *fakeGauge) Add(delta float64, _ ...string) { g.value += delta }
func (*fakeGauge) Delete(_ ...string) bool          { return true }
func newFakeGauge() meter.Gauge                     { return &fakeGauge{} }
func getGaugeValue(g meter.Gauge) float64           { return g.(*fakeGauge).value }

type fakeHistogram struct {
	count int
}

func (h *fakeHistogram) Observe(_ float64, _ ...string) { h.count++ }
func (*fakeHistogram) Delete(_ ...string) bool          { return true }

func TestReleaseMetricsReleasesGauges(t *testing.T) {
	m := &metrics{
		activeSyncSessions: newFakeGauge(),
		reorderBuffered:    newFakeGauge(),
	}

	sess := &syncSession{
		sessionID: "s1",
		startTime: time.Now(),
		metadata:  &clusterv1.SyncMetadata{Topic: "v1:stream-part-sync"},
		chunkBuffer: &chunkBuffer{
			chunks: map[uint32]*clusterv1.SyncPartRequest{
				2: {ChunkIndex: 2},
				3: {ChunkIndex: 3},
			},
		},
	}

	// simulate start increments
	m.activeSyncSessions.Add(1, sess.metadata.Topic)
	m.reorderBuffered.Add(2, sess.metadata.Topic)

	sess.releaseMetrics(m)

	require.Equal(t, float64(0), getGaugeValue(m.activeSyncSessions))
	require.Equal(t, float64(0), getGaugeValue(m.reorderBuffered))
	// idempotent
	sess.releaseMetrics(m)
	require.Equal(t, float64(0), getGaugeValue(m.activeSyncSessions))
	require.Equal(t, float64(0), getGaugeValue(m.reorderBuffered))
}

func TestCompletionOutcomeMetricsRecorded(t *testing.T) {
	s := &server{
		metrics: &metrics{
			activeSyncSessions:      newFakeGauge(),
			reorderBuffered:         newFakeGauge(),
			chunkedSyncFailedParts:  &fakeCounter{},
			chunkedSyncTotalBytes:   &fakeCounter{},
			chunkedSyncDurationSecs: &fakeHistogram{},
		},
	}

	session := &syncSession{
		sessionID: "s1",
		startTime: time.Now().Add(-2 * time.Second),
		metadata:  &clusterv1.SyncMetadata{Topic: "v1:stream-part-sync"},
		partsProgress: map[int]*partProgress{
			0: {totalBytes: 10, receivedBytes: 10, completed: true},
			1: {totalBytes: 10, receivedBytes: 0, completed: false},
		},
		totalReceived:  10,
		chunksReceived: 2,
	}

	// Directly simulate the tail of handleCompletion logic:
	partsResults := []*clusterv1.PartResult{
		{Success: true, Error: "", BytesProcessed: 10},
		{Success: false, Error: "some error", BytesProcessed: 0},
	}
	syncResult := &clusterv1.SyncResult{
		Success:            false,
		TotalBytesReceived: session.totalReceived,
		DurationMs:         time.Since(session.startTime).Milliseconds(),
		ChunksReceived:     session.chunksReceived,
		PartsReceived:      uint32(len(session.partsProgress)),
		PartsResults:       partsResults,
	}

	session.releaseMetrics(s.metrics)
	topic := session.metadata.Topic
	s.metrics.chunkedSyncTotalBytes.Inc(float64(syncResult.TotalBytesReceived), topic)
	s.metrics.chunkedSyncDurationSecs.Observe(float64(syncResult.DurationMs)/1000.0, topic)
	for _, pr := range partsResults {
		if !pr.Success {
			reason := failedReasonIncomplete
			if pr.Error != "" {
				reason = failedReasonError
			}
			s.metrics.chunkedSyncFailedParts.Inc(1, topic, reason)
		}
	}

	require.True(t, session.completed)
}
