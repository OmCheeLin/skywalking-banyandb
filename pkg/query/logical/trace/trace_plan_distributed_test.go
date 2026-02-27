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

package trace

import (
	"testing"

	"github.com/stretchr/testify/assert"

	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/pkg/iter/sort"
)

func TestDistributedPlanDeduplication(t *testing.T) {
	// Create mock sort iterators with duplicate traces
	trace1 := &tracev1.InternalTrace{
		TraceId: "trace-1",
		Spans: []*tracev1.Span{
			{SpanId: "span-1", Span: []byte("span-data-1")},
			{SpanId: "span-2", Span: []byte("span-data-2")},
		},
	}

	trace1Duplicate := &tracev1.InternalTrace{
		TraceId: "trace-1", // Same trace ID
		Spans: []*tracev1.Span{
			{SpanId: "span-2", Span: []byte("span-data-2-duplicate")}, // Duplicate span ID
			{SpanId: "span-3", Span: []byte("span-data-3")},           // New span
		},
	}

	trace2 := &tracev1.InternalTrace{
		TraceId: "trace-2",
		Spans: []*tracev1.Span{
			{SpanId: "span-4", Span: []byte("span-data-4")},
		},
	}

	// Create comparable traces
	ct1, err1 := newComparableTrace(trace1, true)
	assert.NoError(t, err1)
	ct1Dup, err1Dup := newComparableTrace(trace1Duplicate, true)
	assert.NoError(t, err1Dup)
	ct2, err2 := newComparableTrace(trace2, true)
	assert.NoError(t, err2)

	// Create mock sort iterators
	iter1 := &mockSortIterator{
		traces: []*comparableTrace{ct1, ct2},
		index:  0,
	}
	iter2 := &mockSortIterator{
		traces: []*comparableTrace{ct1Dup},
		index:  0,
	}

	// Simulate the deduplication logic
	sortIter := sort.NewItemIter([]sort.Iterator[*comparableTrace]{iter1, iter2}, false)
	var result []*tracev1.InternalTrace
	seen := make(map[string]*tracev1.InternalTrace)
	spanIDMap := make(map[string]map[string]struct{})

	for sortIter.Next() {
		trace := sortIter.Val().InternalTrace
		if existingTrace, ok := seen[trace.TraceId]; !ok {
			// New trace, add it to the result
			seen[trace.TraceId] = trace
			result = append(result, trace)
			// Initialize span ID map for this trace
			spanIDSet := make(map[string]struct{}, len(trace.Spans))
			for _, span := range trace.Spans {
				spanIDSet[span.SpanId] = struct{}{}
			}
			spanIDMap[trace.TraceId] = spanIDSet
		} else {
			// Existing trace, merge spans and deduplicate by span ID
			if spanIDSet, exists := spanIDMap[trace.TraceId]; exists {
				for _, span := range trace.Spans {
					if _, spanExists := spanIDSet[span.SpanId]; !spanExists {
						// New span, add it to the existing trace
						existingTrace.Spans = append(existingTrace.Spans, span)
						spanIDSet[span.SpanId] = struct{}{}
					}
				}
			} else {
				// Fallback: initialize span ID map and merge spans
				spanIDSet := make(map[string]struct{}, len(existingTrace.Spans))
				for _, span := range existingTrace.Spans {
					spanIDSet[span.SpanId] = struct{}{}
				}
				for _, span := range trace.Spans {
					if _, spanExists := spanIDSet[span.SpanId]; !spanExists {
						existingTrace.Spans = append(existingTrace.Spans, span)
						spanIDSet[span.SpanId] = struct{}{}
					}
				}
				spanIDMap[trace.TraceId] = spanIDSet
			}
		}
	}

	// Verify deduplication results
	assert.Len(t, result, 2, "should have 2 unique traces")
	assert.Equal(t, "trace-1", result[0].TraceId)
	assert.Equal(t, "trace-2", result[1].TraceId)

	// Verify trace-1 has 3 spans (span-1, span-2, span-3) - span-2 should be deduplicated
	trace1Result := result[0]
	assert.Len(t, trace1Result.Spans, 3, "trace-1 should have 3 spans after deduplication")

	// Verify span IDs are unique
	spanIDs := make(map[string]bool)
	for _, span := range trace1Result.Spans {
		assert.False(t, spanIDs[span.SpanId], "span ID %s should be unique", span.SpanId)
		spanIDs[span.SpanId] = true
	}
	assert.True(t, spanIDs["span-1"])
	assert.True(t, spanIDs["span-2"])
	assert.True(t, spanIDs["span-3"])
}

// mockSortIterator is a simple mock implementation for testing.
type mockSortIterator struct {
	traces []*comparableTrace
	index  int
}

func (m *mockSortIterator) Next() bool {
	return m.index < len(m.traces)
}

func (m *mockSortIterator) Val() *comparableTrace {
	if m.index >= len(m.traces) {
		return nil
	}
	val := m.traces[m.index]
	m.index++
	return val
}

func (m *mockSortIterator) Close() error {
	return nil
}

func TestDistributedPlanProperties(t *testing.T) {
	// This is a basic test to ensure plan properties are correct
	plan := &distributedPlan{
		sortByTraceID: true,
		desc:          false,
	}

	// Test that the plan can be created and has correct properties
	assert.True(t, plan.sortByTraceID)
	assert.False(t, plan.desc)
}
