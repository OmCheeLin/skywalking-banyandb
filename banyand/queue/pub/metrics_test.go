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

package pub

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
)

type fakeSendClient struct {
	clusterv1.Service_SendClient
	ctx context.Context

	sendErrs []error
	sendIdx  int
}

func (f *fakeSendClient) Send(_ *clusterv1.SendRequest) error {
	if f.sendIdx >= len(f.sendErrs) {
		return nil
	}
	err := f.sendErrs[f.sendIdx]
	f.sendIdx++
	return err
}

func (f *fakeSendClient) Context() context.Context {
	return f.ctx
}

type countingCounter struct {
	count float64
}

func (c *countingCounter) Inc(delta float64, _ ...string) {
	c.count += delta
}

func (*countingCounter) Delete(_ ...string) bool {
	return true
}

type noopGauge struct{}

func (*noopGauge) Set(_ float64, _ ...string) {}
func (*noopGauge) Add(_ float64, _ ...string) {}
func (*noopGauge) Delete(_ ...string) bool    { return true }

type noopHistogram struct{}

func (*noopHistogram) Observe(_ float64, _ ...string) {}
func (*noopHistogram) Delete(_ ...string) bool        { return true }

// TestRetryMetrics ensures that retry-related metrics are updated when retrySend
// observes retryable errors and eventually exhausts retries.
func TestRetryMetrics(t *testing.T) {
	p := &pub{
		metrics: &pubMetrics{
			sendRetryAttempts:  &countingCounter{},
			sendRetryExhausted: &countingCounter{},
			sendErrTotal:       &countingCounter{},
			sendBackoffSeconds: &countingCounter{},
			sendTotal:          &countingCounter{},
			sendBytesTotal:     &countingCounter{},
			sendLatencySeconds: &noopHistogram{},
			inflightStreams:    &noopGauge{},
			inflightRequests:   &noopGauge{},
		},
	}

	bp := &batchPublisher{
		pub: p,
	}

	// Use a context that will not be canceled during the test.
	ctx := context.Background()
	client := &fakeSendClient{
		// Use retryable gRPC errors so that retrySend will treat them as transient.
		sendErrs: []error{
			status.Error(codes.Unavailable, "transient"),
			status.Error(codes.Unavailable, "transient"),
			status.Error(codes.Unavailable, "transient"),
			status.Error(codes.Unavailable, "transient"),
		},
		ctx: ctx,
	}

	req := &clusterv1.SendRequest{
		Topic: "test-topic",
		Body:  []byte("payload"),
	}

	const nodeName = "test-node"
	const topicStr = "test-topic"

	err := bp.retrySend(ctx, client, req, nodeName, topicStr)
	if err == nil {
		t.Fatalf("expected error from retrySend, got nil")
	}

	retries := p.metrics.sendRetryAttempts.(*countingCounter).count
	if retries == 0 {
		t.Fatalf("expected retries to be recorded, got 0")
	}

	exhausted := p.metrics.sendRetryExhausted.(*countingCounter).count
	if exhausted != 1 {
		t.Fatalf("expected retry_exhausted to be 1, got %v", exhausted)
	}

	errCount := p.metrics.sendErrTotal.(*countingCounter).count
	if errCount == 0 {
		t.Fatalf("expected send_err_total to be incremented, got 0")
	}
}
