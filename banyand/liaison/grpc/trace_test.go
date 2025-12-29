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

package grpc

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// mockNodeRegistry is a simple mock implementation of NodeRegistry for testing
type mockNodeRegistry struct {
	locateFunc func(group, name string, shardID, replicaID uint32) (string, error)
}

func (m *mockNodeRegistry) Locate(group, name string, shardID, replicaID uint32) (string, error) {
	if m.locateFunc != nil {
		return m.locateFunc(group, name, shardID, replicaID)
	}
	return "", errors.New("not implemented")
}

func (m *mockNodeRegistry) String() string {
	return "mock"
}

func TestTraceServicePublishMessagesRehash(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Setup mocks
	publisher := queue.NewMockBatchPublisher(ctrl)
	groupRepo := &groupRepo{
		resourceOpts: map[string]*commonv1.ResourceOpts{
			"test-group": {
				Replicas: 2, // copies = Replicas + 1 = 3
			},
		},
	}

	ts := &traceService{
		discoveryService: &discoveryService{
			groupRepo: groupRepo,
		},
		l: logger.GetLogger("test"),
	}

	metadata := &commonv1.Metadata{
		Group: "test-group",
		Name:  "test-trace",
	}
	writeEntity := &tracev1.WriteRequest{
		Version: 1,
		Tags:    []*modelv1.TagValue{},
	}
	shardID := common.ShardID(0)

	// Test case 1: First node fails, second node succeeds (rehashing)
	t.Run("rehash_on_first_node_failure", func(t *testing.T) {
		nodeMetadataSent := make(map[string]bool)
		nodeSpecSent := make(map[string]bool)

		callCount := 0
		mockNR := &mockNodeRegistry{
			locateFunc: func(group, name string, shardID, replicaID uint32) (string, error) {
				callCount++
				if replicaID == 0 {
					return "", errors.New("node unavailable")
				}
				if replicaID == 1 {
					return "node-2", nil
				}
				return "", errors.New("node unavailable")
			},
		}
		ts.discoveryService.nodeRegistry = mockNR

		// Publish succeeds on second node
		publisher.EXPECT().
			Publish(gomock.Any(), data.TopicTraceWrite, gomock.Any()).
			Return(nil, nil).
			Times(1)

		nodes, err := ts.publishMessages(
			context.Background(),
			publisher,
			writeEntity,
			metadata,
			nil,
			shardID,
			nodeMetadataSent,
			nodeSpecSent,
		)

		assert.NoError(t, err)
		assert.Equal(t, []string{"node-2"}, nodes)
		assert.GreaterOrEqual(t, callCount, 2) // Should have tried at least 2 replicas
	})

	// Test case 2: First node locates but publish fails, second node succeeds
	t.Run("rehash_on_publish_failure", func(t *testing.T) {
		nodeMetadataSent := make(map[string]bool)
		nodeSpecSent := make(map[string]bool)

		callCount := 0
		mockNR := &mockNodeRegistry{
			locateFunc: func(group, name string, shardID, replicaID uint32) (string, error) {
				callCount++
				if replicaID == 0 {
					return "node-1", nil
				}
				if replicaID == 1 {
					return "node-2", nil
				}
				return "", errors.New("node unavailable")
			},
		}
		ts.discoveryService.nodeRegistry = mockNR

		// First publish fails
		publisher.EXPECT().
			Publish(gomock.Any(), data.TopicTraceWrite, gomock.Any()).
			Return(nil, errors.New("publish failed")).
			Times(1)

		// Second publish succeeds
		publisher.EXPECT().
			Publish(gomock.Any(), data.TopicTraceWrite, gomock.Any()).
			Return(nil, nil).
			Times(1)

		nodes, err := ts.publishMessages(
			context.Background(),
			publisher,
			writeEntity,
			metadata,
			nil,
			shardID,
			nodeMetadataSent,
			nodeSpecSent,
		)

		assert.NoError(t, err)
		assert.Equal(t, []string{"node-2"}, nodes)
		assert.GreaterOrEqual(t, callCount, 2) // Should have tried at least 2 replicas
	})

	// Test case 3: All nodes fail
	t.Run("all_nodes_fail", func(t *testing.T) {
		nodeMetadataSent := make(map[string]bool)
		nodeSpecSent := make(map[string]bool)

		mockNR := &mockNodeRegistry{
			locateFunc: func(group, name string, shardID, replicaID uint32) (string, error) {
				return "", errors.New("node unavailable")
			},
		}
		ts.discoveryService.nodeRegistry = mockNR

		nodes, err := ts.publishMessages(
			context.Background(),
			publisher,
			writeEntity,
			metadata,
			nil,
			shardID,
			nodeMetadataSent,
			nodeSpecSent,
		)

		assert.Error(t, err)
		assert.Nil(t, nodes)
		assert.Contains(t, err.Error(), "failed to locate or publish to any available node")
	})
}
