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

package other_test

import (
	"context"
	"fmt"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
	"net/http"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	casesMeasureData "github.com/apache/skywalking-banyandb/test/cases/measure/data"
	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var _ = g.Describe("Verify deletion of single measure", func() {
	var deferFn func()
	var baseTime time.Time
	var interval time.Duration
	var conn *grpclib.ClientConn
	var goods []gleak.Goroutine
	var grpcAddr, httpAddr string
	var measureName = "service_cpm_minute"
	var measureName1 = "service_instance_cpm_minute"
	var groupName = "sw_metric"

	g.BeforeEach(func() {
		grpcAddr, httpAddr, deferFn = setup.Standalone()
		var err error
		conn, err = grpchelper.Conn(grpcAddr, 10*time.Second, grpclib.WithTransportCredentials(insecure.NewCredentials()))
		gm.Expect(err).NotTo(gm.HaveOccurred())
		ns := timestamp.NowMilli().UnixNano()
		baseTime = time.Unix(0, ns-ns%int64(time.Minute))
		interval = 500 * time.Millisecond
		casesMeasureData.Write(conn, measureName, groupName, "service_cpm_minute_data.json", baseTime, interval)
		// create other new measure and write data.
		casesMeasureData.Write(conn, measureName1, groupName, "service_instance_cpm_minute_data.json", baseTime, interval)
		goods = gleak.Goroutines()
	})

	g.AfterEach(func() {
		gm.Expect(conn.Close()).To(gm.Succeed())
		deferFn()
		gm.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
	})

	g.It("delete", func() {
		httpClient := &http.Client{}
		measureURL := fmt.Sprintf("http://%s/api/v1/measure/schema/%s/%s", httpAddr, groupName, measureName)
		gm.Eventually(func() error {
			req, err := http.NewRequest(http.MethodDelete, measureURL, nil)
			if err != nil {
				return err
			}
			resp, respErr := httpClient.Do(req)
			if respErr != nil {
				return respErr
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
			}
			return nil
		}, flags.EventuallyTimeout).Should(gm.Succeed())

		// delete measure schema.
		gm.Eventually(func() error {
			req, err := http.NewRequest(http.MethodGet, measureURL, nil)
			if err != nil {
				return err
			}
			resp, respErr := httpClient.Do(req)
			if respErr != nil {
				return respErr
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusNotFound {
				return fmt.Errorf("expected status 404, got %d", resp.StatusCode)
			}
			return nil
		}, flags.EventuallyTimeout).Should(gm.Succeed())

		// metadata should be removed immediately.
		ctx := context.Background()
		metadata := &commonv1.Metadata{
			Name:  measureName,
			Group: groupName,
		}
		schema := databasev1.NewMeasureRegistryServiceClient(conn)
		_, err := schema.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{Metadata: metadata})
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Error()).To(gm.ContainSubstring("resource not found"))

		// write data again.
		c := measurev1.NewMeasureServiceClient(conn)
		writeClient, err := c.Write(ctx)
		gm.Expect(err).NotTo(gm.HaveOccurred())
		dataPointValue := &measurev1.DataPointValue{
			Timestamp: timestamppb.New(time.Unix(0, (time.Now().UnixNano()/int64(time.Millisecond))*int64(time.Millisecond))),
			TagFamilies: []*modelv1.TagFamilyForWrite{
				{
					Tags: []*modelv1.TagValue{
						{
							Value: &modelv1.TagValue_Str{
								Str: &modelv1.Str{
									Value: "host-1",
								},
							},
						},
						{
							Value: &modelv1.TagValue_Str{
								Str: &modelv1.Str{
									Value: "ip-1",
								},
							},
						},
					},
				},
			},
			Fields: []*modelv1.FieldValue{
				{
					Value: &modelv1.FieldValue_Int{
						Int: &modelv1.Int{
							Value: 100,
						},
					},
				},
			},
		}
		gm.Expect(writeClient.Send(&measurev1.WriteRequest{Metadata: metadata, DataPoint: dataPointValue, MessageId: uint64(time.Now().UnixNano())})).
			Should(gm.Succeed())
		gm.Expect(writeClient.CloseSend()).To(gm.Succeed())
		resp, err := writeClient.Recv()
		gm.Expect(err).NotTo(gm.HaveOccurred())
		gm.Expect(resp.GetStatus()).To(gm.ContainSubstring("STATUS_INTERNAL_ERROR"))

		// Query the data of measures whose schema has already been deleted.
		measureServiceClient := measurev1.NewMeasureServiceClient(conn)
		ctx = context.Background()
		now, err := time.ParseInLocation("2006-01-02T15:04:05", "2021-09-01T23:30:00", time.Local)
		gm.Expect(err).NotTo(gm.HaveOccurred())
		_, err = measureServiceClient.Query(ctx, &measurev1.QueryRequest{
			Groups: []string{groupName},
			Name:   measureName,
			TagProjection: &modelv1.TagProjection{
				TagFamilies: []*modelv1.TagProjection_TagFamily{
					{
						Name: "default",
						Tags: []string{"id", "entity_id"},
					},
				},
			},
			TimeRange: &modelv1.TimeRange{
				Begin: timestamppb.New(now.Add(-20 * time.Minute)),
				End:   timestamppb.New(now.Add(5 * time.Minute)),
			},
		})
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Error()).To(gm.ContainSubstring("measure doesn't exist"))

		// Query the data of other existing measures.
		ctx = context.Background()
		queryResp, err := measureServiceClient.Query(ctx, &measurev1.QueryRequest{
			Groups: []string{groupName},
			Name:   measureName1,
			TagProjection: &modelv1.TagProjection{
				TagFamilies: []*modelv1.TagProjection_TagFamily{
					{
						Name: "default",
						Tags: []string{"id", "entity_id"},
					},
				},
			},
			TimeRange: &modelv1.TimeRange{
				Begin: timestamppb.New(now.Add(-20 * time.Minute)),
				End:   timestamppb.New(now.Add(5 * time.Minute)),
			},
		})
		gm.Expect(err).NotTo(gm.HaveOccurred())
		fmt.Println("queryResp: ", queryResp)

		// create other new measure and write data
		//ns := timestamp.NowMilli().UnixNano()
		//baseTime = time.Unix(0, ns-ns%int64(time.Minute))
		//interval = 500 * time.Millisecond
		//for i := 0; i < 8; i++ {
		//	casesMeasureData.Write(conn, "service_instance_cpm_minute", groupName, "service_instance_cpm_minute_data.json", baseTime, interval)
		//	baseTime = baseTime.Add(-5 * time.Minute)
		//}

		//schema = databasev1.NewMeasureRegistryServiceClient(conn)
		//measureSchema := &databasev1.Measure{
		//	Metadata: &commonv1.Metadata{
		//		Name:  "measure1",
		//		Group: groupName,
		//	},
		//	TagFamilies: []*databasev1.TagFamilySpec{
		//		{
		//			Name: "default",
		//			Tags: []*databasev1.TagSpec{
		//				{Name: "t1", Type: databasev1.TagType_TAG_TYPE_STRING},
		//				{Name: "t2", Type: databasev1.TagType_TAG_TYPE_STRING},
		//			},
		//		},
		//	},
		//	Fields: []*databasev1.FieldSpec{
		//		{
		//			Name:              "value",
		//			FieldType:         databasev1.FieldType_FIELD_TYPE_FLOAT,
		//			EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
		//			CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
		//		},
		//	},
		//	Entity: &databasev1.Entity{
		//		TagNames: []string{"t1"},
		//	},
		//}
		//_, err = schema.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{Measure: measureSchema})
		//gm.Expect(err).To(gm.HaveOccurred())
	})
})
