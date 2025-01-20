package dashvector_test

import (
	"context"
	"github.com/CharLemAznable/dashvector-sdk-go"
	"github.com/gogf/gf/v2/container/garray"
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/test/gtest"
	"github.com/gogf/gf/v2/util/guid"
	"testing"
)

func Test_Collection_Default(t *testing.T) {
	gtest.C(t, func(t *gtest.T) {
		name := guid.S()
		createResponse, err := client.CreateServing(ctx, name,
			dashvector.WithDimension(4),
			dashvector.WithDataType(dashvector.DataTypeFloat),
			dashvector.WithMetric(dashvector.MetricEuclidean),
			dashvector.WithFieldSchema("name", dashvector.FieldTypeString),
			dashvector.WithFieldSchema("age", dashvector.FieldTypeInt),
			dashvector.WithFieldSchema("weight", dashvector.FieldTypeFloat),
			dashvector.WithExtraParams(
				dashvector.WithQuantizeType(dashvector.QuantizeTypeInt8),
				dashvector.WithAutoId("false"),
			),
		)
		t.AssertNil(err)
		t.Assert(createResponse.GetCode(), 0)
		t.Assert(createResponse.GetMessage(), "")
		t.AssertNE(createResponse.GetRequestId(), "")

		descResponse, err := client.Desc(ctx, name)
		t.AssertNil(err)
		t.Assert(descResponse.GetCode(), 0)
		t.Assert(descResponse.GetMessage(), "")
		t.AssertNE(descResponse.GetRequestId(), "")
		desc := descResponse.GetOutput()
		t.Assert(desc.GetName(), name)
		t.Assert(desc.GetDimension(), 4)
		t.Assert(desc.GetDataType(), dashvector.DataTypeFloat)
		t.Assert(desc.GetMetric(), dashvector.MetricEuclidean)
		t.Assert(desc.GetStatus(), dashvector.StatusServing)
		t.Assert(desc.GetFieldsSchema()["name"], dashvector.FieldTypeString)
		t.Assert(desc.GetFieldsSchema()["age"], dashvector.FieldTypeInt)
		t.Assert(desc.GetFieldsSchema()["weight"], dashvector.FieldTypeFloat)
		t.Assert(desc.GetVectorsSchema()["proxima_vector"].GetDimension(), 4)
		t.Assert(desc.GetVectorsSchema()["proxima_vector"].GetDataType(), dashvector.DataTypeFloat)
		t.Assert(desc.GetVectorsSchema()["proxima_vector"].GetMetric(), dashvector.MetricEuclidean)
		t.Assert(desc.GetVectorsSchema()["proxima_vector"].GetQuantizeType(), dashvector.QuantizeTypeInt8)
		t.Assert(len(desc.GetPartitionStatus()), 1)
		t.Assert(desc.GetPartitionStatus()["default"], dashvector.StatusServing)

		listResponse, err := client.List(ctx)
		t.AssertNil(err)
		t.Assert(listResponse.GetCode(), 0)
		t.Assert(listResponse.GetMessage(), "")
		t.AssertNE(listResponse.GetRequestId(), "")
		list := listResponse.GetOutput()
		t.AssertGT(len(list), 0)
		t.Assert(garray.NewStrArrayFrom(list).Contains(name), true)

		statsResponse, err := client.Stats(ctx, name)
		t.AssertNil(err)
		t.Assert(statsResponse.GetCode(), 0)
		t.Assert(statsResponse.GetMessage(), "")
		t.AssertNE(statsResponse.GetRequestId(), "")
		stats := statsResponse.GetOutput()
		t.Assert(stats.GetTotalDocCount(), 0)
		t.Assert(stats.GetIndexCompleteness(), 1.0)
		t.Assert(stats.GetPartitions()["default"].GetTotalDocCount(), 0)

		deleteResponse, err := client.Delete(ctx, name)
		t.AssertNil(err)
		t.Assert(deleteResponse.GetCode(), 0)
		t.Assert(deleteResponse.GetMessage(), "")
		t.AssertNE(deleteResponse.GetRequestId(), "")
	})
}

func Test_Collection_MultiDimension(t *testing.T) {
	gtest.C(t, func(t *gtest.T) {
		name := guid.S()
		createResponse, err := client.CreateServing(ctx, name,
			dashvector.WithFieldSchema("author", dashvector.FieldTypeString),
			dashvector.WithExtraParams(),
			dashvector.WithVectorSchema("title", 4),
			dashvector.WithVectorSchema("content", 6,
				dashvector.WithVectorDataType(dashvector.DataTypeFloat),
				dashvector.WithVectorMetric(dashvector.MetricCosine),
				dashvector.WithVectorQuantizeType(dashvector.QuantizeTypeInt8),
			),
		)
		t.AssertNil(err)
		t.Assert(createResponse.GetCode(), 0)
		t.Assert(createResponse.GetMessage(), "")
		t.AssertNE(createResponse.GetRequestId(), "")

		descResponse, err := client.Desc(ctx, name)
		t.AssertNil(err)
		t.Assert(descResponse.GetCode(), 0)
		t.Assert(descResponse.GetMessage(), "")
		t.AssertNE(descResponse.GetRequestId(), "")
		desc := descResponse.GetOutput()
		t.Assert(desc.GetName(), name)
		t.Assert(desc.GetDimension(), 0)
		t.Assert(desc.GetDataType(), dashvector.DataTypeFloat)
		t.Assert(desc.GetMetric(), dashvector.MetricEuclidean)
		t.Assert(desc.GetStatus(), dashvector.StatusServing)
		t.Assert(desc.GetFieldsSchema()["author"], dashvector.FieldTypeString)
		t.Assert(desc.GetVectorsSchema()["title"].GetDimension(), 4)
		t.Assert(desc.GetVectorsSchema()["title"].GetDataType(), dashvector.DataTypeFloat)
		t.Assert(desc.GetVectorsSchema()["title"].GetMetric(), dashvector.MetricEuclidean)
		t.Assert(desc.GetVectorsSchema()["title"].GetQuantizeType(), "")
		t.Assert(desc.GetVectorsSchema()["content"].GetDimension(), 6)
		t.Assert(desc.GetVectorsSchema()["content"].GetDataType(), dashvector.DataTypeFloat)
		t.Assert(desc.GetVectorsSchema()["content"].GetMetric(), dashvector.MetricCosine)
		t.Assert(desc.GetVectorsSchema()["content"].GetQuantizeType(), dashvector.QuantizeTypeInt8)
		t.Assert(len(desc.GetPartitionStatus()), 1)
		t.Assert(desc.GetPartitionStatus()["default"], dashvector.StatusServing)

		listResponse, err := client.List(ctx)
		t.AssertNil(err)
		t.Assert(listResponse.GetCode(), 0)
		t.Assert(listResponse.GetMessage(), "")
		t.AssertNE(listResponse.GetRequestId(), "")
		list := listResponse.GetOutput()
		t.AssertGT(len(list), 0)
		t.Assert(garray.NewStrArrayFrom(list).Contains(name), true)

		statsResponse, err := client.Stats(ctx, name)
		t.AssertNil(err)
		t.Assert(statsResponse.GetCode(), 0)
		t.Assert(statsResponse.GetMessage(), "")
		t.AssertNE(statsResponse.GetRequestId(), "")
		stats := statsResponse.GetOutput()
		t.Assert(stats.GetTotalDocCount(), 0)
		t.Assert(stats.GetIndexCompleteness(), 1.0)
		t.Assert(stats.GetPartitions()["default"].GetTotalDocCount(), 0)

		deleteResponse, err := client.Delete(ctx, name)
		t.AssertNil(err)
		t.Assert(deleteResponse.GetCode(), 0)
		t.Assert(deleteResponse.GetMessage(), "")
		t.AssertNE(deleteResponse.GetRequestId(), "")
	})
}

func Test_Collection_Validation(t *testing.T) {
	gtest.C(t, func(t *gtest.T) {
		_, err := client.Create(ctx, "")
		t.AssertNE(err, nil)
		t.Assert(err.Error(), "collectionName is required")

		_, err = client.Desc(ctx, "")
		t.AssertNE(err, nil)
		t.Assert(err.Error(), "collectionName is required")

		_, err = client.Stats(ctx, "")
		t.AssertNE(err, nil)
		t.Assert(err.Error(), "collectionName is required")

		_, err = client.Delete(ctx, "")
		t.AssertNE(err, nil)
		t.Assert(err.Error(), "collectionName is required")

		_, err = client.CreateServing(ctx, "")
		t.AssertNE(err, nil)
		t.Assert(err.Error(), "collectionName is required")

		g.TryCatch(ctx, func(ctx context.Context) {
			_ = client.GetCollection("")
		}, func(ctx context.Context, err error) {
			t.AssertNE(err, nil)
			t.Assert(err.Error(), "collectionName is required")
		})
	})
}
