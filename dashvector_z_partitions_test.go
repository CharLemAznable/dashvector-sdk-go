package dashvector_test

import (
	"github.com/CharLemAznable/dashvector-sdk-go"
	"github.com/gogf/gf/v2/container/garray"
	"github.com/gogf/gf/v2/test/gtest"
	"github.com/gogf/gf/v2/util/guid"
	"testing"
)

func Test_Partition_Default(t *testing.T) {
	gtest.C(t, func(t *gtest.T) {
		name := guid.S()
		_, _ = client.CreateServing(ctx, name, dashvector.WithDimension(2))
		collection := client.GetCollection(name)

		createResponse, err := collection.CreateServing(ctx, "test")
		t.AssertNil(err)
		t.Assert(createResponse.GetCode(), 0)
		t.Assert(createResponse.GetMessage(), "")
		t.AssertNE(createResponse.GetRequestId(), "")

		descResponse, err := collection.Desc(ctx, "test")
		t.AssertNil(err)
		t.Assert(descResponse.GetCode(), 0)
		t.Assert(descResponse.GetMessage(), "")
		t.AssertNE(descResponse.GetRequestId(), "")
		t.Assert(descResponse.GetOutput(), dashvector.StatusServing)

		listResponse, err := collection.List(ctx)
		t.AssertNil(err)
		t.Assert(listResponse.GetCode(), 0)
		t.Assert(listResponse.GetMessage(), "")
		t.AssertNE(listResponse.GetRequestId(), "")
		list := listResponse.GetOutput()
		t.AssertGT(len(list), 0)
		t.Assert(garray.NewStrArrayFrom(list).Contains("test"), true)

		statsResponse, err := collection.Stats(ctx, "test")
		t.AssertNil(err)
		t.Assert(statsResponse.GetCode(), 0)
		t.Assert(statsResponse.GetMessage(), "")
		t.AssertNE(statsResponse.GetRequestId(), "")
		stats := statsResponse.GetOutput()
		t.Assert(stats.GetTotalDocCount(), 0)

		deleteResponse, err := collection.Delete(ctx, "test")
		t.AssertNil(err)
		t.Assert(deleteResponse.GetCode(), 0)
		t.Assert(deleteResponse.GetMessage(), "")
		t.AssertNE(deleteResponse.GetRequestId(), "")

		_, _ = client.Delete(ctx, name)
	})
}

func Test_Partition_Validation(t *testing.T) {
	gtest.C(t, func(t *gtest.T) {
		name := guid.S()
		_, _ = client.CreateServing(ctx, name, dashvector.WithDimension(2))
		collection := client.GetCollection(name)

		_, err := collection.Create(ctx, "")
		t.AssertNE(err, nil)
		t.Assert(err.Error(), "partitionName is required")

		_, err = collection.Desc(ctx, "")
		t.AssertNE(err, nil)
		t.Assert(err.Error(), "partitionName is required")

		_, err = collection.Stats(ctx, "")
		t.AssertNE(err, nil)
		t.Assert(err.Error(), "partitionName is required")

		_, err = collection.Delete(ctx, "")
		t.AssertNE(err, nil)
		t.Assert(err.Error(), "partitionName is required")

		_, err = collection.CreateServing(ctx, "")
		t.AssertNE(err, nil)
		t.Assert(err.Error(), "partitionName is required")

		_, _ = client.Delete(ctx, name)
	})
}
