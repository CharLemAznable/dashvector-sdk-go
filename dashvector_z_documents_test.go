package dashvector_test

import (
	"github.com/CharLemAznable/dashvector-sdk-go"
	"github.com/CharLemAznable/gfx/frame/gx"
	"github.com/gogf/gf/v2/test/gtest"
	"github.com/gogf/gf/v2/util/guid"
	"testing"
	"time"
)

func Test_Document_Default(t *testing.T) {
	gtest.C(t, func(t *gtest.T) {
		name := guid.S()
		_, _ = client.CreateServing(ctx, name,
			dashvector.WithDimension(4))
		collection := client.GetCollection(name)
		var docId string

		insertResponse, err := collection.Insert(ctx,
			dashvector.WithDocument(
				dashvector.WithVector(0.1, 0.2, 0.3, 0.4),
				dashvector.WithSparseVector(1, 0.4),
				dashvector.WithSparseVector(10000, 0.6),
				dashvector.WithSparseVector(222222, 0.8),
			),
		)
		t.AssertNil(err)
		t.Assert(insertResponse.GetCode(), 0)
		t.Assert(insertResponse.GetMessage(), "Success")
		t.AssertNE(insertResponse.GetRequestId(), "")
		writeResults := insertResponse.GetOutput()
		t.Assert(len(writeResults), 1)
		docId = writeResults[0].GetId()
		t.AssertNE(docId, "")
		t.Assert(writeResults[0].GetCode(), 0)
		t.Assert(writeResults[0].GetMessage(), "")
		t.Assert(writeResults[0].GetDocOp(), dashvector.DocOpInsert)
		t.Assert(insertResponse.GetUsage().GetWriteUnits() > 0, true)

		retryUntilSuccess(func() {
			getResponse, err := collection.Get(ctx, docId)
			t.AssertNil(err)
			t.Assert(getResponse.GetCode(), 0)
			t.Assert(getResponse.GetMessage(), "Success")
			t.AssertNE(getResponse.GetRequestId(), "")
			readResults := getResponse.GetOutput()
			t.Assert(readResults[docId].GetId(), docId)
			t.Assert(readResults[docId].GetVector(), []float32{0.1, 0.2, 0.3, 0.4})
			t.Assert(readResults[docId].GetSparseVector()[1], 0.4)
			t.Assert(readResults[docId].GetSparseVector()[10000], 0.6)
			t.Assert(readResults[docId].GetSparseVector()[222222], 0.8)
			t.Assert(readResults[docId].GetScore(), 0)
			t.Assert(getResponse.GetUsage().GetReadUnits() > 0, true)
		})

		queryResponse, err := collection.Query(ctx,
			dashvector.QueryWithId(docId),
			dashvector.QueryWithVectorQueryParam(
				dashvector.QueryWithNumCandidates(10),
				dashvector.QueryWithLinear(false),
				dashvector.QueryWithEf(1000),
				dashvector.QueryWithRadius(0.53)),
			dashvector.QueryWithTopk(10),
			dashvector.QueryWithIncludeVector(true))
		t.AssertNil(err)
		t.Assert(queryResponse.GetCode(), 0)
		t.Assert(queryResponse.GetMessage(), "Success")
		t.AssertNE(queryResponse.GetRequestId(), "")
		queryResults := queryResponse.GetOutput()
		t.Assert(len(queryResults), 1)
		t.Assert(queryResults[0].GetId(), docId)
		t.Assert(queryResults[0].GetVector(), []float32{0.1, 0.2, 0.3, 0.4})
		t.Assert(queryResults[0].GetSparseVector()[1], 0.4)
		t.Assert(queryResults[0].GetSparseVector()[10000], 0.6)
		t.Assert(queryResults[0].GetSparseVector()[222222], 0.8)
		t.Assert(queryResults[0].GetScore(), 0)
		t.Assert(queryResponse.GetUsage().GetReadUnits() > 0, true)

		updateResponse, err := collection.Update(ctx,
			dashvector.WithDocument(
				dashvector.WithId(docId),
				dashvector.WithVector(0.2, 0.4, 0.6, 0.8),
				dashvector.WithSparseVector(1, 0.2),
				dashvector.WithSparseVector(10000, 0.3),
				dashvector.WithSparseVector(222222, 0.4),
			),
		)
		t.AssertNil(err)
		t.Assert(updateResponse.GetCode(), 0)
		t.Assert(updateResponse.GetMessage(), "Success")
		t.AssertNE(updateResponse.GetRequestId(), "")
		writeResults = updateResponse.GetOutput()
		t.Assert(writeResults[0].GetId(), docId)
		t.Assert(writeResults[0].GetCode(), 0)
		t.Assert(writeResults[0].GetMessage(), "")
		t.Assert(writeResults[0].GetDocOp(), dashvector.DocOpUpdate)
		t.Assert(updateResponse.GetUsage().GetWriteUnits() > 0, true)

		retryUntilSuccess(func() {
			getResponse, err := collection.Get(ctx, docId)
			t.AssertNil(err)
			t.Assert(getResponse.GetCode(), 0)
			t.Assert(getResponse.GetMessage(), "Success")
			t.AssertNE(getResponse.GetRequestId(), "")
			readResults := getResponse.GetOutput()
			t.Assert(readResults[docId].GetId(), docId)
			t.Assert(readResults[docId].GetVector(), []float32{0.2, 0.4, 0.6, 0.8})
			t.Assert(readResults[docId].GetSparseVector()[1], 0.2)
			t.Assert(readResults[docId].GetSparseVector()[10000], 0.3)
			t.Assert(readResults[docId].GetSparseVector()[222222], 0.4)
			t.Assert(readResults[docId].GetScore(), 0)
			t.Assert(getResponse.GetUsage().GetReadUnits() > 0, true)
		})

		queryResponse, err = collection.Query(ctx,
			dashvector.QueryWithVector(0.2, 0.4, 0.6, 0.8),
			dashvector.QueryWithSparseVector(1, 0.2),
			dashvector.QueryWithSparseVector(10000, 0.3),
			dashvector.QueryWithSparseVector(222222, 0.4),
			dashvector.QueryWithVectorQueryParam(),
			dashvector.QueryWithTopk(10),
			dashvector.QueryWithIncludeVector(true))
		t.AssertNil(err)
		t.Assert(queryResponse.GetCode(), 0)
		t.Assert(queryResponse.GetMessage(), "Success")
		t.AssertNE(queryResponse.GetRequestId(), "")
		queryResults = queryResponse.GetOutput()
		t.Assert(len(queryResults), 1)
		t.Assert(queryResults[0].GetId(), docId)
		t.Assert(queryResults[0].GetVector(), []float32{0.2, 0.4, 0.6, 0.8})
		t.Assert(queryResults[0].GetSparseVector()[1], 0.2)
		t.Assert(queryResults[0].GetSparseVector()[10000], 0.3)
		t.Assert(queryResults[0].GetSparseVector()[222222], 0.4)
		t.Assert(queryResults[0].GetScore(), 0)
		t.Assert(queryResponse.GetUsage().GetReadUnits() > 0, true)

		dropResponse, err := collection.Drop(ctx, docId)
		t.AssertNil(err)
		t.Assert(dropResponse.GetCode(), 0)
		t.Assert(dropResponse.GetMessage(), "Success")
		t.AssertNE(dropResponse.GetRequestId(), "")
		writeResults = dropResponse.GetOutput()
		t.Assert(writeResults[0].GetId(), docId)
		t.Assert(writeResults[0].GetCode(), 0)
		t.Assert(writeResults[0].GetMessage(), "")
		t.Assert(writeResults[0].GetDocOp(), dashvector.DocOpDelete)
		t.Assert(dropResponse.GetUsage().GetWriteUnits() > 0, true)

		_, _ = client.Delete(ctx, name)
	})
}

func Test_Document_With_Partition(t *testing.T) {
	gtest.C(t, func(t *gtest.T) {
		name := guid.S()
		_, _ = client.CreateServing(ctx, name,
			dashvector.WithVectorSchema("title", 4),
			dashvector.WithVectorSchema("content", 6))
		collection := client.GetCollection(name)
		_, _ = collection.CreateServing(ctx, "test")
		partition := collection.GetPartition("test")
		var docId string

		upsertResponse, err := partition.Upsert(ctx,
			dashvector.WithDocument(
				dashvector.WithSchemaVector("title", 0.3, 0.4, 0.5, 0.6),
				dashvector.WithSchemaVector("content", 0.3, 0.4, 0.5, 0.6, 0.7, 0.8),
				dashvector.WithField("author", "ZhangSan"),
			),
		)
		t.AssertNil(err)
		t.Assert(upsertResponse.GetCode(), 0)
		t.Assert(upsertResponse.GetMessage(), "Success")
		t.AssertNE(upsertResponse.GetRequestId(), "")
		writeResults := upsertResponse.GetOutput()
		t.Assert(len(writeResults), 1)
		docId = writeResults[0].GetId()
		t.AssertNE(docId, "")
		t.Assert(writeResults[0].GetCode(), 0)
		t.Assert(writeResults[0].GetMessage(), "")
		t.Assert(writeResults[0].GetDocOp(), dashvector.DocOpInsert)
		t.Assert(upsertResponse.GetUsage().GetWriteUnits() > 0, true)

		retryUntilSuccess(func() {
			getResponse, err := partition.Get(ctx, docId)
			t.AssertNil(err)
			t.Assert(getResponse.GetCode(), 0)
			t.Assert(getResponse.GetMessage(), "Success")
			t.AssertNE(getResponse.GetRequestId(), "")
			readResults := getResponse.GetOutput()
			t.Assert(readResults[docId].GetId(), docId)
			t.Assert(readResults[docId].GetVectors()["title"], []float32{0.3, 0.4, 0.5, 0.6})
			t.Assert(readResults[docId].GetVectors()["content"], []float32{0.3, 0.4, 0.5, 0.6, 0.7, 0.8})
			t.Assert(readResults[docId].GetFields()["author"], "ZhangSan")
			t.Assert(readResults[docId].GetScore(), 0)
			t.Assert(getResponse.GetUsage().GetReadUnits() > 0, true)
		})

		queryResponse, err := partition.Query(ctx,
			dashvector.QueryWithSchemaVector("title", []float32{0.3, 0.4, 0.5, 0.6}),
			dashvector.QueryWithSchemaVector("content", []float32{0.3, 0.4, 0.5, 0.6, 0.7, 0.8}),
			dashvector.QueryWithFilter("author = 'ZhangSan'"),
			dashvector.QueryWithOutputFields("author"),
			dashvector.QueryWithRrfRanker(100),
			dashvector.QueryWithTopk(10),
			dashvector.QueryWithIncludeVector(true))
		t.AssertNil(err)
		t.Assert(queryResponse.GetCode(), 0)
		t.Assert(queryResponse.GetMessage(), "Success")
		t.AssertNE(queryResponse.GetRequestId(), "")
		queryResults := queryResponse.GetOutput()
		t.Assert(len(queryResults), 1)
		t.Assert(queryResults[0].GetId(), docId)
		t.Assert(queryResults[0].GetVectors()["title"], []float32{0.3, 0.4, 0.5, 0.6})
		t.Assert(queryResults[0].GetVectors()["content"], []float32{0.3, 0.4, 0.5, 0.6, 0.7, 0.8})
		t.Assert(queryResults[0].GetFields()["author"], "ZhangSan")
		t.Assert(queryResults[0].GetScore() > 0, true)
		t.Assert(queryResponse.GetUsage().GetReadUnits() > 0, true)

		queryResponse, err = partition.Query(ctx,
			dashvector.QueryWithSchemaVector("title", []float32{0.3, 0.4, 0.5, 0.6}),
			dashvector.QueryWithSchemaVector("content", []float32{0.3, 0.4, 0.5, 0.6, 0.7, 0.8}),
			dashvector.QueryWithFilter("author = 'ZhangSan'"),
			dashvector.QueryWithOutputFields("author"),
			dashvector.QueryWithWeightedRanker(map[string]float32{"title": 0.2, "content": 0.8}),
			dashvector.QueryWithTopk(10),
			dashvector.QueryWithIncludeVector(true))
		t.AssertNil(err)
		t.Assert(queryResponse.GetCode(), 0)
		t.Assert(queryResponse.GetMessage(), "Success")
		t.AssertNE(queryResponse.GetRequestId(), "")
		queryResults = queryResponse.GetOutput()
		t.Assert(len(queryResults), 1)
		t.Assert(queryResults[0].GetId(), docId)
		t.Assert(queryResults[0].GetVectors()["title"], []float32{0.3, 0.4, 0.5, 0.6})
		t.Assert(queryResults[0].GetVectors()["content"], []float32{0.3, 0.4, 0.5, 0.6, 0.7, 0.8})
		t.Assert(queryResults[0].GetFields()["author"], "ZhangSan")
		t.Assert(queryResults[0].GetScore() > 0, true)
		t.Assert(queryResponse.GetUsage().GetReadUnits() > 0, true)

		dropResponse, err := partition.DropAll(ctx)
		t.AssertNil(err)
		t.Assert(dropResponse.GetCode(), 0)
		t.Assert(dropResponse.GetMessage(), "Success")
		t.AssertNE(dropResponse.GetRequestId(), "")
		writeResults = dropResponse.GetOutput()
		t.Assert(len(writeResults), 0)
		t.AssertNil(dropResponse.GetUsage())

		_, _ = client.Delete(ctx, name)
	})
}

func Test_Document_Group_Query(t *testing.T) {
	gtest.C(t, func(t *gtest.T) {
		name := guid.S()
		_, _ = client.CreateServing(ctx, name,
			dashvector.WithDimension(4),
			dashvector.WithFieldSchema("title", dashvector.FieldTypeString),
			dashvector.WithFieldSchema("length", dashvector.FieldTypeInt))
		collection := client.GetCollection(name)
		var docId string

		insertResponse, err := collection.Insert(ctx,
			dashvector.WithDocument(
				dashvector.WithVector(0.1, 0.2, 0.3, 0.4),
				dashvector.WithSparseVector(1, 0.4),
				dashvector.WithSparseVector(10000, 0.6),
				dashvector.WithSparseVector(222222, 0.8),
				dashvector.WithField("title", "abc"),
				dashvector.WithField("length", 10),
			),
		)
		t.AssertNil(err)
		t.Assert(insertResponse.GetCode(), 0)
		t.Assert(insertResponse.GetMessage(), "Success")
		t.AssertNE(insertResponse.GetRequestId(), "")
		writeResults := insertResponse.GetOutput()
		t.Assert(len(writeResults), 1)
		docId = writeResults[0].GetId()
		t.AssertNE(docId, "")
		t.Assert(writeResults[0].GetCode(), 0)
		t.Assert(writeResults[0].GetMessage(), "")
		t.Assert(writeResults[0].GetDocOp(), dashvector.DocOpInsert)
		t.Assert(insertResponse.GetUsage().GetWriteUnits() > 0, true)

		retryUntilSuccess(func() {
			getResponse, err := collection.Get(ctx, docId)
			t.AssertNil(err)
			t.Assert(getResponse.GetCode(), 0)
			t.Assert(getResponse.GetMessage(), "Success")
			t.AssertNE(getResponse.GetRequestId(), "")
			readResults := getResponse.GetOutput()
			t.Assert(readResults[docId].GetId(), docId)
			t.Assert(readResults[docId].GetVector(), []float32{0.1, 0.2, 0.3, 0.4})
			t.Assert(readResults[docId].GetSparseVector()[1], 0.4)
			t.Assert(readResults[docId].GetSparseVector()[10000], 0.6)
			t.Assert(readResults[docId].GetSparseVector()[222222], 0.8)
			t.Assert(readResults[docId].GetScore(), 0)
			t.Assert(getResponse.GetUsage().GetReadUnits() > 0, true)
		})

		queryResponse, err := collection.GroupQuery(ctx, "title",
			dashvector.GroupQueryWithId(docId),
			dashvector.GroupQueryWithTopk(1),
			dashvector.GroupQueryWithCount(3),
			dashvector.GroupQueryWithIncludeVector(true),
			dashvector.GroupQueryWithOutputFields("length"))
		t.AssertNil(err)
		t.Assert(queryResponse.GetCode(), 0)
		t.Assert(queryResponse.GetMessage(), "Success")
		t.AssertNE(queryResponse.GetRequestId(), "")
		groupResults := queryResponse.GetOutput()
		t.Assert(len(groupResults), 1)
		t.Assert(groupResults[0].GetGroupId(), "abc")
		docs := groupResults[0].GetDocs()
		t.Assert(len(docs), 1)
		t.Assert(docs[0].GetVector(), []float32{0.1, 0.2, 0.3, 0.4})
		t.Assert(docs[0].GetSparseVector()[1], 0.4)
		t.Assert(docs[0].GetSparseVector()[10000], 0.6)
		t.Assert(docs[0].GetSparseVector()[222222], 0.8)
		t.AssertNil(docs[0].GetFields()["title"])
		t.Assert(docs[0].GetFields()["length"], 10)
		t.Assert(docs[0].GetScore(), 0)

		insertResponse, err = collection.Insert(ctx,
			dashvector.WithDocument(
				dashvector.WithVector(0.2, 0.4, 0.6, 0.8),
				dashvector.WithSparseVector(1, 0.2),
				dashvector.WithSparseVector(10000, 0.3),
				dashvector.WithSparseVector(222222, 0.4),
				dashvector.WithField("title", "def"),
				dashvector.WithField("length", 20),
			),
		)
		t.AssertNil(err)
		t.Assert(insertResponse.GetCode(), 0)
		t.Assert(insertResponse.GetMessage(), "Success")
		t.AssertNE(insertResponse.GetRequestId(), "")
		writeResults = insertResponse.GetOutput()
		t.Assert(len(writeResults), 1)
		docId = writeResults[0].GetId()
		t.AssertNE(docId, "")
		t.Assert(writeResults[0].GetCode(), 0)
		t.Assert(writeResults[0].GetMessage(), "")
		t.Assert(writeResults[0].GetDocOp(), dashvector.DocOpInsert)
		t.Assert(insertResponse.GetUsage().GetWriteUnits() > 0, true)

		retryUntilSuccess(func() {
			getResponse, err := collection.Get(ctx, docId)
			t.AssertNil(err)
			t.Assert(getResponse.GetCode(), 0)
			t.Assert(getResponse.GetMessage(), "Success")
			t.AssertNE(getResponse.GetRequestId(), "")
			readResults := getResponse.GetOutput()
			t.Assert(readResults[docId].GetId(), docId)
			t.Assert(readResults[docId].GetVector(), []float32{0.2, 0.4, 0.6, 0.8})
			t.Assert(readResults[docId].GetSparseVector()[1], 0.2)
			t.Assert(readResults[docId].GetSparseVector()[10000], 0.3)
			t.Assert(readResults[docId].GetSparseVector()[222222], 0.4)
			t.Assert(readResults[docId].GetScore(), 0)
			t.Assert(getResponse.GetUsage().GetReadUnits() > 0, true)
		})

		queryResponse, err = collection.GroupQuery(ctx, "title",
			dashvector.GroupQueryWithVector(0.2, 0.4, 0.6, 0.8),
			dashvector.GroupQueryWithSparseVector(1, 0.2),
			dashvector.GroupQueryWithSparseVector(10000, 0.3),
			dashvector.GroupQueryWithSparseVector(222222, 0.4),
			dashvector.GroupQueryWithTopk(1),
			dashvector.GroupQueryWithCount(3),
			dashvector.GroupQueryWithIncludeVector(true),
			dashvector.GroupQueryWithFilter("length > 10"))
		t.AssertNil(err)
		t.Assert(queryResponse.GetCode(), 0)
		t.Assert(queryResponse.GetMessage(), "Success")
		t.AssertNE(queryResponse.GetRequestId(), "")
		groupResults = queryResponse.GetOutput()
		t.Assert(len(groupResults), 1)
		t.Assert(groupResults[0].GetGroupId(), "def")
		docs = groupResults[0].GetDocs()
		t.Assert(len(docs), 1)
		t.Assert(docs[0].GetVector(), []float32{0.2, 0.4, 0.6, 0.8})
		t.Assert(docs[0].GetSparseVector()[1], 0.2)
		t.Assert(docs[0].GetSparseVector()[10000], 0.3)
		t.Assert(docs[0].GetSparseVector()[222222], 0.4)
		t.Assert(docs[0].GetFields()["title"], "def")
		t.Assert(docs[0].GetFields()["length"], 20)
		t.Assert(docs[0].GetScore(), 0)

		_, _ = client.Delete(ctx, name)

		name = guid.S()
		_, _ = client.CreateServing(ctx, name,
			dashvector.WithVectorSchema("title", 4),
			dashvector.WithVectorSchema("content", 6),
			dashvector.WithFieldSchema("author", dashvector.FieldTypeString))
		collection = client.GetCollection(name)

		insertResponse, err = collection.Insert(ctx,
			dashvector.WithDocument(
				dashvector.WithSchemaVector("title", 0.3, 0.4, 0.5, 0.6),
				dashvector.WithSchemaVector("content", 0.3, 0.4, 0.5, 0.6, 0.7, 0.8),
				dashvector.WithField("author", "ZhangSan"),
			),
		)
		t.AssertNil(err)
		t.Assert(insertResponse.GetCode(), 0)
		t.Assert(insertResponse.GetMessage(), "Success")
		t.AssertNE(insertResponse.GetRequestId(), "")
		writeResults = insertResponse.GetOutput()
		t.Assert(len(writeResults), 1)
		docId = writeResults[0].GetId()
		t.AssertNE(docId, "")
		t.Assert(writeResults[0].GetCode(), 0)
		t.Assert(writeResults[0].GetMessage(), "")
		t.Assert(writeResults[0].GetDocOp(), dashvector.DocOpInsert)
		t.Assert(insertResponse.GetUsage().GetWriteUnits() > 0, true)

		retryUntilSuccess(func() {
			getResponse, err := collection.Get(ctx, docId)
			t.AssertNil(err)
			t.Assert(getResponse.GetCode(), 0)
			t.Assert(getResponse.GetMessage(), "Success")
			t.AssertNE(getResponse.GetRequestId(), "")
			readResults := getResponse.GetOutput()
			t.Assert(readResults[docId].GetId(), docId)
			t.Assert(readResults[docId].GetVectors()["title"], []float32{0.3, 0.4, 0.5, 0.6})
			t.Assert(readResults[docId].GetVectors()["content"], []float32{0.3, 0.4, 0.5, 0.6, 0.7, 0.8})
			t.Assert(readResults[docId].GetFields()["author"], "ZhangSan")
			t.Assert(readResults[docId].GetScore(), 0)
			t.Assert(getResponse.GetUsage().GetReadUnits() > 0, true)
		})

		queryResponse, err = collection.GroupQuery(ctx, "author",
			dashvector.GroupQueryWithVector(0.3, 0.4, 0.5, 0.6),
			dashvector.GroupQueryWithTopk(1),
			dashvector.GroupQueryWithCount(3),
			dashvector.GroupQueryWithIncludeVector(true),
			dashvector.GroupQueryWithSchemaVector("title"))
		t.AssertNil(err)
		t.Assert(queryResponse.GetCode(), 0)
		t.Assert(queryResponse.GetMessage(), "Success")
		t.AssertNE(queryResponse.GetRequestId(), "")
		groupResults = queryResponse.GetOutput()
		t.Assert(len(groupResults), 1)
		t.Assert(groupResults[0].GetGroupId(), "ZhangSan")
		docs = groupResults[0].GetDocs()
		t.Assert(len(docs), 1)
		t.Assert(docs[0].GetVectors()["title"], []float32{0.3, 0.4, 0.5, 0.6})
		t.Assert(docs[0].GetVectors()["content"], []float32{0.3, 0.4, 0.5, 0.6, 0.7, 0.8})
		t.Assert(docs[0].GetFields()["author"], "ZhangSan")
		t.Assert(docs[0].GetScore(), 0)

		_, _ = client.Delete(ctx, name)
	})
}

func Test_Document_Validation(t *testing.T) {
	gtest.C(t, func(t *gtest.T) {
		name := guid.S()
		_, _ = client.CreateServing(ctx, name, dashvector.WithDimension(4))
		collection := client.GetCollection(name)

		_, err := collection.Insert(ctx, dashvector.WithDocument(dashvector.WithId("9999")))
		t.AssertNE(err, nil)
		t.Assert(err.Error(), "docs is empty")

		_, err = collection.Update(ctx, dashvector.WithDocument(dashvector.WithVector(0.1, 0.2, 0.3, 0.4)))
		t.AssertNE(err, nil)
		t.Assert(err.Error(), "docs is empty")

		_, err = collection.Upsert(ctx, dashvector.WithDocument(dashvector.WithField("author", "ZhangSan")))
		t.AssertNE(err, nil)
		t.Assert(err.Error(), "docs is empty")

		_, err = collection.Get(ctx)
		t.AssertNE(err, nil)
		t.Assert(err.Error(), "ids is empty")

		_, err = collection.Drop(ctx)
		t.AssertNE(err, nil)
		t.Assert(err.Error(), "ids is empty")

		_, err = collection.GroupQuery(ctx, "")
		t.AssertNE(err, nil)
		t.Assert(err.Error(), "group_by_field is required")

		_, _ = client.Delete(ctx, name)
	})
}

func retryUntilSuccess(fn func()) {
	for {
		if err := gx.TryX(fn); err == nil {
			return
		}
		time.Sleep(time.Second)
	}
}
