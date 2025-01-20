package dashvector

import (
	"context"
	"github.com/CharLemAznable/gfx/net/gclientx"
	"github.com/gogf/gf/v2/encoding/gjson"
	"github.com/gogf/gf/v2/encoding/gurl"
	"github.com/gogf/gf/v2/errors/gcode"
	"github.com/gogf/gf/v2/errors/gerror"
	"github.com/gogf/gf/v2/text/gstr"
	"github.com/gogf/gf/v2/util/gvalid"
	"github.com/samber/lo"
)

func newDocuments(client *gclientx.Client, collectionName string, partitionName string) Partition {
	return &documents{
		Client:         client,
		collectionName: collectionName,
		partitionName:  partitionName,
	}
}

type documents struct {
	*gclientx.Client
	collectionName string
	partitionName  string
}

func (d *documents) Insert(ctx context.Context, configs ...DocumentsConfig) (DocumentsWriteResponse, error) {
	request := newDocumentsWriteRequest(validInsertDocument, d.partitionName, configs...)
	if len(request.Docs) == 0 {
		return nil, gerror.NewCode(gcode.CodeInvalidParameter, "docs is empty")
	}
	return decode(parseDocumentsWriteResponse, d.PostBytes, ctx, "/collections/"+d.collectionName+"/docs", request)
}

func (d *documents) Update(ctx context.Context, configs ...DocumentsConfig) (DocumentsWriteResponse, error) {
	request := newDocumentsWriteRequest(validUpdateDocument, d.partitionName, configs...)
	if len(request.Docs) == 0 {
		return nil, gerror.NewCode(gcode.CodeInvalidParameter, "docs is empty")
	}
	return decode(parseDocumentsWriteResponse, d.PutBytes, ctx, "/collections/"+d.collectionName+"/docs", request)
}

func (d *documents) Upsert(ctx context.Context, configs ...DocumentsConfig) (DocumentsWriteResponse, error) {
	request := newDocumentsWriteRequest(func(d *doc) bool {
		return validUpdateDocument(d) || validInsertDocument(d)
	}, d.partitionName, configs...)
	if len(request.Docs) == 0 {
		return nil, gerror.NewCode(gcode.CodeInvalidParameter, "docs is empty")
	}
	return decode(parseDocumentsWriteResponse, d.PostBytes, ctx, "/collections/"+d.collectionName+"/docs/upsert", request)
}

func (d *documents) Get(ctx context.Context, ids ...string) (DocumentsReadResponse, error) {
	if len(ids) == 0 {
		return nil, gerror.NewCode(gcode.CodeInvalidParameter, "ids is empty")
	}
	return decode(parseDocumentsReadResponse, d.GetBytes, ctx, "/collections/"+d.collectionName+"/docs"+
		"?ids="+gstr.Join(ids, ",")+"&partition="+gurl.Encode(d.partitionName))
}

func (d *documents) Drop(ctx context.Context, ids ...string) (DocumentsWriteResponse, error) {
	if len(ids) == 0 {
		return nil, gerror.NewCode(gcode.CodeInvalidParameter, "ids is empty")
	}
	request := newDocumentsDropRequest(d.partitionName, ids...)
	return decode(parseDocumentsWriteResponse, d.DeleteBytes, ctx, "/collections/"+d.collectionName+"/docs", request)
}

func (d *documents) DropAll(ctx context.Context) (DocumentsWriteResponse, error) {
	request := newDocumentsDropAllRequest(d.partitionName)
	return decode(parseDocumentsWriteResponse, d.DeleteBytes, ctx, "/collections/"+d.collectionName+"/docs", request)
}

func (d *documents) Query(ctx context.Context, configs ...DocumentsQueryConfig) (DocumentsQueryResponse, error) {
	request := newDocumentsQueryRequest(d.partitionName, configs...)
	return decode(parseDocumentsQueryResponse, d.PostBytes, ctx, "/collections/"+d.collectionName+"/query", request)
}

func (d *documents) GroupQuery(ctx context.Context, field string, configs ...DocumentsGroupQueryConfig) (DocumentsGroupQueryResponse, error) {
	if err := gvalid.New().Rules("required").Messages("group_by_field is required").
		Data(field).Run(ctx); err != nil {
		return nil, err
	}
	request := newDocumentsGroupQueryRequest(d.partitionName, field, configs...)
	return decode(parseDocumentsGroupQueryResponse, d.PostBytes, ctx, "/collections/"+d.collectionName+"/query_group_by", request)
}

func parseDocumentsWriteResponse(json *gjson.Json) DocumentsWriteResponse {
	return &documentsWriteResponse{
		Response: parseResponse(json),
		Output: lo.Map(json.Get("output").Array(),
			func(item any, _ int) DocOpResult {
				return parseDocOpResult(gjson.New(item))
			}),
		Usage: parseResponseUsage(json.GetJson("usage")),
	}
}

type documentsWriteResponse struct {
	Response
	Output []DocOpResult
	Usage  ResponseUsage
}

func (r *documentsWriteResponse) GetOutput() []DocOpResult {
	return r.Output
}

func (r *documentsWriteResponse) GetUsage() ResponseUsage {
	return r.Usage
}

func parseDocumentsReadResponse(json *gjson.Json) DocumentsReadResponse {
	return &documentsReadResponse{
		Response: parseResponse(json),
		Output: lo.MapValues(json.Get("output").MapStrAny(),
			func(value any, _ string) Doc {
				return parseDoc(gjson.New(value))
			}),
		Usage: parseResponseUsage(json.GetJson("usage")),
	}
}

type documentsReadResponse struct {
	Response
	Output map[string]Doc
	Usage  ResponseUsage
}

func (r *documentsReadResponse) GetOutput() map[string]Doc {
	return r.Output
}

func (r *documentsReadResponse) GetUsage() ResponseUsage {
	return r.Usage
}

func parseDocumentsQueryResponse(json *gjson.Json) DocumentsQueryResponse {
	return &documentsQueryResponse{
		Response: parseResponse(json),
		Output: lo.Map(json.Get("output").Array(),
			func(item any, _ int) Doc {
				return parseDoc(gjson.New(item))
			}),
		Usage: parseResponseUsage(json.GetJson("usage")),
	}
}

type documentsQueryResponse struct {
	Response
	Output []Doc
	Usage  ResponseUsage
}

func (r *documentsQueryResponse) GetOutput() []Doc {
	return r.Output
}

func (r *documentsQueryResponse) GetUsage() ResponseUsage {
	return r.Usage
}

func parseDocumentsGroupQueryResponse(json *gjson.Json) DocumentsGroupQueryResponse {
	return &documentsGroupQueryResponse{
		Response: parseResponse(json),
		Output: lo.Map(json.Get("output").Array(),
			func(item any, _ int) Group { return parseGroup(gjson.New(item)) }),
	}
}

type documentsGroupQueryResponse struct {
	Response
	Output []Group
}

func (r *documentsGroupQueryResponse) GetOutput() []Group {
	return r.Output
}
