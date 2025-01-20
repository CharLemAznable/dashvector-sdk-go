package dashvector

import (
	"context"
	"github.com/CharLemAznable/gfx/net/gclientx"
	"github.com/gogf/gf/v2/container/gmap"
	"github.com/gogf/gf/v2/encoding/gjson"
	"time"
)

func newCollections(client *gclientx.Client) Client {
	return &collections{
		Client:         client,
		collectionsMap: gmap.NewStrAnyMap(true),
	}
}

type collections struct {
	*gclientx.Client
	collectionsMap *gmap.StrAnyMap
}

func (c *collections) Create(ctx context.Context, collectionName string, configs ...CollectionConfig) (Response, error) {
	if err := validateCollectionName(ctx, collectionName); err != nil {
		return nil, err
	}
	request := newCollectionCreateRequest(collectionName, configs...)
	return decode(parseResponse, c.PostBytes, ctx, "/collections", request)
}

func (c *collections) Desc(ctx context.Context, collectionName string) (CollectionDescResponse, error) {
	if err := validateCollectionName(ctx, collectionName); err != nil {
		return nil, err
	}
	return decode(parseCollectionDescResponse, c.GetBytes, ctx, "/collections/"+collectionName)
}

func (c *collections) List(ctx context.Context) (CollectionListResponse, error) {
	return decode(parseCollectionListResponse, c.GetBytes, ctx, "/collections")
}

func (c *collections) Stats(ctx context.Context, collectionName string) (CollectionStatsResponse, error) {
	if err := validateCollectionName(ctx, collectionName); err != nil {
		return nil, err
	}
	return decode(parseCollectionStatsResponse, c.GetBytes, ctx, "/collections/"+collectionName+"/stats")
}

func (c *collections) Delete(ctx context.Context, collectionName string) (Response, error) {
	if err := validateCollectionName(ctx, collectionName); err != nil {
		return nil, err
	}
	return decode(parseResponse, c.DeleteBytes, ctx, "/collections/"+collectionName)
}

func (c *collections) CreateServing(ctx context.Context, collectionName string, configs ...CollectionConfig) (Response, error) {
	createResponse, err := c.Create(ctx, collectionName, configs...)
	if err != nil || createResponse.GetCode() != 0 {
		return createResponse, err
	}
	for {
		time.Sleep(time.Millisecond * 10)
		descResponse, err := c.Desc(ctx, collectionName)
		if err != nil || descResponse.GetOutput().GetStatus() == StatusServing {
			return descResponse, err
		}
	}
}

func (c *collections) GetCollection(collectionName string) Collection {
	if err := validateCollectionName(context.Background(), collectionName); err != nil {
		panic(err)
	}
	return c.collectionsMap.GetOrSetFuncLock(collectionName, func() any {
		return newPartitions(c.Client, collectionName)
	}).(Collection)
}

func parseCollectionDescResponse(json *gjson.Json) CollectionDescResponse {
	return &collectionDescResponse{
		Response: parseResponse(json),
		Output:   parseCollectionMeta(json.GetJson("output")),
	}
}

type collectionDescResponse struct {
	Response
	Output CollectionMeta
}

func (r *collectionDescResponse) GetOutput() CollectionMeta {
	return r.Output
}

func parseCollectionListResponse(json *gjson.Json) CollectionListResponse {
	return &collectionListResponse{
		Response: parseResponse(json),
		Output:   json.Get("output").Strings(),
	}
}

type collectionListResponse struct {
	Response
	Output []string
}

func (r *collectionListResponse) GetOutput() []string {
	return r.Output
}

func parseCollectionStatsResponse(json *gjson.Json) CollectionStatsResponse {
	return &collectionStatsResponse{
		Response: parseResponse(json),
		Output:   parseCollectionStats(json.GetJson("output")),
	}
}

type collectionStatsResponse struct {
	Response
	Output CollectionStats
}

func (r *collectionStatsResponse) GetOutput() CollectionStats {
	return r.Output
}
