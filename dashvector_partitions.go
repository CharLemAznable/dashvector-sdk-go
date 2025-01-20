package dashvector

import (
	"context"
	"github.com/CharLemAznable/gfx/net/gclientx"
	"github.com/gogf/gf/v2/container/gmap"
	"github.com/gogf/gf/v2/encoding/gjson"
	"time"
)

func newPartitions(client *gclientx.Client, collectionName string) Collection {
	p := &partitions{
		Client:         client,
		collectionName: collectionName,
		partitionsMap:  gmap.NewStrAnyMap(true),
	}
	p.Partition = p.GetPartition()
	return p
}

type partitions struct {
	*gclientx.Client
	collectionName string
	partitionsMap  *gmap.StrAnyMap
	Partition
}

func (p *partitions) Create(ctx context.Context, partitionName string) (Response, error) {
	if err := validatePartitionName(ctx, partitionName); err != nil {
		return nil, err
	}
	request := newPartitionCreateRequest(partitionName)
	return decode(parseResponse, p.PostBytes, ctx, "/collections/"+p.collectionName+"/partitions", request)
}

func (p *partitions) Desc(ctx context.Context, partitionName string) (PartitionDescResponse, error) {
	if err := validatePartitionName(ctx, partitionName); err != nil {
		return nil, err
	}
	return decode(parsePartitionDescResponse, p.GetBytes, ctx, "/collections/"+p.collectionName+"/partitions/"+partitionName)
}

func (p *partitions) List(ctx context.Context) (PartitionListResponse, error) {
	return decode(parsePartitionListResponse, p.GetBytes, ctx, "/collections/"+p.collectionName+"/partitions")
}

func (p *partitions) Stats(ctx context.Context, partitionName string) (PartitionStatsResponse, error) {
	if err := validatePartitionName(ctx, partitionName); err != nil {
		return nil, err
	}
	return decode(parsePartitionStatsResponse, p.GetBytes, ctx, "/collections/"+p.collectionName+"/partitions/"+partitionName+"/stats")
}

func (p *partitions) Delete(ctx context.Context, partitionName string) (Response, error) {
	if err := validatePartitionName(ctx, partitionName); err != nil {
		return nil, err
	}
	return decode(parseResponse, p.DeleteBytes, ctx, "/collections/"+p.collectionName+"/partitions/"+partitionName)
}

func (p *partitions) CreateServing(ctx context.Context, partitionName string) (Response, error) {
	createResponse, err := p.Create(ctx, partitionName)
	if err != nil || createResponse.GetCode() != 0 {
		return createResponse, err
	}
	for {
		time.Sleep(time.Millisecond * 10)
		descResponse, err := p.Desc(ctx, partitionName)
		if err != nil || descResponse.GetOutput() == StatusServing {
			return descResponse, err
		}
	}
}

const defaultPartitionName = "default"

func (p *partitions) GetPartition(partitionName ...string) Partition {
	name := defaultPartitionName
	if len(partitionName) > 0 && partitionName[0] != "" {
		name = partitionName[0]
	}
	return p.partitionsMap.GetOrSetFuncLock(name, func() any {
		return newDocuments(p.Client, p.collectionName, name)
	}).(Partition)
}

func newPartitionCreateRequest(partitionName string) *partitionCreateRequest {
	return &partitionCreateRequest{Name: partitionName}
}

type partitionCreateRequest struct {
	Name string `json:"name"`
}

func parsePartitionDescResponse(json *gjson.Json) PartitionDescResponse {
	return &partitionDescResponse{
		Response: parseResponse(json),
		Output:   Status(json.Get("output").String()),
	}
}

type partitionDescResponse struct {
	Response
	Output Status
}

func (r *partitionDescResponse) GetOutput() Status {
	return r.Output
}

func parsePartitionListResponse(json *gjson.Json) PartitionListResponse {
	return &partitionListResponse{
		Response: parseResponse(json),
		Output:   json.Get("output").Strings(),
	}
}

type partitionListResponse struct {
	Response
	Output []string
}

func (r *partitionListResponse) GetOutput() []string {
	return r.Output
}

func parsePartitionStatsResponse(json *gjson.Json) PartitionStatsResponse {
	return &partitionStatsResponse{
		Response: parseResponse(json),
		Output:   parsePartitionStats(json.GetJson("output")),
	}
}

type partitionStatsResponse struct {
	Response
	Output PartitionStats
}

func (r *partitionStatsResponse) GetOutput() PartitionStats {
	return r.Output
}
