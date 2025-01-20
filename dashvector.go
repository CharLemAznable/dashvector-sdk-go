package dashvector

import "context"

func NewClient(ctx context.Context, clientName ...string) Client {
	return newCollections(client(ctx, clientName...))
}

type Client interface {
	Create(ctx context.Context, collectionName string, configs ...CollectionConfig) (Response, error)
	Desc(ctx context.Context, collectionName string) (CollectionDescResponse, error)
	List(ctx context.Context) (CollectionListResponse, error)
	Stats(ctx context.Context, collectionName string) (CollectionStatsResponse, error)
	Delete(ctx context.Context, collectionName string) (Response, error)
	CreateServing(ctx context.Context, collectionName string, configs ...CollectionConfig) (Response, error)
	GetCollection(collectionName string) Collection
}

type CollectionDescResponse interface {
	Response
	GetOutput() CollectionMeta
}

type CollectionListResponse interface {
	Response
	GetOutput() []string
}

type CollectionStatsResponse interface {
	Response
	GetOutput() CollectionStats
}

type Collection interface {
	Create(ctx context.Context, partitionName string) (Response, error)
	Desc(ctx context.Context, partitionName string) (PartitionDescResponse, error)
	List(ctx context.Context) (PartitionListResponse, error)
	Stats(ctx context.Context, partitionName string) (PartitionStatsResponse, error)
	Delete(ctx context.Context, partitionName string) (Response, error)
	CreateServing(ctx context.Context, partitionName string) (Response, error)
	GetPartition(partitionName ...string) Partition
	Partition
}

type PartitionDescResponse interface {
	Response
	GetOutput() Status
}

type PartitionListResponse interface {
	Response
	GetOutput() []string
}

type PartitionStatsResponse interface {
	Response
	GetOutput() PartitionStats
}

type Partition interface {
	Insert(ctx context.Context, configs ...DocumentsConfig) (DocumentsWriteResponse, error)
	Update(ctx context.Context, configs ...DocumentsConfig) (DocumentsWriteResponse, error)
	Upsert(ctx context.Context, configs ...DocumentsConfig) (DocumentsWriteResponse, error)
	Get(ctx context.Context, ids ...string) (DocumentsReadResponse, error)
	Drop(ctx context.Context, ids ...string) (DocumentsWriteResponse, error)
	DropAll(ctx context.Context) (DocumentsWriteResponse, error)
	Query(ctx context.Context, configs ...DocumentsQueryConfig) (DocumentsQueryResponse, error)
	GroupQuery(ctx context.Context, field string, configs ...DocumentsGroupQueryConfig) (DocumentsGroupQueryResponse, error)
}

type DocumentsWriteResponse interface {
	Response
	GetOutput() []DocOpResult
	GetUsage() ResponseUsage
}

type DocumentsReadResponse interface {
	Response
	GetOutput() map[string]Doc
	GetUsage() ResponseUsage
}

type DocumentsQueryResponse interface {
	Response
	GetOutput() []Doc
	GetUsage() ResponseUsage
}

type DocumentsGroupQueryResponse interface {
	Response
	GetOutput() []Group
}
