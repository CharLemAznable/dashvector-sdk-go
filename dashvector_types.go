package dashvector

type DataType string

//goland:noinspection GoUnusedConst
const (
	DataTypeFloat DataType = "FLOAT"
	DataTypeInt   DataType = "INT"
)

type Metric string

//goland:noinspection GoUnusedConst
const (
	MetricEuclidean  Metric = "euclidean"
	MetricDotproduct Metric = "dotproduct"
	MetricCosine     Metric = "cosine"
)

type FieldType string

//goland:noinspection GoUnusedConst
const (
	FieldTypeBool   FieldType = "BOOL"
	FieldTypeString FieldType = "STRING"
	FieldTypeInt    FieldType = "INT"
	FieldTypeFloat  FieldType = "FLOAT"
)

type QuantizeType string

//goland:noinspection GoUnusedConst
const QuantizeTypeInt8 = "DT_VECTOR_INT8"

type Status string

//goland:noinspection GoUnusedConst
const (
	StatusInitialized Status = "INITIALIZED"
	StatusServing     Status = "SERVING"
	StatusDropping    Status = "DROPPING"
	StatusError       Status = "ERROR"
)

type CollectionMeta interface {
	GetName() string
	GetDimension() int
	GetDataType() DataType
	GetMetric() Metric
	GetStatus() Status
	GetFieldsSchema() map[string]FieldType
	GetVectorsSchema() map[string]VectorSchema
	GetPartitionStatus() map[string]Status
}

type VectorSchema interface {
	GetDimension() int
	GetDataType() DataType
	GetMetric() Metric
	GetQuantizeType() QuantizeType
}

type CollectionStats interface {
	GetTotalDocCount() int64
	GetIndexCompleteness() float64
	GetPartitions() map[string]PartitionStats
}

type PartitionStats interface {
	GetTotalDocCount() int64
}

type Doc interface {
	GetId() string
	GetVector() []float32
	GetVectors() map[string][]float32
	GetSparseVector() map[int32]float32
	GetFields() map[string]any
	GetScore() float32
}

type Group interface {
	GetGroupId() string
	GetDocs() []Doc
}

type DocOp string

//goland:noinspection GoUnusedConst
const (
	DocOpInsert DocOp = "insert"
	DocOpUpdate DocOp = "update"
	DocOpUpsert DocOp = "upsert"
	DocOpDelete DocOp = "delete"
)

type DocOpResult interface {
	GetId() string
	GetCode() int
	GetMessage() string
	GetDocOp() DocOp
}

type Response interface {
	GetCode() int
	GetMessage() string
	GetRequestId() string
}

type ResponseUsage interface {
	GetReadUnits() int
	GetWriteUnits() int
}
