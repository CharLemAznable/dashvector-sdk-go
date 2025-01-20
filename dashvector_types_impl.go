package dashvector

import (
	"github.com/gogf/gf/v2/container/gvar"
	"github.com/gogf/gf/v2/encoding/gjson"
	"github.com/gogf/gf/v2/util/gconv"
	"github.com/samber/lo"
)

func parseCollectionMeta(json *gjson.Json) CollectionMeta {
	return &collectionMeta{
		Name:      json.Get("name").String(),
		Dimension: json.Get("dimension").Int(),
		DataType:  DataType(json.Get("dtype").String()),
		Metric:    Metric(json.Get("metric").String()),
		Status:    Status(json.Get("status").String()),
		FieldsSchema: lo.MapValues(json.Get("fields_schema").MapStrStr(),
			func(value string, _ string) FieldType { return FieldType(value) }),
		VectorsSchema: lo.MapValues(json.Get("vectors_schema").MapStrAny(),
			func(value any, _ string) VectorSchema {
				return parseVectorSchema(gjson.New(value))
			}),
		PartitionStatus: lo.MapValues(json.Get("partitions").MapStrStr(),
			func(value string, _ string) Status { return Status(value) }),
	}
}

type collectionMeta struct {
	Name            string
	Dimension       int
	DataType        DataType
	Metric          Metric
	Status          Status
	FieldsSchema    map[string]FieldType
	VectorsSchema   map[string]VectorSchema
	PartitionStatus map[string]Status
}

func (m *collectionMeta) GetName() string {
	return m.Name
}

func (m *collectionMeta) GetDimension() int {
	return m.Dimension
}

func (m *collectionMeta) GetDataType() DataType {
	return m.DataType
}

func (m *collectionMeta) GetMetric() Metric {
	return m.Metric
}

func (m *collectionMeta) GetStatus() Status {
	return m.Status
}

func (m *collectionMeta) GetFieldsSchema() map[string]FieldType {
	return m.FieldsSchema
}

func (m *collectionMeta) GetVectorsSchema() map[string]VectorSchema {
	return m.VectorsSchema
}

func (m *collectionMeta) GetPartitionStatus() map[string]Status {
	return m.PartitionStatus
}

func parseVectorSchema(json *gjson.Json) VectorSchema {
	return &vectorSchema{
		Dimension:    json.Get("dimension").Int(),
		DataType:     DataType(json.Get("dtype").String()),
		Metric:       Metric(json.Get("metric").String()),
		QuantizeType: QuantizeType(json.Get("quantize_type").String()),
	}
}

type vectorSchema struct {
	Dimension    int
	DataType     DataType
	Metric       Metric
	QuantizeType QuantizeType
}

func (s *vectorSchema) GetDimension() int {
	return s.Dimension
}

func (s *vectorSchema) GetDataType() DataType {
	return s.DataType
}

func (s *vectorSchema) GetMetric() Metric {
	return s.Metric
}

func (s *vectorSchema) GetQuantizeType() QuantizeType {
	return s.QuantizeType
}

func parseCollectionStats(json *gjson.Json) CollectionStats {
	return &collectionStats{
		TotalDocCount:     json.Get("total_doc_count").Int64(),
		IndexCompleteness: json.Get("index_completeness").Float64(),
		Partitions: lo.MapValues(json.Get("partitions").MapStrAny(),
			func(value any, _ string) PartitionStats {
				return parsePartitionStats(gjson.New(value))
			}),
	}
}

type collectionStats struct {
	TotalDocCount     int64
	IndexCompleteness float64
	Partitions        map[string]PartitionStats
}

func (s *collectionStats) GetTotalDocCount() int64 {
	return s.TotalDocCount
}

func (s *collectionStats) GetIndexCompleteness() float64 {
	return s.IndexCompleteness
}

func (s *collectionStats) GetPartitions() map[string]PartitionStats {
	return s.Partitions
}

func parsePartitionStats(json *gjson.Json) PartitionStats {
	return &partitionStats{
		TotalDocCount: json.Get("total_doc_count").Int64(),
	}
}

type partitionStats struct {
	TotalDocCount int64
}

func (s *partitionStats) GetTotalDocCount() int64 {
	return s.TotalDocCount
}

func parseDoc(json *gjson.Json) Doc {
	return &doc{
		Id:     json.Get("id").String(),
		Vector: json.Get("vector").Float32s(),
		Vectors: lo.MapValues(json.Get("vectors").MapStrAny(),
			func(value any, _ string) []float32 {
				return gvar.New(value).Float32s()
			}),
		SparseVector: lo.MapEntries(json.Get("sparse_vector").MapStrAny(),
			func(key string, value any) (int32, float32) {
				return gconv.Int32(key), gconv.Float32(value)
			}),
		Fields: json.Get("fields").MapStrAny(),
		Score:  json.Get("score").Float32(),
	}
}

type doc struct {
	Id           string               `json:"id,omitempty"`
	Vector       []float32            `json:"vector,omitempty"`
	Vectors      map[string][]float32 `json:"vectors,omitempty"`
	SparseVector map[int32]float32    `json:"sparse_vector,omitempty"`
	Fields       map[string]any       `json:"fields,omitempty"`
	Score        float32              `json:"score,omitempty"`
}

func (d *doc) GetId() string {
	return d.Id
}

func (d *doc) GetVector() []float32 {
	return d.Vector
}

func (d *doc) GetVectors() map[string][]float32 {
	return d.Vectors
}

func (d *doc) GetSparseVector() map[int32]float32 {
	return d.SparseVector
}

func (d *doc) GetFields() map[string]any {
	return d.Fields
}

func (d *doc) GetScore() float32 {
	return d.Score
}

func parseGroup(json *gjson.Json) Group {
	return &group{
		GroupId: json.Get("group_id").String(),
		Docs: lo.Map(json.Get("docs").Array(),
			func(item any, _ int) Doc { return parseDoc(gjson.New(item)) }),
	}
}

type group struct {
	GroupId string
	Docs    []Doc
}

func (g *group) GetGroupId() string {
	return g.GroupId
}

func (g *group) GetDocs() []Doc {
	return g.Docs
}

func parseDocOpResult(json *gjson.Json) DocOpResult {
	return &docOpResult{
		Id:      json.Get("id").String(),
		Code:    json.Get("code").Int(),
		Message: json.Get("message").String(),
		DocOp:   DocOp(json.Get("doc_op").String()),
	}
}

type docOpResult struct {
	Id      string
	Code    int
	Message string
	DocOp   DocOp
}

func (r *docOpResult) GetId() string {
	return r.Id
}

func (r *docOpResult) GetCode() int {
	return r.Code
}

func (r *docOpResult) GetMessage() string {
	return r.Message
}

func (r *docOpResult) GetDocOp() DocOp {
	return r.DocOp
}

func parseResponse(json *gjson.Json) Response {
	return &response{
		Code:      json.Get("code").Int(),
		Message:   json.Get("message").String(),
		RequestId: json.Get("request_id").String(),
	}
}

type response struct {
	Code      int
	Message   string
	RequestId string
}

func (r *response) GetCode() int {
	return r.Code
}

func (r *response) GetMessage() string {
	return r.Message
}

func (r *response) GetRequestId() string {
	return r.RequestId
}

func parseResponseUsage(json *gjson.Json) ResponseUsage {
	if json.IsNil() {
		return nil
	}
	return &responseUsage{
		ReadUnits:  json.Get("read_units").Int(),
		WriteUnits: json.Get("write_units").Int(),
	}
}

type responseUsage struct {
	ReadUnits  int
	WriteUnits int
}

func (u *responseUsage) GetReadUnits() int {
	return u.ReadUnits
}

func (u *responseUsage) GetWriteUnits() int {
	return u.WriteUnits
}
