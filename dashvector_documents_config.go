package dashvector

import "github.com/gogf/gf/v2/encoding/gjson"

type DocumentsConfig func(*documentsWriteRequest, func(*doc) bool)

type DocumentConfig func(*doc)

type DocumentsQueryConfig func(*documentsQueryRequest)

type VectorQueryConfig func(*vectorQuery)

type DocumentsGroupQueryConfig func(*documentsGroupQueryRequest)

////////////////////////////////////////////////////////////////////////////////

func WithDocument(configs ...DocumentConfig) DocumentsConfig {
	return func(request *documentsWriteRequest, valid func(*doc) bool) {
		document := &doc{}
		for _, cfg := range configs {
			cfg(document)
		}
		if !valid(document) {
			return
		}
		request.Docs = append(request.Docs, document)
	}
}

////////////////////////////////////////////////////////////////////////////////

func WithId(id string) DocumentConfig {
	return func(doc *doc) {
		doc.Id = id
	}
}

func WithVector(vector ...float32) DocumentConfig {
	return func(doc *doc) {
		doc.Vector = vector
	}
}

func WithSchemaVector(name string, vector ...float32) DocumentConfig {
	return func(doc *doc) {
		if doc.Vectors == nil {
			doc.Vectors = make(map[string][]float32)
		}
		doc.Vectors[name] = vector
	}
}

func WithSparseVector(key int32, value float32) DocumentConfig {
	return func(doc *doc) {
		if doc.SparseVector == nil {
			doc.SparseVector = make(map[int32]float32)
		}
		doc.SparseVector[key] = value
	}
}

func WithField(name string, value any) DocumentConfig {
	return func(doc *doc) {
		if doc.Fields == nil {
			doc.Fields = make(map[string]any)
		}
		doc.Fields[name] = value
	}
}

////////////////////////////////////////////////////////////////////////////////

func QueryWithVector(vector ...float32) DocumentsQueryConfig {
	return func(request *documentsQueryRequest) {
		request.Vector = vector
	}
}

func QueryWithVectorQueryParam(configs ...VectorQueryConfig) DocumentsQueryConfig {
	return func(request *documentsQueryRequest) {
		request.VectorParam = newVectorParamQuery(configs...)
	}
}

func QueryWithSparseVector(key int32, value float32) DocumentsQueryConfig {
	return func(request *documentsQueryRequest) {
		if request.SparseVector == nil {
			request.SparseVector = make(map[int32]float32)
		}
		request.SparseVector[key] = value
	}
}

func QueryWithId(id string) DocumentsQueryConfig {
	return func(request *documentsQueryRequest) {
		request.Id = id
	}
}

func QueryWithTopk(topk int) DocumentsQueryConfig {
	return func(request *documentsQueryRequest) {
		request.Topk = topk
	}
}

func QueryWithIncludeVector(includeVector bool) DocumentsQueryConfig {
	return func(request *documentsQueryRequest) {
		request.IncludeVector = includeVector
	}
}

func QueryWithFilter(filter string) DocumentsQueryConfig {
	return func(request *documentsQueryRequest) {
		request.Filter = filter
	}
}

func QueryWithOutputFields(fields ...string) DocumentsQueryConfig {
	return func(request *documentsQueryRequest) {
		request.OutputFields = fields
	}
}

func QueryWithSchemaVector(name string, vector []float32, configs ...VectorQueryConfig) DocumentsQueryConfig {
	return func(request *documentsQueryRequest) {
		if request.Vectors == nil {
			request.Vectors = make(map[string]*vectorQuery)
		}
		request.Vectors[name] = newVectorQuery(vector, configs...)
	}
}

func QueryWithRrfRanker(rankConstant int) DocumentsQueryConfig {
	return func(request *documentsQueryRequest) {
		request.Rerank = newRrfRanker(rankConstant)
	}
}

func QueryWithWeightedRanker(weights map[string]float32) DocumentsQueryConfig {
	return func(request *documentsQueryRequest) {
		request.Rerank = newWeightedRanker(weights)
	}
}

////////////////////////////////////////////////////////////////////////////////

func QueryWithNumCandidates(numCandidates int) VectorQueryConfig {
	return func(param *vectorQuery) {
		param.NumCandidates = numCandidates
	}
}

func QueryWithLinear(isLinear bool) VectorQueryConfig {
	return func(param *vectorQuery) {
		param.IsLinear = isLinear
	}
}

func QueryWithEf(ef int) VectorQueryConfig {
	return func(param *vectorQuery) {
		param.Ef = ef
	}
}

func QueryWithRadius(radius float32) VectorQueryConfig {
	return func(param *vectorQuery) {
		param.Radius = radius
	}
}

////////////////////////////////////////////////////////////////////////////////

func GroupQueryWithCount(groupCount int) DocumentsGroupQueryConfig {
	return func(request *documentsGroupQueryRequest) {
		request.GroupCount = groupCount
	}
}

func GroupQueryWithTopk(groupTopk int) DocumentsGroupQueryConfig {
	return func(request *documentsGroupQueryRequest) {
		request.GroupTopk = groupTopk
	}
}

func GroupQueryWithVector(vector ...float32) DocumentsGroupQueryConfig {
	return func(request *documentsGroupQueryRequest) {
		request.Vector = vector
	}
}

func GroupQueryWithSparseVector(key int32, value float32) DocumentsGroupQueryConfig {
	return func(request *documentsGroupQueryRequest) {
		if request.SparseVector == nil {
			request.SparseVector = make(map[int32]float32)
		}
		request.SparseVector[key] = value
	}
}

func GroupQueryWithId(id string) DocumentsGroupQueryConfig {
	return func(request *documentsGroupQueryRequest) {
		request.Id = id
	}
}

func GroupQueryWithIncludeVector(includeVector bool) DocumentsGroupQueryConfig {
	return func(request *documentsGroupQueryRequest) {
		request.IncludeVector = includeVector
	}
}

func GroupQueryWithFilter(filter string) DocumentsGroupQueryConfig {
	return func(request *documentsGroupQueryRequest) {
		request.Filter = filter
	}
}

func GroupQueryWithOutputFields(fields ...string) DocumentsGroupQueryConfig {
	return func(request *documentsGroupQueryRequest) {
		request.OutputFields = fields
	}
}

func GroupQueryWithSchemaVector(vectorField string) DocumentsGroupQueryConfig {
	return func(request *documentsGroupQueryRequest) {
		request.VectorField = vectorField
	}
}

////////////////////////////////////////////////////////////////////////////////

func newDocumentsWriteRequest(valid func(*doc) bool, partition string, configs ...DocumentsConfig) *documentsWriteRequest {
	request := &documentsWriteRequest{Docs: make([]*doc, 0), Partition: partition}
	for _, cfg := range configs {
		cfg(request, valid)
	}
	return request
}

type documentsWriteRequest struct {
	Docs      []*doc `json:"docs"`
	Partition string `json:"partition,omitempty"`
}

////////////////////////////////////////////////////////////////////////////////

func newDocumentsDropRequest(partition string, ids ...string) *documentsDropRequest {
	return &documentsDropRequest{
		Ids:       ids,
		Partition: partition,
	}
}

func newDocumentsDropAllRequest(partition string) *documentsDropRequest {
	return &documentsDropRequest{
		Partition: partition,
		DeleteAll: true,
	}
}

type documentsDropRequest struct {
	Ids       []string `json:"ids"`
	Partition string   `json:"partition,omitempty"`
	DeleteAll bool     `json:"delete_all,omitempty"`
}

////////////////////////////////////////////////////////////////////////////////

func newDocumentsQueryRequest(partition string, configs ...DocumentsQueryConfig) *documentsQueryRequest {
	request := &documentsQueryRequest{Partition: partition}
	for _, cfg := range configs {
		cfg(request)
	}
	return request
}

type documentsQueryRequest struct {
	Vector        []float32               `json:"vector,omitempty"`
	VectorParam   *vectorQuery            `json:"vector_param,omitempty"`
	SparseVector  map[int32]float32       `json:"sparse_vector,omitempty"`
	Id            string                  `json:"id,omitempty"`
	Topk          int                     `json:"topk,omitempty"`
	IncludeVector bool                    `json:"include_vector,omitempty"`
	Filter        string                  `json:"filter,omitempty"`
	OutputFields  []string                `json:"output_fields,omitempty"`
	Vectors       map[string]*vectorQuery `json:"vectors,omitempty"`
	Rerank        *rerank                 `json:"rerank,omitempty"`
	Partition     string                  `json:"partition,omitempty"`
}

func newVectorParamQuery(configs ...VectorQueryConfig) *vectorQuery {
	if len(configs) == 0 {
		return nil
	}
	return newVectorQuery(nil, configs...)
}

func newVectorQuery(vector []float32, configs ...VectorQueryConfig) *vectorQuery {
	param := &vectorQuery{Vector: vector}
	for _, cfg := range configs {
		cfg(param)
	}
	return param
}

type vectorQuery struct {
	Vector        []float32 `json:"vector,omitempty"`
	NumCandidates int       `json:"num_candidates,omitempty"`
	IsLinear      bool      `json:"is_linear,omitempty"`
	Ef            int       `json:"ef,omitempty"`
	Radius        float32   `json:"radius,omitempty"`
}

func newRrfRanker(rankConstant int) *rerank {
	return &rerank{
		RankerName: "rrf",
		RankerParams: &rrfRanker{
			RankConstant: rankConstant,
		},
	}
}

func newWeightedRanker(weights map[string]float32) *rerank {
	return &rerank{
		RankerName: "weighted",
		RankerParams: &weightedRanker{
			Weights: gjson.New(weights).MustToJsonString(),
		},
	}
}

type rerank struct {
	RankerName   string      `json:"ranker_name,omitempty"`
	RankerParams interface{} `json:"ranker_params,omitempty"`
}

type rrfRanker struct {
	RankConstant int `json:"rank_constant,omitempty"`
}

type weightedRanker struct {
	Weights string `json:"weights,omitempty"`
}

////////////////////////////////////////////////////////////////////////////////

func newDocumentsGroupQueryRequest(partition string, field string, configs ...DocumentsGroupQueryConfig) *documentsGroupQueryRequest {
	request := &documentsGroupQueryRequest{Partition: partition, GroupByField: field}
	for _, cfg := range configs {
		cfg(request)
	}
	return request
}

type documentsGroupQueryRequest struct {
	GroupByField  string            `json:"group_by_field"`
	GroupCount    int               `json:"group_count,omitempty"`
	GroupTopk     int               `json:"group_topk,omitempty"`
	Vector        []float32         `json:"vector,omitempty"`
	SparseVector  map[int32]float32 `json:"sparse_vector,omitempty"`
	Id            string            `json:"id,omitempty"`
	IncludeVector bool              `json:"include_vector,omitempty"`
	Filter        string            `json:"filter,omitempty"`
	OutputFields  []string          `json:"output_fields,omitempty"`
	VectorField   string            `json:"vector_field,omitempty"`
	Partition     string            `json:"partition,omitempty"`
}
