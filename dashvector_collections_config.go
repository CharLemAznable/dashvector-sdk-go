package dashvector

type CollectionConfig func(*collectionCreateRequest)

type ExtraParamsConfig func(*extraParams)

type VectorSchemaConfig func(*vectorSchemaParam)

////////////////////////////////////////////////////////////////////////////////

func WithDimension(dimension int) CollectionConfig {
	return func(request *collectionCreateRequest) {
		request.Dimension = dimension
	}
}

func WithDataType(dataType DataType) CollectionConfig {
	return func(request *collectionCreateRequest) {
		request.DataType = dataType
	}
}

func WithMetric(metric Metric) CollectionConfig {
	return func(request *collectionCreateRequest) {
		request.Metric = metric
	}
}

func WithFieldSchema(name string, fieldType FieldType) CollectionConfig {
	return func(request *collectionCreateRequest) {
		if request.FieldsSchema == nil {
			request.FieldsSchema = make(map[string]FieldType)
		}
		request.FieldsSchema[name] = fieldType
	}
}

func WithExtraParams(configs ...ExtraParamsConfig) CollectionConfig {
	return func(request *collectionCreateRequest) {
		request.ExtraParams = newExtraParams(configs...)
	}
}

func WithVectorSchema(name string, dimension int, configs ...VectorSchemaConfig) CollectionConfig {
	return func(request *collectionCreateRequest) {
		if request.VectorsSchema == nil {
			request.VectorsSchema = make(map[string]*vectorSchemaParam)
		}
		request.VectorsSchema[name] = newVectorSchemaParam(dimension, configs...)
	}
}

////////////////////////////////////////////////////////////////////////////////

func WithQuantizeType(quantizeType QuantizeType) ExtraParamsConfig {
	return func(extraParams *extraParams) {
		extraParams.QuantizeType = quantizeType
	}
}

func WithAutoId(autoId string) ExtraParamsConfig {
	return func(extraParams *extraParams) {
		extraParams.AutoId = autoId
	}
}

////////////////////////////////////////////////////////////////////////////////

func WithVectorDataType(dataType DataType) VectorSchemaConfig {
	return func(vectorParam *vectorSchemaParam) {
		vectorParam.DataType = dataType
	}
}

func WithVectorMetric(metric Metric) VectorSchemaConfig {
	return func(vectorParam *vectorSchemaParam) {
		vectorParam.Metric = metric
	}
}

func WithVectorQuantizeType(quantizeType QuantizeType) VectorSchemaConfig {
	return func(vectorParam *vectorSchemaParam) {
		vectorParam.QuantizeType = quantizeType
	}
}

////////////////////////////////////////////////////////////////////////////////

func newCollectionCreateRequest(collectionName string, configs ...CollectionConfig) *collectionCreateRequest {
	request := &collectionCreateRequest{Name: collectionName}
	for _, cfg := range configs {
		cfg(request)
	}
	return request
}

type collectionCreateRequest struct {
	Name          string                        `json:"name"`
	Dimension     int                           `json:"dimension,omitempty"`
	DataType      DataType                      `json:"dtype,omitempty"`
	Metric        Metric                        `json:"metric,omitempty"`
	FieldsSchema  map[string]FieldType          `json:"fields_schema,omitempty"`
	ExtraParams   *extraParams                  `json:"extra_params,omitempty"`
	VectorsSchema map[string]*vectorSchemaParam `json:"vectors_schema,omitempty"`
}

func newExtraParams(configs ...ExtraParamsConfig) *extraParams {
	if len(configs) == 0 {
		return nil
	}
	params := &extraParams{}
	for _, config := range configs {
		config(params)
	}
	return params
}

type extraParams struct {
	QuantizeType QuantizeType `json:"quantize_type,omitempty"`
	AutoId       string       `json:"auto_id,omitempty"`
}

func newVectorSchemaParam(dimension int, configs ...VectorSchemaConfig) *vectorSchemaParam {
	param := &vectorSchemaParam{Dimension: dimension}
	for _, config := range configs {
		config(param)
	}
	return param
}

type vectorSchemaParam struct {
	Dimension    int          `json:"dimension"`
	DataType     DataType     `json:"dtype,omitempty"`
	Metric       Metric       `json:"metric,omitempty"`
	QuantizeType QuantizeType `json:"quantize_type,omitempty"`
}
