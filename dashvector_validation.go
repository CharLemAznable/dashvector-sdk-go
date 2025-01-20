package dashvector

import (
	"context"
	"github.com/gogf/gf/v2/util/gvalid"
)

func validateCollectionName(ctx context.Context, collectionName string) error {
	return gvalid.New().Rules("required").
		Messages("collectionName is required").
		Data(collectionName).Run(ctx)
}

func validatePartitionName(ctx context.Context, partitionName string) error {
	return gvalid.New().Rules("required").
		Messages("partitionName is required").
		Data(partitionName).Run(ctx)
}

func validInsertDocument(document *doc) bool {
	if len(document.Vector) > 0 {
		return true
	}
	for name, vec := range document.Vectors {
		if name != "" && len(vec) > 0 {
			return true
		}
	}
	return false
}

func validUpdateDocument(document *doc) bool {
	return document.Id != ""
}
