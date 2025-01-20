package dashvector

import (
	"context"
	"fmt"
	"github.com/CharLemAznable/gfx/container/gvarx"
	"github.com/CharLemAznable/gfx/frame/gx"
	"github.com/CharLemAznable/gfx/net/gclientx"
	"github.com/gogf/gf/v2/container/gmap"
	"github.com/gogf/gf/v2/encoding/gjson"
	"github.com/gogf/gf/v2/errors/gerror"
	"github.com/gogf/gf/v2/frame/g"
)

const (
	loggerName = "dashvector"

	defaultClientName = "default"

	configKeyForClusterEndpoint    = "dashvector.clusterEndpoint"
	configKeyFmtForClusterEndpoint = "dashvector.%s.clusterEndpoint"
	configKeyForApiKey             = "dashvector.apiKey"
	configKeyFmtForApiKey          = "dashvector.%s.apiKey"

	baseUrlFmt      = "https://%s/v1"
	headerAuthToken = "dashvector-auth-token"
)

var (
	logger = g.Log(loggerName)

	clientMapping = gmap.NewStrAnyMap(true)
)

func client(ctx context.Context, clientName ...string) *gclientx.Client {
	configKey := defaultClientName
	if len(clientName) > 0 && clientName[0] != "" {
		configKey = clientName[0]
	}
	return clientMapping.GetOrSetFuncLock(configKey, func() any {
		clusterEndpoint := getConfigWithNamePattern(ctx,
			configKeyFmtForClusterEndpoint, configKey, configKeyForClusterEndpoint)
		apiKey := getConfigWithNamePattern(ctx,
			configKeyFmtForApiKey, configKey, configKeyForApiKey)
		if clusterEndpoint == "" || apiKey == "" {
			panic(gerror.Newf("dashvector client config not found: %s", configKey))
		}
		return gx.Client().SetIntLog(logger).ContentJson().
			Prefix(fmt.Sprintf(baseUrlFmt, clusterEndpoint)).
			Header(headerAuthToken, apiKey)
	}).(*gclientx.Client)
}

func getConfigWithNamePattern(ctx context.Context, namePattern, name, defPattern string) (v string) {
	return gvarx.DefaultIfEmpty(g.Cfg().MustGetWithEnv(ctx, fmt.Sprintf(namePattern, name)),
		g.Cfg().MustGetWithEnv(ctx, defPattern).String()).String()
}

func decode[T any](parser func(json *gjson.Json) T,
	requestBytes func(context.Context, string, ...any) ([]byte, error),
	ctx context.Context, url string, data ...any) (T, error) {
	bytes, err := requestBytes(ctx, url, data...)
	if err != nil {
		return any(nil).(T), err
	}
	return parser(gjson.New(bytes)), nil
}
