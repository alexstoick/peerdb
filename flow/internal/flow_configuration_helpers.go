package internal

import (
	"context"
	"database/sql"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func TableNameMapping(tableMappings []*protos.TableMapping, resync bool) map[string]string {
	tblNameMapping := make(map[string]string, len(tableMappings))
	if resync {
		for _, mapping := range tableMappings {
			if mapping.Engine != protos.TableEngine_CH_ENGINE_NULL {
				mapping.DestinationTableIdentifier += "_resync"
			}
		}
	}
	for _, v := range tableMappings {
		tblNameMapping[v.SourceTableIdentifier] = v.DestinationTableIdentifier
	}

	return tblNameMapping
}

func FetchConfigFromDB(ctx context.Context, catalogPool shared.CatalogPool, flowName string) (*protos.FlowConnectionConfigsCore, error) {
	var configBytes sql.RawBytes
	if err := catalogPool.QueryRow(ctx,
		"SELECT config_proto FROM flows WHERE name = $1 LIMIT 1", flowName,
	).Scan(&configBytes); err != nil {
		return nil, fmt.Errorf("unable to query flow config from catalog: %w", err)
	}

	var cfgFromDB protos.FlowConnectionConfigsCore
	if err := proto.Unmarshal(configBytes, &cfgFromDB); err != nil {
		return nil, fmt.Errorf("unable to unmarshal flow config: %w", err)
	}

	return &cfgFromDB, nil
}
