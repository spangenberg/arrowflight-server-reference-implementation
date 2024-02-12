package noophandler

import (
	"context"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/cloudquery/plugin-sdk/v4/message"

	"github.com/spangenberg/arrowflight-server-reference-implementation/handler"
)

type CloudQuery struct{}

// MigrateTable should implement the migration of a table
func (*CloudQuery) MigrateTable(ctx context.Context, msg message.WriteMigrateTables) error {
	return nil
}

// DeleteRecord should implement the deletion of a records
func (*CloudQuery) DeleteRecord(ctx context.Context, msg message.WriteDeleteRecords) error {
	return nil
}

// DeleteStale should implement the deletion of stale records
func (*CloudQuery) DeleteStale(ctx context.Context, msg message.WriteDeleteStales) error {
	return nil
}

type ArrowFlight struct{}

// DoGet should implement retrieval of records and sending them to the channel
func (*ArrowFlight) DoGet(ctx context.Context, table *handler.Table, r chan<- arrow.Record) error {
	return nil
}

// DoPut should implement the insertion of a record
func (*ArrowFlight) DoPut(ctx context.Context, r arrow.Record) error {
	return nil
}

// LookupTable should implement the retrieval of a table
func (*ArrowFlight) LookupTable(ctx context.Context, tableName string) (*handler.Table, error) {
	return &handler.Table{
		Name: tableName,
	}, nil
}
