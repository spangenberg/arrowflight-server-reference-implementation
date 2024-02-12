package handler

import (
	"context"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/cloudquery/plugin-sdk/v4/message"
)

type Table struct {
	Name   string
	Schema *arrow.Schema
}

type ArrowFlightHandler interface {
	DoGet(ctx context.Context, table *Table, r chan<- arrow.Record) error
	DoPut(ctx context.Context, r arrow.Record) error
	LookupTable(ctx context.Context, tableName string) (*Table, error)
}

type CloudQueryHandler interface {
	MigrateTable(ctx context.Context, msg message.WriteMigrateTables) error
	DeleteRecord(ctx context.Context, msg message.WriteDeleteRecords) error
	DeleteStale(ctx context.Context, msg message.WriteDeleteStales) error
}

type Handler struct {
	CloudQueryHandler
	ArrowFlightHandler
}
