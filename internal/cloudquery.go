package internal

import (
	"context"
	"fmt"

	pb "github.com/cloudquery/plugin-pb-go/pb/plugin/v3"
	"github.com/cloudquery/plugin-sdk/v4/message"
	"github.com/cloudquery/plugin-sdk/v4/schema"
	"google.golang.org/protobuf/proto"
)

func (s *flightServer) migrateTable(ctx context.Context, data []byte) error {
	var m pb.Write_MessageMigrateTable
	if err := proto.Unmarshal(data, &m); err != nil {
		return fmt.Errorf("failed to unmarshal body: %w", err)
	}
	arrowSchema, err := pb.NewSchemaFromBytes(m.GetTable())
	if err != nil {
		return fmt.Errorf("failed to create arrow schema: %w", err)
	}
	var table *schema.Table
	if table, err = schema.NewTableFromArrowSchema(arrowSchema); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}
	s.logger.Debug().Str("table", table.Name).Bool("forceMigrate", m.GetMigrateForce()).Msg("migrate table")
	if err = s.handler.MigrateTable(ctx, message.WriteMigrateTables{
		{
			Table:        table,
			MigrateForce: m.GetMigrateForce(),
		},
	}); err != nil {
		return fmt.Errorf("failed to migrate table: %w", err)
	}
	return nil
}

func (s *flightServer) deleteRecord(ctx context.Context, data []byte) error {
	var m pb.Write_MessageDeleteRecord
	if err := proto.Unmarshal(data, &m); err != nil {
		return fmt.Errorf("failed to unmarshal body: %w", err)
	}
	var tableRelations message.TableRelations
	for _, tr := range m.GetTableRelations() {
		tableRelations = append(tableRelations, message.TableRelation{
			TableName:   tr.GetTableName(),
			ParentTable: tr.GetParentTable(),
		})
	}
	var predicateGroups message.PredicateGroups
	for _, predicateGroup := range m.GetWhereClause() {
		var predicates message.Predicates
		for _, p := range predicateGroup.GetPredicates() {
			record, err := pb.NewRecordFromBytes(p.GetRecord())
			if err != nil {
				return fmt.Errorf("failed to create record: %w", err)
			}
			predicates = append(predicates, message.Predicate{
				Operator: p.GetOperator().String(),
				Column:   p.GetColumn(),
				Record:   record,
			})
		}
		predicateGroups = append(predicateGroups, message.PredicateGroup{
			GroupingType: predicateGroup.GetGroupingType().String(),
			Predicates:   predicates,
		})
	}
	if err := s.handler.DeleteRecord(ctx, message.WriteDeleteRecords{
		{
			DeleteRecord: message.DeleteRecord{
				TableName:      m.GetTableName(),
				TableRelations: tableRelations,
				WhereClause:    predicateGroups,
			},
		},
	}); err != nil {
		return fmt.Errorf("failed to delete record: %w", err)
	}
	return nil
}

func (s *flightServer) deleteStale(ctx context.Context, data []byte) error {
	var m pb.Write_MessageDeleteStale
	if err := proto.Unmarshal(data, &m); err != nil {
		return fmt.Errorf("failed to unmarshal body: %w", err)
	}
	s.logger.Debug().Str("sourceName", m.GetSourceName()).Time("syncTime", m.GetSyncTime().AsTime()).Str("tableName", m.GetTableName()).Msg("migrate table")
	if err := s.handler.DeleteStale(ctx, message.WriteDeleteStales{
		{
			TableName:  m.GetTableName(),
			SourceName: m.GetSourceName(),
			SyncTime:   m.GetSyncTime().AsTime(),
		},
	}); err != nil {
		return fmt.Errorf("failed to delete stale: %w", err)
	}
	return nil
}
