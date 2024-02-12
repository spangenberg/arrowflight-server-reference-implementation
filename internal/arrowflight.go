package internal

import (
	"context"
	"fmt"
	"os"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/flight"
	"github.com/apache/arrow/go/v15/arrow/ipc"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/google/uuid"
	expiremap "github.com/nursik/go-expire-map"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/spangenberg/arrowflight-server-reference-implementation/handler"
)

const (
	deleteStale  = "DeleteStale"
	deleteRecord = "DeleteRecord"
	migrateTable = "MigrateTable"
)

var (
	kb = 1024
	mb = 1024 * kb
)

type flightServer struct {
	expireMap *expiremap.ExpireMap
	handler   handler.Handler
	logger    zerolog.Logger
	mem       memory.Allocator

	flight.BaseFlightServer
}

func Serve(logger zerolog.Logger, addr string, authToken string, handler handler.Handler) error {
	logger.Info().Str("addr", addr).Msg("Serving flight server")

	var expireMap *expiremap.ExpireMap
	{
		var closer func()
		expireMap = expiremap.New()
		closer = expireMap.Close
		defer closer()
	}

	svc := &flightServer{
		expireMap: expireMap,
		handler:   handler,
		logger:    logger.With().Str("module", "flightServer").Logger(),
		mem:       memory.DefaultAllocator,
	}
	svc.SetAuthHandler(&serverAuth{
		AuthToken: authToken,
		logger:    logger.With().Str("module", "flightServerAuth").Logger(),
	})
	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(20 * mb), // 20MB max message size for flight server (default is 4MB) to handle large tables
	}
	server := flight.NewServerWithMiddleware(nil, opts...)
	server.RegisterFlightService(svc)
	if err := server.Init(addr); err != nil {
		return fmt.Errorf("failed to initialize flight server: %w", err)
	}
	server.SetShutdownOnSignals(os.Interrupt, os.Kill)
	if err := server.Serve(); err != nil {
		return fmt.Errorf("failed to serve flight server: %w", err)
	}
	return nil
}

func (s *flightServer) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	s.logger.Info().Str("action", action.Type).Msg("DoAction")
	switch action.Type {
	case deleteStale:
		if err := s.deleteStale(stream.Context(), action.Body); err != nil {
			return fmt.Errorf("failed to delete stale: %w", err)
		}
	case deleteRecord:
		if err := s.deleteRecord(stream.Context(), action.Body); err != nil {
			return fmt.Errorf("failed to delete record: %w", err)
		}
	case migrateTable:
		if err := s.migrateTable(stream.Context(), action.Body); err != nil {
			return fmt.Errorf("failed to migrate table: %w", err)
		}
	default:
		return fmt.Errorf("unknown action: %s", action.Type)
	}
	if err := stream.Send(&flight.Result{Body: []byte("success")}); err != nil {
		return fmt.Errorf("failed to send: %w", err)
	}
	return nil
}

func (s *flightServer) DoGet(ticket *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	logger := s.logger.With().Str("action", "DoGet").Str("ticket", string(ticket.GetTicket())).Logger()
	ctx := stream.Context()
	uid := &uuid.UUID{}
	if err := uid.UnmarshalBinary(ticket.GetTicket()); err != nil {
		return fmt.Errorf("invalid ticket: %w", err)
	}
	tableNameAny, ok := s.expireMap.Get(uid.String())
	if !ok {
		return fmt.Errorf("invalid ticket")
	}
	tableName, ok := tableNameAny.(string)
	if !ok {
		return fmt.Errorf("invalid ticket")
	}
	table, err := s.handler.LookupTable(ctx, tableName)
	if err != nil {
		return fmt.Errorf("failed to get arrow schema for table: %w", err)
	}
	writer := flight.NewRecordWriter(stream,
		ipc.WithSchema(table.Schema))
	defer func(writer *flight.Writer) {
		if err = writer.Close(); err != nil {
			logger.Warn().Err(err).Msg("failed to close writer")
		}
	}(writer)
	writer.SetFlightDescriptor(&flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{"cloudquery", "arrowflight", table.Name},
	})
	ch := make(chan arrow.Record)
	done := make(chan struct{})
	go func() {
		for rec := range ch {
			if err = writer.Write(rec); err != nil {
				logger.Error().Err(err).Msg("failed to write during DoGet")
			}
		}
		close(done)
	}()
	if err = s.handler.DoGet(ctx, table, ch); err != nil {
		return fmt.Errorf("failed to read: %w", err)
	}
	close(ch)
	<-done
	return nil
}

func (s *flightServer) DoPut(stream flight.FlightService_DoPutServer) error {
	logger := s.logger.With().Str("action", "DoPut").Logger()
	var rdr *flight.Reader
	{
		var err error
		if rdr, err = flight.NewRecordReader(stream, ipc.WithAllocator(s.mem), ipc.WithDelayReadSchema(true)); err != nil {
			logger.Error().Err(err).Msg("failed to create reader")
			return status.Errorf(codes.InvalidArgument, "failed to read input stream: %s", err.Error())
		}
	}
	ch := make(chan flight.StreamChunk)
	go flight.StreamChunksFromReader(rdr, ch)
loop:
	for {
		select {
		case chunk, ok := <-ch:
			if !ok {
				break loop
			}
			if chunk.Err != nil {
				if status.Code(chunk.Err) == codes.Unavailable || status.Code(chunk.Err) == codes.Canceled {
					return nil
				}
				logger.Error().Err(chunk.Err).Msg("failed to handle chunk")
			}
			if err := s.handler.DoPut(stream.Context(), chunk.Data); err != nil {
				logger.Error().Err(err).Msg("failed to do put")
			}
			if chunk.AppMetadata != nil {
				if err := stream.Send(&flight.PutResult{AppMetadata: chunk.AppMetadata}); err != nil {
					if status.Code(err) != codes.Unavailable {
						logger.Error().Err(err).Msg("failed to send")
					}
				}
			}
			if chunk.Data != nil {
				chunk.Data.Release()
			}
		}
	}
	return nil
}

func (s *flightServer) GetFlightInfo(ctx context.Context, flightDescriptor *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if len(flightDescriptor.Path) >= 3 {
		return nil, fmt.Errorf("invalid path")
	}
	tableName := flightDescriptor.Path[2] // cloudquery/arrowflight/{tableName}
	table, err := s.handler.LookupTable(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get arrow schema for table: %w", err)
	}
	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(table.Schema, s.mem),
		FlightDescriptor: flightDescriptor,
		Endpoint:         s.endpointInfo(table.Name),
		TotalRecords:     -1,
		TotalBytes:       -1,
		Ordered:          true,
		AppMetadata:      nil,
	}, nil
}
