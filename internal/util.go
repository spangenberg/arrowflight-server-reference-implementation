package internal

import (
	"time"

	"github.com/apache/arrow/go/v15/arrow/flight"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var ttl = time.Minute * 5

func (s *flightServer) endpointInfo(tableName string) []*flight.FlightEndpoint {
	ticket, expirationTime := s.issueTicket(tableName)
	return []*flight.FlightEndpoint{
		{
			Ticket:         ticket,
			Location:       nil,
			ExpirationTime: timestamppb.New(expirationTime),
			AppMetadata:    nil,
		},
	}
}

func (s *flightServer) issueTicket(tableName string) (*flight.Ticket, time.Time) {
	uid := uuid.New()
	data, _ := uid.MarshalBinary()
	expirationTime := time.Now().Add(ttl)
	s.expireMap.Set(uid.String(), tableName, ttl)
	return &flight.Ticket{
		Ticket: data,
	}, expirationTime
}
