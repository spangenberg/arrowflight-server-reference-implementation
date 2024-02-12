package internal

import (
	"errors"
	"io"
	"math/rand"

	"github.com/apache/arrow/go/v15/arrow/flight"
	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

type serverAuth struct {
	AuthToken   string
	logger      zerolog.Logger
	validTokens []string
}

func (a *serverAuth) Authenticate(c flight.AuthConn) error {
	in, err := c.Read()
	if errors.Is(err, io.EOF) {
		return status.Error(codes.Unauthenticated, "no auth info provided")
	}
	if err != nil {
		return status.Error(codes.FailedPrecondition, "error reading auth handshake")
	}
	if string(in) != a.AuthToken {
		return status.Error(codes.PermissionDenied, "invalid auth token")
	}
	a.logger.Info().Str("token", string(in)).Msg("Authenticated")
	return c.Send(a.generateToken())
}

func (a *serverAuth) IsValid(token string) (interface{}, error) {
	for _, t := range a.validTokens {
		if token == t {
			a.logger.Info().Str("token", token).Msg("IsValid")
			return nil, nil
		}
	}
	return nil, status.Error(codes.PermissionDenied, "invalid auth token")
}

// Note: This is not a secure way to generate tokens and should not be used in production!
func (a *serverAuth) generateToken() []byte {
	var token []byte
	for i := 0; i < 32; i++ {
		randNum := rand.Intn(len(charset))
		token = append(token, charset[randNum])
	}
	a.validTokens = append(a.validTokens, string(token))
	return token
}
