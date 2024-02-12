package main

import (
	"flag"
	"net"
	"os"
	"strconv"

	"github.com/rs/zerolog"

	"github.com/spangenberg/arrowflight-server-reference-implementation/handler"
	"github.com/spangenberg/arrowflight-server-reference-implementation/internal"
	"github.com/spangenberg/arrowflight-server-reference-implementation/internal/noophandler"
)

func main() {
	var (
		authToken = flag.String("auth-token", "foobar", "auth token to use for the server")
		host      = flag.String("host", "localhost", "hostname to bind to")
		port      = flag.Int("port", 9090, "port to bind to")
	)

	flag.Parse()

	var logger zerolog.Logger
	{
		var closer func()
		out := os.Stdout
		logger = zerolog.New(zerolog.ConsoleWriter{Out: out}).With().Timestamp().Logger()
		closer = func() {
			if err := out.Close(); err != nil {
				logger.Error().Err(err).Msg("failed to close stdout")
			}
		}
		defer closer()
	}

	// Create a new handler with no-op implementations.
	// Swap these out with your own implementations.
	myHandler := handler.Handler{
		CloudQueryHandler:  &noophandler.CloudQuery{},
		ArrowFlightHandler: &noophandler.ArrowFlight{},
	}

	if err := internal.Serve(logger, net.JoinHostPort(*host, strconv.Itoa(*port)), *authToken, myHandler); err != nil {
		logger.Fatal().Err(err).Msg("failed to start flight server")
	}
}
