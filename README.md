# Apache ArrowFlight Server Reference Implementation

This is a reference implementation of an Apache ArrowFlight server to be used together with  [cq-destination-arrowflight](https://github.com/spangenberg/cq-destination-arrowflight).

It is a simple server that can be used to test and develop other ArrowFlight clients and various server handlers.

## Running

To run the server, simply run the following command:

```shell
$ go run . 
11:46PM INF Serving flight server addr=localhost:9090
```

## Configuration

The server can be configured using the following flags:

- `-auth-token`: The auth token to use for the server. Default is `foobar`.
- `-host`: The host to bind the server to. Default is `localhost`.
- `-port`: The port to bind the server to. Default is `9090`.

## Handlers

The server has the following handlers:

### `NoOpHandler`

This handler does nothing. It is useful for testing the server and to use as a base for creating new handlers.
The handler can be found in the `internal/noophandler` package.
