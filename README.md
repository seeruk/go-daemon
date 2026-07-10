# go-daemon

A library for building graceful, long-running daemons from composable background routines with Go.

For "daemon"-style applications, like web or RPC servers, you'll often want to have one or more
background routines. You'll also want them to do things like shutdown gracefully when requested, or
have your application quit immediately on a second signal, or to have control over the order in
which these background routines start. This library aims to tackle all of these requirements with a 
simple and straightforward API. 

My philosophy on a `main` function is that it should focus on orchestrating the lifecycle of an
application. As a result, it should be pretty lightweight! This library helps keep your `main` clean
and ensure that what's really happening is clear.

## Installation

```
go get github.com/seeruk/go-daemon@latest
```

## Basic Usage

There are a couple of entry-points for `go-daemon`:

1. `daemon.Run(gracePeriod time.Duration, routines ...Routine) int`: Returns an int expected to be
   passed to `os.Exit`. If there's an error, it'll return 1, otherwise 0.
2. `daemon.RunE(gracePeriod time.Duration, routines ...Routine) error`: Similar to `Run`, but
   returns the actual error, so you can respond to it however you want (e.g. logging).

Some convenience wrappers and commonly used routine types are included as part of `go-daemon`. Once
you have your threads, you can use `go-daemon` like this:

```go
package main

import (
	"os"
	"time"
	
	"example.com/app/internal"
	"github.com/seeruk/go-daemon"
)

func main() {
	// Imagine you have another type which does your dependency wiring:
	container := internal.NewContainer(internal.ReadConfig())
	
	// Routines are started in order, so if `InMemoryAcmeStoreRoutine` was an 
	// `InitializableRoutine`, it would be initialized before starting the gRPC and HTTP servers
	os.Exit(daemon.Run(5 * time.Second,
		container.InMemoryAcmeStoreRoutine(),
		container.GRPCServerRoutine(),
		container.HTTPServerRoutine(),
    ))
}
```

See the contents of [routine.go](routine.go) to see the interfaces that this library provides, and
to see the available convenience types.

There's also an example in [example/main.go](example/main.go), showcasing the `httpsrv` subpackage,
which provides a convenient way to run HTTP servers as daemons with graceful shutdown support.

## License

MIT
