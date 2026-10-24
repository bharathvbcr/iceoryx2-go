# iceoryx2-go

Go Language Binding for Eclipse iceoryx2™

The package `iox2` wraps the iceoryx2 C API with cgo. Progress is tracked in
[eclipse-iceoryx/iceoryx2#2104](https://github.com/eclipse-iceoryx/iceoryx2/issues/2104).

> **Status:** early development. Creating a `Node`, publish-subscribe
> communication of bytes or fixed-size values and event notification are
> available so far.

## Requirements

* Go 1.26 or newer, with cgo enabled and a C compiler
* A stable Rust toolchain and, on Linux, `libclang-dev` and `clang`, to build
  the iceoryx2 C library
* Linux (amd64, arm64) or macOS. Windows is planned.

## Build and Test

The iceoryx2 repository is a git submodule, pinned to a release.

```sh
git clone --recurse-submodules https://github.com/eclipse-iceoryx/iceoryx2-go
cd iceoryx2-go
./build.sh
```

`build.sh` builds the static C library with
`cargo build --release --package iceoryx2-ffi-c` and then builds and tests the
binding. Afterwards, use the Go tool chain directly:

```sh
source scripts/cgo-env.sh
go test -race ./...
```

`scripts/cgo-env.sh` sets `CGO_CFLAGS` and `CGO_LDFLAGS`; in your own project
set them to the include directory of `iox2/iceoryx2.h` and to the path of
`libiceoryx2_ffi_c.a` in the same way.

## Example

```go
package main

import (
	"log"

	iox2 "github.com/eclipse-iceoryx/iceoryx2-go"
)

// Reading has the memory layout of a C struct with the same fields.
type Reading struct {
	ID    uint32
	Value float64
}

func main() {
	node, err := iox2.NewNodeBuilder().Name("my-node").Create(iox2.ServiceTypeIpc)
	if err != nil {
		log.Fatal(err)
	}
	defer node.Close()

	service, err := node.PublishSubscribe("Sensors/Reading").
		Payload(iox2.FixedSizePayload[Reading]("Reading")).
		OpenOrCreate()
	if err != nil {
		log.Fatal(err)
	}
	defer service.Close()

	publisher, err := service.PublisherBuilder().Create()
	if err != nil {
		log.Fatal(err)
	}
	defer publisher.Close()
	subscriber, err := service.SubscriberBuilder().Create()
	if err != nil {
		log.Fatal(err)
	}
	defer subscriber.Close()

	if _, err := iox2.SendValue(publisher, Reading{ID: 1, Value: 2.5}); err != nil {
		log.Fatal(err)
	}
	if reading, ok, err := iox2.ReceiveValue[Reading](subscriber); err != nil {
		log.Fatal(err)
	} else if ok {
		log.Printf("received %+v", reading)
	}
}
```

A service without `Payload` transmits byte slices of any length with
`Publisher.Send` and `Subscriber.Receive`.

### Events

An event service wakes up other processes or goroutines without transmitting
data. A `Notifier` sends event ids, a `Listener` waits for them:

```go
service, err := node.Event("Sensors/Reading/Ready").OpenOrCreate()
if err != nil {
	log.Fatal(err)
}
defer service.Close()

notifier, err := service.NotifierBuilder().DefaultEventID(1).Create()
if err != nil {
	log.Fatal(err)
}
defer notifier.Close()
listener, err := service.ListenerBuilder().Create()
if err != nil {
	log.Fatal(err)
}
defer listener.Close()

if _, err := notifier.Notify(); err != nil {
	log.Fatal(err)
}
events, err := listener.TimedWait(time.Second)
if err != nil {
	log.Fatal(err)
}
for _, event := range events {
	log.Printf("event %d arrived %d time(s)", event.ID, event.Count)
}
```

`Listener.TryWait` never blocks and `Listener.BlockingWait` waits without a
time limit. While a wait is pending, `Listener.Close` waits for it, so prefer a
loop of `TimedWait` calls where the wait has to end on a condition.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Security issues are handled as
described in [SECURITY.md](SECURITY.md).

## License

Apache-2.0 OR MIT, see [LICENSE-APACHE](LICENSE-APACHE),
[LICENSE-MIT](LICENSE-MIT) and [NOTICE.md](NOTICE.md).
