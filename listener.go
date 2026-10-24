// Copyright (c) 2026 Contributors to the Eclipse Foundation
//
// See the NOTICE file(s) distributed with this work for additional
// information regarding copyright ownership.
//
// This program and the accompanying materials are made available under the
// terms of the Apache Software License 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0, or the MIT license
// which is available at https://opensource.org/licenses/MIT.
//
// SPDX-License-Identifier: Apache-2.0 OR MIT

package iox2

// #include "iox2/iceoryx2.h"
//
// // The callback is the exported Go function below. Its declaration has to
// // match the one cgo generates, which has no const.
// extern void iox2GoListenerCallback(iox2_event_id_t*, uint64_t, void*);
//
// static iox2_listener_wait_all_callback iox2_go_listener_callback(void) {
//     return (iox2_listener_wait_all_callback)iox2GoListenerCallback;
// }
import "C"

import (
	"runtime/cgo"
	"sync"
	"time"
	"unsafe"
)

// Event is a notification that a [Listener] received. Notifications with the
// same id are reported as one Event with the number of times they were sent.
type Event struct {
	ID    EventID
	Count uint64
}

// ListenerBuilder collects the settings of a [Listener]. It holds no native
// resources, so it needs no cleanup. Get one from [EventService.ListenerBuilder].
type ListenerBuilder struct {
	service *EventService
}

// ListenerBuilder starts building a [Listener] for the service.
func (s *EventService) ListenerBuilder() *ListenerBuilder {
	return &ListenerBuilder{service: s}
}

// Create creates the [Listener]. The caller owns the result and has to call
// [Listener.Close] on it.
func (b *ListenerBuilder) Create() (*Listener, error) {
	s := b.service
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.handle == nil {
		return nil, ErrClosed
	}

	builder := C.iox2_port_factory_event_listener_builder(&s.handle, nil)
	var handle C.iox2_listener_h
	if rc := C.iox2_port_factory_listener_builder_create(builder, nil, &handle); rc != C.IOX2_OK {
		return nil, ListenerCreateError(rc)
	}
	return &Listener{handle: handle}, nil
}

// Listener receives the events that [Notifier] send to an event service. It has
// to be released with [Listener.Close]. A Listener is safe for concurrent use,
// but only one goroutine at a time makes progress in it: while a wait is
// pending, all other methods, including Close, wait for it.
type Listener struct {
	mu     sync.Mutex
	handle C.iox2_listener_h // nil once closed
}

// TryWait returns the events that arrived since the last wait. It returns
// immediately and the result is empty if there are none.
func (l *Listener) TryWait() ([]Event, error) {
	return l.wait(func(handle *C.iox2_listener_h, count *C.uint64_t, ctx C.iox2_callback_context) C.int {
		return C.iox2_listener_try_wait(handle, count, C.iox2_go_listener_callback(), ctx)
	})
}

// TimedWait blocks until at least one event arrived or the timeout elapsed and
// returns the events. The result is empty if the timeout elapsed first. A
// timeout shorter than a microsecond behaves like [Listener.TryWait].
func (l *Listener) TimedWait(timeout time.Duration) ([]Event, error) {
	// The unix datagram socket that carries the events of an ipc service takes
	// the timeout in microseconds, and a timeout of zero microseconds means
	// waiting forever instead of not at all.
	if timeout < time.Microsecond {
		return l.TryWait()
	}
	seconds := C.uint64_t(timeout / time.Second)
	nanoseconds := C.uint32_t(timeout % time.Second)
	return l.wait(func(handle *C.iox2_listener_h, count *C.uint64_t, ctx C.iox2_callback_context) C.int {
		return C.iox2_listener_timed_wait(handle, count, C.iox2_go_listener_callback(), ctx, seconds, nanoseconds)
	})
}

// BlockingWait blocks until at least one event arrived and returns the events.
// It cannot be canceled and keeps [Listener.Close] waiting, so use a loop of
// [Listener.TimedWait] if the wait has to end on a condition.
func (l *Listener) BlockingWait() ([]Event, error) {
	return l.wait(func(handle *C.iox2_listener_h, count *C.uint64_t, ctx C.iox2_callback_context) C.int {
		return C.iox2_listener_blocking_wait(handle, count, C.iox2_go_listener_callback(), ctx)
	})
}

func (l *Listener) wait(
	call func(handle *C.iox2_listener_h, count *C.uint64_t, ctx C.iox2_callback_context) C.int,
) ([]Event, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.handle == nil {
		return nil, ErrClosed
	}

	// The C API reports every event through a callback. The slice cannot be
	// passed to C, so a handle to it travels as the context. The pointer to the
	// handle value, which holds no Go pointer, may be passed to C.
	var events []Event
	handle := cgo.NewHandle(&events)
	defer handle.Delete()

	var count C.uint64_t
	if rc := call(&l.handle, &count, C.iox2_callback_context(unsafe.Pointer(&handle))); rc != C.IOX2_OK {
		return nil, ListenerWaitError(rc)
	}
	return events, nil
}

//export iox2GoListenerCallback
func iox2GoListenerCallback(id *C.iox2_event_id_t, count C.uint64_t, ctx unsafe.Pointer) {
	events := (*cgo.Handle)(ctx).Value().(*[]Event)
	*events = append(*events, Event{ID: EventID(id.value), Count: uint64(count)})
}

// Close releases the listener. Calling Close more than once is safe and has no
// further effect.
func (l *Listener) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.handle != nil {
		C.iox2_listener_drop(l.handle)
		l.handle = nil
	}
}
