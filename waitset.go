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
// // match the one cgo generates.
// extern enum iox2_callback_progression_e iox2GoWaitSetCallback(iox2_waitset_attachment_id_h, void*);
//
// static iox2_waitset_run_callback iox2_go_waitset_callback(void) {
//     return (iox2_waitset_run_callback)iox2GoWaitSetCallback;
// }
import "C"

import (
	"fmt"
	"runtime/cgo"
	"sync"
	"time"
	"unsafe"
)

// SignalHandlingMode selects whether a [WaitSet] registers signal handlers.
type SignalHandlingMode int

const (
	// SignalHandlingDisabled registers no signal handler. It is the default,
	// because the Go runtime owns the signal handlers of the process, see
	// [WaitSetBuilder.SignalHandlingMode].
	SignalHandlingDisabled SignalHandlingMode = iota
	// SignalHandlingTerminationRequests lets iceoryx2 register handlers for
	// SIGINT and SIGTERM and makes the wait functions of the [WaitSet] return
	// [RunResultInterrupt] or [RunResultTerminationRequest] when one of them
	// arrives. iceoryx2 keeps only the last signal and reports it once, so a
	// request can go unnoticed if a wait ends for another reason at the same
	// time.
	SignalHandlingTerminationRequests
)

func (m SignalHandlingMode) String() string {
	switch m {
	case SignalHandlingDisabled:
		return "disabled"
	case SignalHandlingTerminationRequests:
		return "termination requests"
	default:
		return "invalid"
	}
}

func (m SignalHandlingMode) toC() (C.enum_iox2_signal_handling_mode_e, bool) {
	switch m {
	case SignalHandlingDisabled:
		return C.iox2_signal_handling_mode_e_DISABLED, true
	case SignalHandlingTerminationRequests:
		return C.iox2_signal_handling_mode_e_HANDLE_TERMINATION_REQUESTS, true
	default:
		return 0, false
	}
}

// Progression is what the handler of a [WaitSet] answers for every
// notification it handled.
type Progression int

const (
	// ProgressionStop ends the wait call at once. Notifications that were not
	// handled yet are lost.
	ProgressionStop Progression = iota
	// ProgressionContinue handles the next notification.
	ProgressionContinue
)

// RunResult tells why a wait call of a [WaitSet] returned.
type RunResult int

// The reasons for which a wait call returns without an error.
const (
	// RunResultTerminationRequest means SIGINT or SIGTERM was received. It can
	// only happen with [SignalHandlingTerminationRequests].
	RunResultTerminationRequest RunResult = C.iox2_waitset_run_result_e_TERMINATION_REQUEST
	// RunResultInterrupt means a signal interrupted the wait. It can only
	// happen with [SignalHandlingTerminationRequests], otherwise the wait
	// functions call iceoryx2 again, because the Go runtime and the process
	// receive signals that are no reason to stop.
	RunResultInterrupt RunResult = C.iox2_waitset_run_result_e_INTERRUPT
	// RunResultStopRequest means the handler answered [ProgressionStop].
	RunResultStopRequest RunResult = C.iox2_waitset_run_result_e_STOP_REQUEST
	// RunResultAllEventsHandled means the handler saw every notification, or
	// the timeout of the call elapsed.
	RunResultAllEventsHandled RunResult = C.iox2_waitset_run_result_e_ALL_EVENTS_HANDLED
)

func (r RunResult) String() string {
	switch r {
	case RunResultTerminationRequest:
		return "termination request"
	case RunResultInterrupt:
		return "interrupt"
	case RunResultStopRequest:
		return "stop request"
	case RunResultAllEventsHandled:
		return "all events handled"
	default:
		return fmt.Sprintf("invalid run result %d", int(r))
	}
}

// WaitSetBuilder collects the settings of a [WaitSet]. It holds no native
// resources, so it needs no cleanup and can be reused to create several wait
// sets.
type WaitSetBuilder struct {
	signalHandling SignalHandlingMode
}

// NewWaitSetBuilder returns a [WaitSetBuilder] with the default settings, which
// disable signal handling.
func NewWaitSetBuilder() *WaitSetBuilder {
	return &WaitSetBuilder{}
}

// SignalHandlingMode sets whether iceoryx2 registers handlers for SIGINT and
// SIGTERM. The default is [SignalHandlingDisabled].
//
// The Go runtime installs its own handlers for these signals, which it needs
// for [os/signal] and for stopping the program, and it expects the handlers of
// the process to run on the signal stack. The handlers of iceoryx2 replace the
// ones of the runtime, so enable this only in a program that does not use
// [os/signal] and has not set up signal handling itself. Otherwise call the wait
// functions in a loop and stop them with a notification, see
// [WaitSet.AttachNotification].
func (b *WaitSetBuilder) SignalHandlingMode(mode SignalHandlingMode) *WaitSetBuilder {
	b.signalHandling = mode
	return b
}

// Create creates the [WaitSet]. The caller owns the result and has to call
// [WaitSet.Close] on it.
func (b *WaitSetBuilder) Create(serviceType ServiceType) (*WaitSet, error) {
	cServiceType, ok := serviceType.toC()
	if !ok {
		return nil, ErrInvalidServiceType
	}
	cMode, ok := b.signalHandling.toC()
	if !ok {
		return nil, ErrInvalidSignalHandlingMode
	}

	var builder C.iox2_waitset_builder_h
	C.iox2_waitset_builder_new(nil, &builder)
	C.iox2_waitset_builder_set_signal_handling_mode(&builder, cMode)

	// The call consumes the builder, also if it fails.
	var handle C.iox2_waitset_h
	if rc := C.iox2_waitset_builder_create(builder, cServiceType, nil, &handle); rc != C.IOX2_OK {
		return nil, WaitSetCreateError(rc)
	}
	return &WaitSet{
		signalHandling: b.signalHandling,
		handle:         handle,
		guards:         make(map[*Guard]struct{}),
	}, nil
}

// WaitSet waits for several sources at once: the [Listener] of event services
// and timers. Sources are attached with [WaitSet.AttachNotification],
// [WaitSet.AttachDeadline] and [WaitSet.AttachInterval], which return a [Guard]
// that detaches the source again. The wait functions call a handler for every
// source that is ready.
//
// A WaitSet is released with [WaitSet.Close]. It is safe for concurrent use, but
// iceoryx2 runs one operation at a time: while a wait function is pending, all
// other methods wait for it, including Close. A handler must not call methods
// of its WaitSet or of the guards of it.
type WaitSet struct {
	// signalHandling is immutable after creation.
	signalHandling SignalHandlingMode

	mu     sync.Mutex
	handle C.iox2_waitset_h // nil once closed
	guards map[*Guard]struct{}
}

// SignalHandlingMode returns the mode the wait set was created with.
func (w *WaitSet) SignalHandlingMode() SignalHandlingMode {
	return w.signalHandling
}

// Len returns the number of attachments, or 0 after Close.
func (w *WaitSet) Len() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.handle == nil {
		return 0
	}
	return int(C.iox2_waitset_len(&w.handle))
}

// Capacity returns how many attachments the wait set can hold, or 0 after
// Close.
func (w *WaitSet) Capacity() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.handle == nil {
		return 0
	}
	return int(C.iox2_waitset_capacity(&w.handle))
}

// AttachNotification makes the wait set report when the listener received an
// event. The handler has to consume the events with [Listener.TryWait],
// otherwise the wait set reports the listener again at once.
//
// The listener can be closed while it is attached, but its resources stay
// allocated until the guard is closed.
func (w *WaitSet) AttachNotification(listener *Listener) (*Guard, error) {
	return w.attach(listener, func(fd C.iox2_file_descriptor_ptr, guard *C.iox2_waitset_guard_h) C.int {
		return C.iox2_waitset_attach_notification(&w.handle, fd, nil, guard)
	})
}

// AttachDeadline is like [WaitSet.AttachNotification] and in addition reports
// the listener if it receives no event for the duration of the deadline. Use
// [AttachmentID.HasMissedDeadline] to tell the two apart.
func (w *WaitSet) AttachDeadline(listener *Listener, deadline time.Duration) (*Guard, error) {
	if deadline <= 0 {
		return nil, ErrInvalidDuration
	}
	seconds, nanoseconds := C.uint64_t(deadline/time.Second), C.uint32_t(deadline%time.Second)
	return w.attach(listener, func(fd C.iox2_file_descriptor_ptr, guard *C.iox2_waitset_guard_h) C.int {
		return C.iox2_waitset_attach_deadline(&w.handle, fd, seconds, nanoseconds, nil, guard)
	})
}

// AttachInterval makes the wait set report the guard periodically.
func (w *WaitSet) AttachInterval(interval time.Duration) (*Guard, error) {
	if interval <= 0 {
		return nil, ErrInvalidDuration
	}
	seconds, nanoseconds := C.uint64_t(interval/time.Second), C.uint32_t(interval%time.Second)
	return w.attach(nil, func(_ C.iox2_file_descriptor_ptr, guard *C.iox2_waitset_guard_h) C.int {
		return C.iox2_waitset_attach_interval(&w.handle, seconds, nanoseconds, nil, guard)
	})
}

func (w *WaitSet) attach(
	listener *Listener,
	call func(fd C.iox2_file_descriptor_ptr, guard *C.iox2_waitset_guard_h) C.int,
) (*Guard, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.handle == nil {
		return nil, ErrClosed
	}

	var fd C.iox2_file_descriptor_ptr
	if listener != nil {
		var ok bool
		if fd, ok = listener.pin(); !ok {
			return nil, ErrClosed
		}
	}

	var handle C.iox2_waitset_guard_h
	if rc := call(fd, &handle); rc != C.IOX2_OK {
		if listener != nil {
			listener.unpin()
		}
		return nil, WaitSetAttachmentError(rc)
	}
	guard := &Guard{waitset: w, listener: listener, handle: handle}
	w.guards[guard] = struct{}{}
	return guard, nil
}

// Guard is the attachment of a source to a [WaitSet]. Closing it detaches the
// source. A Guard is safe for concurrent use.
type Guard struct {
	waitset  *WaitSet
	listener *Listener // nil for an interval

	mu     sync.Mutex
	handle C.iox2_waitset_guard_h // nil once closed
}

// Close detaches the source. Calling Close more than once is safe and has no
// further effect.
func (g *Guard) Close() {
	g.waitset.mu.Lock()
	defer g.waitset.mu.Unlock()
	g.release()
}

// release expects the mutex of the wait set to be held.
func (g *Guard) release() {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.handle != nil {
		C.iox2_waitset_guard_drop(g.handle)
		g.handle = nil
		delete(g.waitset.guards, g)
		if g.listener != nil {
			g.listener.unpin()
		}
	}
}

// Close releases the wait set and detaches everything that is attached. Calling
// Close more than once is safe and has no further effect.
func (w *WaitSet) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.handle != nil {
		// The guards refer to the wait set, so they have to go first.
		for guard := range w.guards {
			guard.release()
		}
		C.iox2_waitset_drop(w.handle)
		w.handle = nil
	}
}

// AttachmentID names the source that made a [WaitSet] call its handler. It is
// only valid inside the handler.
type AttachmentID struct {
	handle C.iox2_waitset_attachment_id_h // nil once the handler returned
}

// HasEventFrom reports whether the notification comes from the source of the
// guard.
func (a *AttachmentID) HasEventFrom(guard *Guard) bool {
	guard.mu.Lock()
	defer guard.mu.Unlock()
	if a.handle == nil || guard.handle == nil {
		return false
	}
	return bool(C.iox2_waitset_attachment_id_has_event_from(&a.handle, &guard.handle))
}

// HasMissedDeadline reports whether the notification is the missed deadline of
// the guard, see [WaitSet.AttachDeadline].
func (a *AttachmentID) HasMissedDeadline(guard *Guard) bool {
	guard.mu.Lock()
	defer guard.mu.Unlock()
	if a.handle == nil || guard.handle == nil {
		return false
	}
	return bool(C.iox2_waitset_attachment_id_has_missed_deadline(&a.handle, &guard.handle))
}

// WaitAndProcessOnce waits until a source is ready or, if an interval or
// deadline is attached, until it is due, calls the handler for everything that
// is ready and returns. It fails with [WaitSetRunErrorNoAttachments] if nothing
// is attached.
func (w *WaitSet) WaitAndProcessOnce(handler func(*AttachmentID) Progression) (RunResult, error) {
	return w.run(handler, time.Time{}, func(handle *C.iox2_waitset_h, ctx C.iox2_callback_context, result *C.enum_iox2_waitset_run_result_e, _ time.Duration) C.int {
		return C.iox2_waitset_wait_and_process_once(handle, C.iox2_go_waitset_callback(), ctx, result)
	})
}

// WaitAndProcessOnceWithTimeout is like [WaitSet.WaitAndProcessOnce] but returns
// after the timeout at the latest, with [RunResultAllEventsHandled].
func (w *WaitSet) WaitAndProcessOnceWithTimeout(handler func(*AttachmentID) Progression, timeout time.Duration) (RunResult, error) {
	return w.run(handler, time.Now().Add(max(timeout, 0)), func(handle *C.iox2_waitset_h, ctx C.iox2_callback_context, result *C.enum_iox2_waitset_run_result_e, remaining time.Duration) C.int {
		seconds, nanoseconds := C.uint64_t(remaining/time.Second), C.uint32_t(remaining%time.Second)
		return C.iox2_waitset_wait_and_process_once_with_timeout(handle, C.iox2_go_waitset_callback(), ctx, seconds, nanoseconds, result)
	})
}

// WaitAndProcess repeats [WaitSet.WaitAndProcessOnce] until the handler answers
// [ProgressionStop] or a signal ends the wait.
func (w *WaitSet) WaitAndProcess(handler func(*AttachmentID) Progression) (RunResult, error) {
	return w.run(handler, time.Time{}, func(handle *C.iox2_waitset_h, ctx C.iox2_callback_context, result *C.enum_iox2_waitset_run_result_e, _ time.Duration) C.int {
		return C.iox2_waitset_wait_and_process(handle, C.iox2_go_waitset_callback(), ctx, result)
	})
}

// runState is what the C callback gets as its context.
type runState struct {
	handler  func(*AttachmentID) Progression
	panicked bool
	panicVal any
}

// run calls a wait function of the C API. A non-zero deadline is the end of the
// timeout of a call with a timeout.
func (w *WaitSet) run(
	handler func(*AttachmentID) Progression,
	deadline time.Time,
	call func(*C.iox2_waitset_h, C.iox2_callback_context, *C.enum_iox2_waitset_run_result_e, time.Duration) C.int,
) (RunResult, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.handle == nil {
		return 0, ErrClosed
	}

	state := &runState{handler: handler}
	h := cgo.NewHandle(state)
	defer h.Delete()
	ctx := C.iox2_callback_context(unsafe.Pointer(&h))

	// cgo checks the whole WaitSet for Go pointers if the address of a field is
	// not taken in the call itself, so the C function gets the address of a
	// copy of the handle.
	handle := w.handle
	for {
		var result C.enum_iox2_waitset_run_result_e
		rc := call(&handle, ctx, &result, max(time.Until(deadline), 0))
		if state.panicked {
			// A panic must not unwind through the frames of the C library, so
			// the callback stopped the wait and the panic continues here.
			panic(state.panicVal)
		}
		if rc != C.IOX2_OK {
			return 0, WaitSetRunError(rc)
		}
		if result == C.iox2_waitset_run_result_e_INTERRUPT && w.signalHandling == SignalHandlingDisabled {
			// The signal is none of the wait set's business. the Go runtime
			// and the process receive signals that interrupt the system call
			// of the wait, and the wait has to go on.
			if deadline.IsZero() || time.Now().Before(deadline) {
				continue
			}
			return RunResultAllEventsHandled, nil
		}
		return RunResult(result), nil
	}
}

//export iox2GoWaitSetCallback
func iox2GoWaitSetCallback(handle C.iox2_waitset_attachment_id_h, ctx unsafe.Pointer) (progression C.enum_iox2_callback_progression_e) {
	state := (*cgo.Handle)(ctx).Value().(*runState)
	id := &AttachmentID{handle: handle}
	defer func() {
		// The C API leaves the release of the id to the callback.
		id.handle = nil
		C.iox2_waitset_attachment_id_drop(handle)
		if r := recover(); r != nil {
			state.panicked, state.panicVal = true, r
			progression = C.iox2_callback_progression_e_STOP
		}
	}()

	if state.handler(id) == ProgressionContinue {
		return C.iox2_callback_progression_e_CONTINUE
	}
	return C.iox2_callback_progression_e_STOP
}
