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

package iox2_test

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	iox2 "github.com/eclipse-iceoryx/iceoryx2-go"
)

func newWaitSet(t *testing.T, serviceType iox2.ServiceType) *iox2.WaitSet {
	t.Helper()
	waitSet, err := iox2.NewWaitSetBuilder().Create(serviceType)
	if err != nil {
		t.Fatalf("Create wait set: %v", err)
	}
	t.Cleanup(waitSet.Close)
	return waitSet
}

// newListener creates a listener and a notifier of an event service that is
// named after the test and the suffix.
func newListener(t *testing.T, node *iox2.Node, suffix string) (*iox2.Listener, *iox2.Notifier) {
	t.Helper()
	service, err := node.Event(serviceName(t) + suffix).OpenOrCreate()
	if err != nil {
		t.Fatalf("OpenOrCreate: %v", err)
	}
	t.Cleanup(service.Close)
	notifier, err := service.NotifierBuilder().Create()
	if err != nil {
		t.Fatalf("create notifier: %v", err)
	}
	t.Cleanup(notifier.Close)
	listener, err := service.ListenerBuilder().Create()
	if err != nil {
		t.Fatalf("create listener: %v", err)
	}
	t.Cleanup(listener.Close)
	return listener, notifier
}

func attach(t *testing.T, waitSet *iox2.WaitSet, listener *iox2.Listener) *iox2.Guard {
	t.Helper()
	guard, err := waitSet.AttachNotification(listener)
	if err != nil {
		t.Fatalf("AttachNotification: %v", err)
	}
	t.Cleanup(guard.Close)
	return guard
}

func TestWaitSetReportsNotification(t *testing.T) {
	for _, serviceType := range serviceTypes {
		t.Run(serviceType.String(), func(t *testing.T) {
			node := newNode(t, serviceType)
			waitSet := newWaitSet(t, serviceType)
			first, firstNotifier := newListener(t, node, "/first")
			second, _ := newListener(t, node, "/second")
			firstGuard, secondGuard := attach(t, waitSet, first), attach(t, waitSet, second)

			if waitSet.Len() != 2 || waitSet.Capacity() < 2 {
				t.Errorf("Len, Capacity = %d, %d, want 2 attachments", waitSet.Len(), waitSet.Capacity())
			}

			if _, err := firstNotifier.NotifyWithEventID(4); err != nil {
				t.Fatalf("Notify: %v", err)
			}
			calls := 0
			result, err := waitSet.WaitAndProcessOnce(func(id *iox2.AttachmentID) iox2.Progression {
				calls++
				if !id.HasEventFrom(firstGuard) || id.HasEventFrom(secondGuard) {
					t.Errorf("HasEventFrom = %v, %v, want the first listener only", id.HasEventFrom(firstGuard), id.HasEventFrom(secondGuard))
				}
				if id.HasMissedDeadline(firstGuard) {
					t.Error("HasMissedDeadline on a notification")
				}
				// The wait set reports a listener until its events are consumed.
				if events, err := first.TryWait(); err != nil || len(events) != 1 || events[0].ID != 4 {
					t.Errorf("TryWait = %v, %v, want the event 4", events, err)
				}
				return iox2.ProgressionContinue
			})
			if err != nil || result != iox2.RunResultAllEventsHandled || calls != 1 {
				t.Errorf("WaitAndProcessOnce = %v, %v after %d calls, want %v after 1 call", result, err, calls, iox2.RunResultAllEventsHandled)
			}
		})
	}
}

func TestWaitSetTimeout(t *testing.T) {
	node := newNode(t, iox2.ServiceTypeLocal)
	waitSet := newWaitSet(t, iox2.ServiceTypeLocal)
	listener, _ := newListener(t, node, "")
	attach(t, waitSet, listener)

	start := time.Now()
	result, err := waitSet.WaitAndProcessOnceWithTimeout(func(*iox2.AttachmentID) iox2.Progression {
		t.Error("handler called without a notification")
		return iox2.ProgressionStop
	}, 50*time.Millisecond)
	if err != nil || result != iox2.RunResultAllEventsHandled {
		t.Fatalf("WaitAndProcessOnceWithTimeout = %v, %v, want %v", result, err, iox2.RunResultAllEventsHandled)
	}
	if elapsed := time.Since(start); elapsed < 40*time.Millisecond {
		t.Errorf("returned after %v, want it to wait for the timeout", elapsed)
	}

	for _, timeout := range []time.Duration{0, -time.Second} {
		if _, err := waitSet.WaitAndProcessOnceWithTimeout(func(*iox2.AttachmentID) iox2.Progression { return iox2.ProgressionStop }, timeout); err != nil {
			t.Errorf("timeout %v: %v", timeout, err)
		}
	}
}

func TestWaitSetInterval(t *testing.T) {
	waitSet := newWaitSet(t, iox2.ServiceTypeLocal)
	guard, err := waitSet.AttachInterval(20 * time.Millisecond)
	if err != nil {
		t.Fatalf("AttachInterval: %v", err)
	}
	defer guard.Close()

	ticks := 0
	result, err := waitSet.WaitAndProcess(func(id *iox2.AttachmentID) iox2.Progression {
		if !id.HasEventFrom(guard) {
			t.Error("tick does not come from the interval guard")
		}
		ticks++
		if ticks == 3 {
			return iox2.ProgressionStop
		}
		return iox2.ProgressionContinue
	})
	if err != nil || result != iox2.RunResultStopRequest || ticks != 3 {
		t.Errorf("WaitAndProcess = %v, %v after %d ticks, want %v after 3", result, err, ticks, iox2.RunResultStopRequest)
	}
}

func TestWaitSetDeadline(t *testing.T) {
	node := newNode(t, iox2.ServiceTypeLocal)
	waitSet := newWaitSet(t, iox2.ServiceTypeLocal)
	listener, notifier := newListener(t, node, "")
	guard, err := waitSet.AttachDeadline(listener, 30*time.Millisecond)
	if err != nil {
		t.Fatalf("AttachDeadline: %v", err)
	}
	defer guard.Close()

	// Nothing arrives, so the deadline is missed.
	missed := false
	if _, err := waitSet.WaitAndProcessOnce(func(id *iox2.AttachmentID) iox2.Progression {
		missed = id.HasMissedDeadline(guard)
		return iox2.ProgressionContinue
	}); err != nil || !missed {
		t.Errorf("silent listener: missed deadline = %v, %v, want true", missed, err)
	}

	if _, err := notifier.Notify(); err != nil {
		t.Fatalf("Notify: %v", err)
	}
	missed = true
	if _, err := waitSet.WaitAndProcessOnce(func(id *iox2.AttachmentID) iox2.Progression {
		missed = id.HasMissedDeadline(guard)
		if !id.HasEventFrom(guard) {
			t.Error("notification does not come from the guard")
		}
		if _, err := listener.TryWait(); err != nil {
			t.Errorf("TryWait: %v", err)
		}
		return iox2.ProgressionContinue
	}); err != nil || missed {
		t.Errorf("notified listener: missed deadline = %v, %v, want false", missed, err)
	}
}

func TestWaitSetStopsAtProgressionStop(t *testing.T) {
	node := newNode(t, iox2.ServiceTypeLocal)
	waitSet := newWaitSet(t, iox2.ServiceTypeLocal)
	first, firstNotifier := newListener(t, node, "/first")
	second, secondNotifier := newListener(t, node, "/second")
	attach(t, waitSet, first)
	attach(t, waitSet, second)

	for _, notifier := range []*iox2.Notifier{firstNotifier, secondNotifier} {
		if _, err := notifier.Notify(); err != nil {
			t.Fatalf("Notify: %v", err)
		}
	}
	calls := 0
	result, err := waitSet.WaitAndProcessOnce(func(*iox2.AttachmentID) iox2.Progression {
		calls++
		return iox2.ProgressionStop
	})
	if err != nil || result != iox2.RunResultStopRequest || calls != 1 {
		t.Errorf("WaitAndProcessOnce = %v, %v after %d calls, want %v after 1 call", result, err, calls, iox2.RunResultStopRequest)
	}
}

func TestWaitSetWithoutAttachments(t *testing.T) {
	waitSet := newWaitSet(t, iox2.ServiceTypeLocal)
	handler := func(*iox2.AttachmentID) iox2.Progression { return iox2.ProgressionStop }

	if _, err := waitSet.WaitAndProcessOnce(handler); !errors.Is(err, iox2.WaitSetRunErrorNoAttachments) {
		t.Errorf("WaitAndProcessOnce: error = %v, want %v", err, iox2.WaitSetRunErrorNoAttachments)
	}
	if _, err := waitSet.WaitAndProcess(handler); !errors.Is(err, iox2.WaitSetRunErrorNoAttachments) {
		t.Errorf("WaitAndProcess: error = %v, want %v", err, iox2.WaitSetRunErrorNoAttachments)
	}
}

func TestGuardCloseDetaches(t *testing.T) {
	node := newNode(t, iox2.ServiceTypeLocal)
	waitSet := newWaitSet(t, iox2.ServiceTypeLocal)
	listener, notifier := newListener(t, node, "")
	guard := attach(t, waitSet, listener)

	if _, err := waitSet.AttachNotification(listener); !errors.Is(err, iox2.WaitSetAttachmentErrorAlreadyAttached) {
		t.Errorf("second AttachNotification: error = %v, want %v", err, iox2.WaitSetAttachmentErrorAlreadyAttached)
	}

	for range 2 { // closing twice is safe
		guard.Close()
	}
	if waitSet.Len() != 0 {
		t.Errorf("Len = %d after Close of the guard, want 0", waitSet.Len())
	}
	if _, err := notifier.Notify(); err != nil {
		t.Fatalf("Notify: %v", err)
	}
	if _, err := waitSet.WaitAndProcessOnce(func(*iox2.AttachmentID) iox2.Progression { return iox2.ProgressionStop }); !errors.Is(err, iox2.WaitSetRunErrorNoAttachments) {
		t.Errorf("wait after detaching: error = %v, want %v", err, iox2.WaitSetRunErrorNoAttachments)
	}

	// The listener can be attached again.
	again := attach(t, waitSet, listener)
	if !receivedFrom(t, waitSet, again) {
		t.Error("the listener was not reported after attaching it again")
	}
}

// receivedFrom reports whether a wait call of at most a second is woken up by
// the guard.
func receivedFrom(t *testing.T, waitSet *iox2.WaitSet, guard *iox2.Guard) (reported bool) {
	t.Helper()
	if _, err := waitSet.WaitAndProcessOnceWithTimeout(func(id *iox2.AttachmentID) iox2.Progression {
		reported = id.HasEventFrom(guard)
		return iox2.ProgressionStop
	}, time.Second); err != nil {
		t.Fatalf("wait: %v", err)
	}
	return reported
}

func TestListenerCloseWhileAttached(t *testing.T) {
	node := newNode(t, iox2.ServiceTypeLocal)
	waitSet := newWaitSet(t, iox2.ServiceTypeLocal)
	listener, notifier := newListener(t, node, "")
	guard := attach(t, waitSet, listener)

	listener.Close()
	if _, err := listener.TryWait(); !errors.Is(err, iox2.ErrClosed) {
		t.Errorf("TryWait after Close: error = %v, want %v", err, iox2.ErrClosed)
	}
	if _, err := waitSet.AttachNotification(listener); !errors.Is(err, iox2.ErrClosed) {
		t.Errorf("attaching a closed listener: error = %v, want %v", err, iox2.ErrClosed)
	}

	// The file descriptor of the listener is still in use.
	if _, err := notifier.Notify(); err != nil {
		t.Fatalf("Notify: %v", err)
	}
	if !receivedFrom(t, waitSet, guard) {
		t.Error("a closed but attached listener was not reported")
	}
	guard.Close()
}

func TestWaitSetCloseDetachesEverything(t *testing.T) {
	node := newNode(t, iox2.ServiceTypeLocal)
	waitSet, err := iox2.NewWaitSetBuilder().Create(iox2.ServiceTypeLocal)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	listener, _ := newListener(t, node, "")
	listenerGuard := attach(t, waitSet, listener)
	intervalGuard, err := waitSet.AttachInterval(time.Hour)
	if err != nil {
		t.Fatalf("AttachInterval: %v", err)
	}

	listener.Close()
	for range 2 { // closing twice is safe
		waitSet.Close()
	}
	listenerGuard.Close()
	intervalGuard.Close()

	handler := func(*iox2.AttachmentID) iox2.Progression { return iox2.ProgressionStop }
	if _, err := waitSet.WaitAndProcessOnce(handler); !errors.Is(err, iox2.ErrClosed) {
		t.Errorf("wait after Close: error = %v, want %v", err, iox2.ErrClosed)
	}
	if _, err := waitSet.AttachInterval(time.Second); !errors.Is(err, iox2.ErrClosed) {
		t.Errorf("AttachInterval after Close: error = %v, want %v", err, iox2.ErrClosed)
	}
	if waitSet.Len() != 0 || waitSet.Capacity() != 0 {
		t.Errorf("Len, Capacity = %d, %d after Close, want 0, 0", waitSet.Len(), waitSet.Capacity())
	}
}

func TestAttachmentIDIsOnlyValidInTheHandler(t *testing.T) {
	waitSet := newWaitSet(t, iox2.ServiceTypeLocal)
	guard, err := waitSet.AttachInterval(10 * time.Millisecond)
	if err != nil {
		t.Fatalf("AttachInterval: %v", err)
	}
	defer guard.Close()

	var kept *iox2.AttachmentID
	if _, err := waitSet.WaitAndProcessOnce(func(id *iox2.AttachmentID) iox2.Progression {
		kept = id
		return iox2.ProgressionStop
	}); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if kept.HasEventFrom(guard) || kept.HasMissedDeadline(guard) {
		t.Error("a kept AttachmentID still reports events")
	}
}

func TestHandlerPanicContinuesInTheCaller(t *testing.T) {
	waitSet := newWaitSet(t, iox2.ServiceTypeLocal)
	guard, err := waitSet.AttachInterval(10 * time.Millisecond)
	if err != nil {
		t.Fatalf("AttachInterval: %v", err)
	}
	defer guard.Close()

	func() {
		defer func() {
			if r := recover(); r != "boom" {
				t.Errorf("recovered %v, want boom", r)
			}
		}()
		_, _ = waitSet.WaitAndProcess(func(*iox2.AttachmentID) iox2.Progression { panic("boom") })
		t.Error("WaitAndProcess returned after a panic of the handler")
	}()

	// The wait set is still usable.
	if _, err := waitSet.WaitAndProcessOnce(func(*iox2.AttachmentID) iox2.Progression { return iox2.ProgressionStop }); err != nil {
		t.Errorf("wait after a panic: %v", err)
	}
}

func TestWaitSetBuilder(t *testing.T) {
	waitSet, err := iox2.NewWaitSetBuilder().Create(iox2.ServiceTypeIpc)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer waitSet.Close()
	if mode := waitSet.SignalHandlingMode(); mode != iox2.SignalHandlingDisabled {
		t.Errorf("default signal handling = %v, want %v", mode, iox2.SignalHandlingDisabled)
	}

	if _, err := iox2.NewWaitSetBuilder().Create(iox2.ServiceType(42)); !errors.Is(err, iox2.ErrInvalidServiceType) {
		t.Errorf("invalid service type: error = %v, want %v", err, iox2.ErrInvalidServiceType)
	}
	if _, err := iox2.NewWaitSetBuilder().SignalHandlingMode(42).Create(iox2.ServiceTypeIpc); !errors.Is(err, iox2.ErrInvalidSignalHandlingMode) {
		t.Errorf("invalid signal handling mode: error = %v, want %v", err, iox2.ErrInvalidSignalHandlingMode)
	}
	for _, d := range []time.Duration{0, -time.Second} {
		if _, err := waitSet.AttachInterval(d); !errors.Is(err, iox2.ErrInvalidDuration) {
			t.Errorf("AttachInterval(%v): error = %v, want %v", d, err, iox2.ErrInvalidDuration)
		}
	}
}

func TestWaitSetErrorMessagesComeFromTheCAPI(t *testing.T) {
	errs := []error{
		iox2.WaitSetCreateErrorInsufficientResources,
		iox2.WaitSetAttachmentErrorAlreadyAttached,
		iox2.WaitSetRunErrorNoAttachments,
	}
	for _, err := range errs {
		if msg := err.Error(); !strings.HasPrefix(msg, "iox2: ") || strings.Contains(msg, "unknown") {
			t.Errorf("Error() = %q, want a message from the C API", msg)
		}
	}
	for _, err := range []error{iox2.WaitSetCreateError(1000), iox2.WaitSetAttachmentError(1000), iox2.WaitSetRunError(1000)} {
		if !strings.Contains(err.Error(), "unknown") {
			t.Errorf("Error() = %q, want it to report an unknown value", err.Error())
		}
	}
	for _, s := range []fmt.Stringer{iox2.RunResultStopRequest, iox2.RunResult(99), iox2.SignalHandlingDisabled, iox2.SignalHandlingMode(99)} {
		if s.String() == "" {
			t.Errorf("String() of %d is empty", s)
		}
	}
}

func TestWaitSetWakesUpForAnotherGoroutine(t *testing.T) {
	for _, serviceType := range serviceTypes {
		t.Run(serviceType.String(), func(t *testing.T) {
			node := newNode(t, serviceType)
			waitSet := newWaitSet(t, serviceType)
			listener, notifier := newListener(t, node, "")
			guard := attach(t, waitSet, listener)

			// Stopping a wait set from the outside takes a notification.
			go func() {
				time.Sleep(50 * time.Millisecond)
				if _, err := notifier.Notify(); err != nil {
					t.Errorf("Notify: %v", err)
				}
			}()
			result, err := waitSet.WaitAndProcess(func(id *iox2.AttachmentID) iox2.Progression {
				if !id.HasEventFrom(guard) {
					t.Error("wake-up does not come from the guard")
				}
				_, _ = listener.TryWait()
				return iox2.ProgressionStop
			})
			if err != nil || result != iox2.RunResultStopRequest {
				t.Errorf("WaitAndProcess = %v, %v, want %v", result, err, iox2.RunResultStopRequest)
			}
		})
	}
}

func TestWaitSetCloseIsSafeToRaceWithUse(t *testing.T) {
	node := newNode(t, iox2.ServiceTypeLocal)
	waitSet, err := iox2.NewWaitSetBuilder().Create(iox2.ServiceTypeLocal)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	listener, notifier := newListener(t, node, "")
	guard := attach(t, waitSet, listener)

	var wg sync.WaitGroup
	for range 4 {
		wg.Add(4)
		go func() {
			defer wg.Done()
			for range 50 {
				_, _ = notifier.Notify()
			}
		}()
		go func() {
			defer wg.Done()
			for range 50 {
				_, err := waitSet.WaitAndProcessOnceWithTimeout(func(id *iox2.AttachmentID) iox2.Progression {
					id.HasEventFrom(guard)
					_, _ = listener.TryWait()
					return iox2.ProgressionContinue
				}, time.Millisecond)
				var runErr iox2.WaitSetRunError
				if err != nil && !errors.Is(err, iox2.ErrClosed) && !errors.As(err, &runErr) {
					t.Errorf("wait: %v", err)
				}
			}
		}()
		go func() {
			defer wg.Done()
			for range 50 {
				if g, err := waitSet.AttachInterval(time.Hour); err == nil {
					g.Close()
				}
			}
		}()
		go func() {
			defer wg.Done()
			guard.Close()
			waitSet.Close()
			listener.Close()
		}()
	}
	wg.Wait()
}

// The wait set registers handlers for SIGINT and SIGTERM on request. That
// replaces the handlers of the Go runtime for the rest of the process, so the
// test runs in a child process.
func TestSignalHandlingModeTerminationRequests(t *testing.T) {
	if os.Getenv("IOX2_TEST_SIGNAL_CHILD") == "1" {
		signalChild()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestSignalHandlingModeTerminationRequests$", "-test.v")
	cmd.Env = append(os.Environ(), "IOX2_TEST_SIGNAL_CHILD=1")
	out, err := cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(out), "child result: all events handled") {
		t.Fatalf("child process: %v\n%s", err, out)
	}
}

func signalChild() {
	waitSet, err := iox2.NewWaitSetBuilder().SignalHandlingMode(iox2.SignalHandlingTerminationRequests).Create(iox2.ServiceTypeLocal)
	if err != nil {
		fmt.Println("child error:", err)
		os.Exit(1)
	}
	defer waitSet.Close()
	if mode := waitSet.SignalHandlingMode(); mode != iox2.SignalHandlingTerminationRequests {
		fmt.Println("child error: signal handling mode", mode)
		os.Exit(1)
	}
	guard, err := waitSet.AttachInterval(time.Hour)
	if err != nil {
		fmt.Println("child error:", err)
		os.Exit(1)
	}
	defer guard.Close()

	result, err := waitSet.WaitAndProcessOnceWithTimeout(func(*iox2.AttachmentID) iox2.Progression { return iox2.ProgressionStop }, 10*time.Millisecond)
	fmt.Printf("child result: %v, %v\n", result, err)
}
