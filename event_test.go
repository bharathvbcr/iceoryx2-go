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
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	iox2 "github.com/eclipse-iceoryx/iceoryx2-go"
)

// eventPorts opens the event service of the test and creates one notifier and
// one listener on it.
func eventPorts(t *testing.T, node *iox2.Node, builder func(*iox2.EventServiceBuilder) *iox2.EventServiceBuilder) (*iox2.EventService, *iox2.Notifier, *iox2.Listener) {
	t.Helper()
	service, err := builder(node.Event(serviceName(t))).OpenOrCreate()
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
	return service, notifier, listener
}

func defaults(b *iox2.EventServiceBuilder) *iox2.EventServiceBuilder { return b }

func TestNotifyReachesListener(t *testing.T) {
	for _, serviceType := range serviceTypes {
		t.Run(serviceType.String(), func(t *testing.T) {
			_, notifier, listener := eventPorts(t, newNode(t, serviceType), defaults)

			if events, err := listener.TryWait(); err != nil || len(events) != 0 {
				t.Fatalf("TryWait on a quiet listener = %v, %v, want no events and no error", events, err)
			}

			if n, err := notifier.Notify(); err != nil || n != 1 {
				t.Fatalf("Notify = %d, %v, want 1 listener and no error", n, err)
			}
			events, err := listener.TryWait()
			if err != nil {
				t.Fatalf("TryWait: %v", err)
			}
			if want := []iox2.Event{{ID: 0, Count: 1}}; !slices.Equal(events, want) {
				t.Errorf("TryWait = %v, want %v", events, want)
			}
			if events, _ := listener.TryWait(); len(events) != 0 {
				t.Errorf("second TryWait = %v, want the events to be consumed", events)
			}
		})
	}
}

func TestNotifyWithoutListenerReachesNobody(t *testing.T) {
	service, err := newNode(t, iox2.ServiceTypeLocal).Event(serviceName(t)).OpenOrCreate()
	if err != nil {
		t.Fatalf("OpenOrCreate: %v", err)
	}
	defer service.Close()
	notifier, err := service.NotifierBuilder().Create()
	if err != nil {
		t.Fatalf("create notifier: %v", err)
	}
	defer notifier.Close()

	if n, err := notifier.Notify(); err != nil || n != 0 {
		t.Errorf("Notify = %d, %v, want 0 listeners and no error", n, err)
	}
}

func TestEventIDs(t *testing.T) {
	service, notifier, listener := eventPorts(t, newNode(t, iox2.ServiceTypeLocal), defaults)
	custom, err := service.NotifierBuilder().DefaultEventID(7).Create()
	if err != nil {
		t.Fatalf("create notifier: %v", err)
	}
	defer custom.Close()

	if _, err := custom.Notify(); err != nil {
		t.Fatalf("Notify: %v", err)
	}
	if _, err := notifier.NotifyWithEventID(3); err != nil {
		t.Fatalf("NotifyWithEventID: %v", err)
	}
	events, err := listener.TryWait()
	if err != nil {
		t.Fatalf("TryWait: %v", err)
	}
	slices.SortFunc(events, func(a, b iox2.Event) int { return int(a.ID) - int(b.ID) })
	want := []iox2.Event{{ID: 3, Count: 1}, {ID: 7, Count: 1}}
	if !slices.Equal(events, want) {
		t.Errorf("TryWait = %v, want %v", events, want)
	}
}

func TestEventIDOutOfBounds(t *testing.T) {
	_, notifier, _ := eventPorts(t, newNode(t, iox2.ServiceTypeLocal), func(b *iox2.EventServiceBuilder) *iox2.EventServiceBuilder {
		return b.EventIDMaxValue(10)
	})

	if _, err := notifier.NotifyWithEventID(10); err != nil {
		t.Errorf("NotifyWithEventID(10): %v, want the maximum to be valid", err)
	}
	if _, err := notifier.NotifyWithEventID(11); !errors.Is(err, iox2.NotifyErrorEventIDOutOfBounds) {
		t.Errorf("NotifyWithEventID(11): error = %v, want %v", err, iox2.NotifyErrorEventIDOutOfBounds)
	}
}

func TestRepeatedNotificationsAreCounted(t *testing.T) {
	_, notifier, listener := eventPorts(t, newNode(t, iox2.ServiceTypeLocal), defaults)

	for range 5 {
		if _, err := notifier.NotifyWithEventID(2); err != nil {
			t.Fatalf("NotifyWithEventID: %v", err)
		}
	}
	var total uint64
	events, err := listener.TryWait()
	if err != nil {
		t.Fatalf("TryWait: %v", err)
	}
	for _, event := range events {
		if event.ID != 2 {
			t.Errorf("received event %v, want only id 2", event)
		}
		total += event.Count
	}
	if total != 5 {
		t.Errorf("TryWait counted %d notifications in %v, want 5", total, events)
	}
}

func TestEveryListenerIsNotified(t *testing.T) {
	service, notifier, first := eventPorts(t, newNode(t, iox2.ServiceTypeLocal), defaults)
	second, err := service.ListenerBuilder().Create()
	if err != nil {
		t.Fatalf("create listener: %v", err)
	}
	defer second.Close()

	if n, err := notifier.Notify(); err != nil || n != 2 {
		t.Fatalf("Notify = %d, %v, want 2 listeners and no error", n, err)
	}
	for i, listener := range []*iox2.Listener{first, second} {
		if events, err := listener.TryWait(); err != nil || len(events) != 1 {
			t.Errorf("listener %d: TryWait = %v, %v, want one event", i, events, err)
		}
	}
}

func TestTimedWait(t *testing.T) {
	for _, serviceType := range serviceTypes {
		t.Run(serviceType.String(), func(t *testing.T) {
			_, notifier, listener := eventPorts(t, newNode(t, serviceType), defaults)

			start := time.Now()
			events, err := listener.TimedWait(50 * time.Millisecond)
			if err != nil || len(events) != 0 {
				t.Fatalf("TimedWait on a quiet listener = %v, %v, want no events and no error", events, err)
			}
			if elapsed := time.Since(start); elapsed < 40*time.Millisecond {
				t.Errorf("TimedWait returned after %v, want it to wait for the timeout", elapsed)
			}

			if _, err := notifier.Notify(); err != nil {
				t.Fatalf("Notify: %v", err)
			}
			if events, err := listener.TimedWait(time.Minute); err != nil || len(events) != 1 {
				t.Errorf("TimedWait with a pending event = %v, %v, want one event", events, err)
			}

			for _, timeout := range []time.Duration{0, time.Nanosecond, -time.Second} {
				if events, err := listener.TimedWait(timeout); err != nil || len(events) != 0 {
					t.Errorf("TimedWait(%v) = %v, %v, want it to return at once", timeout, events, err)
				}
			}
		})
	}
}

func TestBlockingWaitWakesUp(t *testing.T) {
	for _, serviceType := range serviceTypes {
		t.Run(serviceType.String(), func(t *testing.T) {
			_, notifier, listener := eventPorts(t, newNode(t, serviceType), defaults)

			go func() {
				time.Sleep(50 * time.Millisecond)
				if _, err := notifier.NotifyWithEventID(5); err != nil {
					t.Errorf("NotifyWithEventID: %v", err)
				}
			}()
			events, err := listener.BlockingWait()
			if err != nil {
				t.Fatalf("BlockingWait: %v", err)
			}
			if want := []iox2.Event{{ID: 5, Count: 1}}; !slices.Equal(events, want) {
				t.Errorf("BlockingWait = %v, want %v", events, want)
			}
		})
	}
}

func TestEventServiceOpenAndCreate(t *testing.T) {
	for _, serviceType := range serviceTypes {
		t.Run(serviceType.String(), func(t *testing.T) {
			node := newNode(t, serviceType)
			name := serviceName(t)

			if _, err := node.Event(name).Open(); !errors.Is(err, iox2.EventServiceErrorDoesNotExist) {
				t.Fatalf("Open of a missing service: error = %v, want %v", err, iox2.EventServiceErrorDoesNotExist)
			}
			created, err := node.Event(name).Create()
			if err != nil {
				t.Fatalf("Create: %v", err)
			}
			defer created.Close()
			if created.Name() != name {
				t.Errorf("Name = %q, want %q", created.Name(), name)
			}

			if _, err := node.Event(name).Create(); !errors.Is(err, iox2.EventServiceErrorAlreadyExists) {
				t.Errorf("second Create: error = %v, want %v", err, iox2.EventServiceErrorAlreadyExists)
			}
			opened, err := node.Event(name).Open()
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			opened.Close()
		})
	}
}

func TestInvalidEventServiceNames(t *testing.T) {
	node := newNode(t, iox2.ServiceTypeLocal)
	for _, name := range []string{"", "bad\x00name", strings.Repeat("a", 256)} {
		_, err := node.Event(name).OpenOrCreate()
		var semantic iox2.SemanticStringError
		if !errors.As(err, &semantic) {
			t.Errorf("service name %q: error = %v, want a %T", name, err, semantic)
		}
	}
}

func TestEventPortLimitsAreReported(t *testing.T) {
	service, _, _ := eventPorts(t, newNode(t, iox2.ServiceTypeLocal), func(b *iox2.EventServiceBuilder) *iox2.EventServiceBuilder {
		return b.MaxNotifiers(1).MaxListeners(1)
	})

	if _, err := service.NotifierBuilder().Create(); !errors.Is(err, iox2.NotifierCreateErrorExceedsMaxSupportedNotifiers) {
		t.Errorf("second notifier: error = %v, want %v", err, iox2.NotifierCreateErrorExceedsMaxSupportedNotifiers)
	}
	if _, err := service.ListenerBuilder().Create(); !errors.Is(err, iox2.ListenerCreateErrorExceedsMaxSupportedListeners) {
		t.Errorf("second listener: error = %v, want %v", err, iox2.ListenerCreateErrorExceedsMaxSupportedListeners)
	}
}

func TestEventErrorMessagesComeFromTheCAPI(t *testing.T) {
	errs := []error{
		iox2.EventServiceErrorDoesNotExist,
		iox2.EventServiceErrorAlreadyExists,
		iox2.NotifierCreateErrorExceedsMaxSupportedNotifiers,
		iox2.ListenerCreateErrorExceedsMaxSupportedListeners,
		iox2.NotifyErrorEventIDOutOfBounds,
		iox2.ListenerWaitErrorInterruptSignal,
	}
	for _, err := range errs {
		if msg := err.Error(); !strings.HasPrefix(msg, "iox2: ") || strings.Contains(msg, "unknown") {
			t.Errorf("Error() = %q, want a message from the C API", msg)
		}
	}
	unknown := []error{
		iox2.EventServiceError(-1), iox2.EventServiceError(1000),
		iox2.NotifierCreateError(1000), iox2.ListenerCreateError(1000),
		iox2.NotifyError(1000), iox2.ListenerWaitError(1000),
	}
	for _, err := range unknown {
		if !strings.Contains(err.Error(), "unknown") {
			t.Errorf("Error() = %q, want it to report an unknown value", err.Error())
		}
	}
}

func TestClosedEventObjectsReportErrClosed(t *testing.T) {
	node := newNode(t, iox2.ServiceTypeLocal)
	service, notifier, listener := eventPorts(t, node, defaults)

	for range 2 { // closing twice is safe
		notifier.Close()
		listener.Close()
		service.Close()
	}

	if _, err := notifier.Notify(); !errors.Is(err, iox2.ErrClosed) {
		t.Errorf("Notify after Close: error = %v, want %v", err, iox2.ErrClosed)
	}
	if _, err := notifier.NotifyWithEventID(1); !errors.Is(err, iox2.ErrClosed) {
		t.Errorf("NotifyWithEventID after Close: error = %v, want %v", err, iox2.ErrClosed)
	}
	if _, err := listener.TryWait(); !errors.Is(err, iox2.ErrClosed) {
		t.Errorf("TryWait after Close: error = %v, want %v", err, iox2.ErrClosed)
	}
	if _, err := listener.TimedWait(time.Millisecond); !errors.Is(err, iox2.ErrClosed) {
		t.Errorf("TimedWait after Close: error = %v, want %v", err, iox2.ErrClosed)
	}
	if _, err := listener.BlockingWait(); !errors.Is(err, iox2.ErrClosed) {
		t.Errorf("BlockingWait after Close: error = %v, want %v", err, iox2.ErrClosed)
	}
	if _, err := service.NotifierBuilder().Create(); !errors.Is(err, iox2.ErrClosed) {
		t.Errorf("creating a notifier after Close: error = %v, want %v", err, iox2.ErrClosed)
	}
	if _, err := service.ListenerBuilder().Create(); !errors.Is(err, iox2.ErrClosed) {
		t.Errorf("creating a listener after Close: error = %v, want %v", err, iox2.ErrClosed)
	}

	node.Close()
	if _, err := node.Event(serviceName(t)).OpenOrCreate(); !errors.Is(err, iox2.ErrClosed) {
		t.Errorf("creating a service on a closed node: error = %v, want %v", err, iox2.ErrClosed)
	}
}

func TestEventPortsOutliveTheirService(t *testing.T) {
	service, notifier, listener := eventPorts(t, newNode(t, iox2.ServiceTypeLocal), defaults)
	service.Close()

	if n, err := notifier.Notify(); err != nil || n != 1 {
		t.Fatalf("Notify = %d, %v, want 1 listener and no error", n, err)
	}
	if events, err := listener.TryWait(); err != nil || len(events) != 1 {
		t.Errorf("TryWait = %v, %v, want one event", events, err)
	}
}

func TestNotifyAndWaitConcurrently(t *testing.T) {
	for _, serviceType := range serviceTypes {
		t.Run(serviceType.String(), func(t *testing.T) {
			_, notifier, listener := eventPorts(t, newNode(t, serviceType), defaults)

			const notifiers, perNotifier = 4, 100
			var wg sync.WaitGroup
			for range notifiers {
				wg.Go(func() {
					for range perNotifier {
						if _, err := notifier.Notify(); err != nil {
							t.Errorf("Notify: %v", err)
							return
						}
					}
				})
			}

			// Notifications of the same id coalesce, so the total can only be
			// bounded, not predicted. Every wait must hand out valid events.
			var seen uint64
			done := make(chan struct{})
			go func() {
				wg.Wait()
				close(done)
			}()
			for finished := false; !finished; {
				select {
				case <-done:
					finished = true
				default:
				}
				events, err := listener.TimedWait(time.Millisecond)
				if err != nil {
					t.Fatalf("TimedWait: %v", err)
				}
				for _, event := range events {
					if event.ID != 0 || event.Count == 0 {
						t.Fatalf("received invalid event %v", event)
					}
					seen += event.Count
				}
			}
			if seen == 0 || seen > notifiers*perNotifier {
				t.Errorf("listener saw %d notifications, want between 1 and %d", seen, notifiers*perNotifier)
			}
		})
	}
}

func TestCloseIsSafeToRaceWithEventUse(t *testing.T) {
	_, notifier, listener := eventPorts(t, newNode(t, iox2.ServiceTypeLocal), defaults)

	var wg sync.WaitGroup
	for range 4 {
		wg.Add(3)
		go func() {
			defer wg.Done()
			for range 100 {
				if _, err := notifier.Notify(); err != nil && !errors.Is(err, iox2.ErrClosed) {
					t.Errorf("Notify: %v", err)
				}
			}
		}()
		go func() {
			defer wg.Done()
			for range 100 {
				if _, err := listener.TryWait(); err != nil && !errors.Is(err, iox2.ErrClosed) {
					t.Errorf("TryWait: %v", err)
				}
			}
		}()
		go func() {
			defer wg.Done()
			notifier.Close()
			listener.Close()
		}()
	}
	wg.Wait()
}
