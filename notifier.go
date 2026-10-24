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
import "C"

import (
	"sync"
)

// NotifierBuilder collects the settings of a [Notifier]. It holds no native
// resources, so it needs no cleanup. Get one from [EventService.NotifierBuilder].
type NotifierBuilder struct {
	service        *EventService
	defaultEventID EventID
	hasDefault     bool
}

// NotifierBuilder starts building a [Notifier] for the service.
func (s *EventService) NotifierBuilder() *NotifierBuilder {
	return &NotifierBuilder{service: s}
}

// DefaultEventID sets the id that [Notifier.Notify] sends. It is 0 if the
// method is not called.
func (b *NotifierBuilder) DefaultEventID(id EventID) *NotifierBuilder {
	b.defaultEventID, b.hasDefault = id, true
	return b
}

// Create creates the [Notifier]. The caller owns the result and has to call
// [Notifier.Close] on it.
func (b *NotifierBuilder) Create() (*Notifier, error) {
	s := b.service
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.handle == nil {
		return nil, ErrClosed
	}

	builder := C.iox2_port_factory_event_notifier_builder(&s.handle, nil)
	if b.hasDefault {
		id := C.iox2_event_id_t{value: C.size_t(b.defaultEventID)}
		C.iox2_port_factory_notifier_builder_set_default_event_id(&builder, &id)
	}
	var handle C.iox2_notifier_h
	if rc := C.iox2_port_factory_notifier_builder_create(builder, nil, &handle); rc != C.IOX2_OK {
		return nil, NotifierCreateError(rc)
	}
	return &Notifier{handle: handle}, nil
}

// Notifier wakes up the [Listener] of an event service. It has to be released
// with [Notifier.Close]. A Notifier is safe for concurrent use.
type Notifier struct {
	mu     sync.Mutex
	handle C.iox2_notifier_h // nil once closed
}

// Notify sends the default event id of the notifier to all listeners of the
// service and returns how many it reached.
func (n *Notifier) Notify() (listeners int, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.handle == nil {
		return 0, ErrClosed
	}

	var notified C.c_size_t
	if rc := C.iox2_notifier_notify(&n.handle, &notified); rc != C.IOX2_OK {
		return 0, NotifyError(rc)
	}
	return int(notified), nil
}

// NotifyWithEventID sends the given event id to all listeners of the service
// and returns how many it reached. It fails with
// [NotifyErrorEventIDOutOfBounds] if the id exceeds the maximum of the service.
func (n *Notifier) NotifyWithEventID(id EventID) (listeners int, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.handle == nil {
		return 0, ErrClosed
	}

	cID := C.iox2_event_id_t{value: C.size_t(id)}
	var notified C.c_size_t
	if rc := C.iox2_notifier_notify_with_custom_event_id(&n.handle, &cID, &notified); rc != C.IOX2_OK {
		return 0, NotifyError(rc)
	}
	return int(notified), nil
}

// Close releases the notifier. Calling Close more than once is safe and has no
// further effect.
func (n *Notifier) Close() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.handle != nil {
		C.iox2_notifier_drop(n.handle)
		n.handle = nil
	}
}
