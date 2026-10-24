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

// #include <stdlib.h>
// #include "iox2/iceoryx2.h"
import "C"

import (
	"sync"
	"unsafe"
)

// EventID identifies the kind of an event. A [Notifier] sends an event id to
// all [Listener] of a service. The valid range is 0 up to the maximum of the
// service, see [EventServiceBuilder.EventIDMaxValue].
type EventID uint

// EventServiceBuilder collects the settings of an event service. It holds no
// native resources, so it needs no cleanup. Get one from [Node.Event].
type EventServiceBuilder struct {
	node *Node
	name string

	maxNotifiers, maxListeners, eventIDMaxValue optionalSize
}

// optionalSize is a setting that is only applied if it was set.
type optionalSize struct {
	value int
	set   bool
}

// Event starts building an event service with the given name. A service name
// is a path like "Sensors/Camera/Front" of ASCII characters.
func (n *Node) Event(serviceName string) *EventServiceBuilder {
	return &EventServiceBuilder{node: n, name: serviceName}
}

// MaxNotifiers sets how many [Notifier] the service supports. The service
// default applies if it is not called.
func (b *EventServiceBuilder) MaxNotifiers(value int) *EventServiceBuilder {
	b.maxNotifiers = optionalSize{value, true}
	return b
}

// MaxListeners sets how many [Listener] the service supports. The service
// default applies if it is not called.
func (b *EventServiceBuilder) MaxListeners(value int) *EventServiceBuilder {
	b.maxListeners = optionalSize{value, true}
	return b
}

// EventIDMaxValue sets the largest [EventID] the service supports. The service
// default applies if it is not called.
func (b *EventServiceBuilder) EventIDMaxValue(value EventID) *EventServiceBuilder {
	b.eventIDMaxValue = optionalSize{int(value), true}
	return b
}

// OpenOrCreate opens the service if it exists and creates it otherwise. The
// caller owns the result and has to call [EventService.Close] on it.
func (b *EventServiceBuilder) OpenOrCreate() (*EventService, error) {
	return b.build(func(builder C.iox2_service_builder_event_h, handle *C.iox2_port_factory_event_h) C.int {
		return C.iox2_service_builder_event_open_or_create(builder, nil, handle)
	})
}

// Open opens a service that already exists. It fails with
// [EventServiceErrorDoesNotExist] otherwise. The caller owns the result and has
// to call [EventService.Close] on it.
func (b *EventServiceBuilder) Open() (*EventService, error) {
	return b.build(func(builder C.iox2_service_builder_event_h, handle *C.iox2_port_factory_event_h) C.int {
		return C.iox2_service_builder_event_open(builder, nil, handle)
	})
}

// Create creates a service that does not exist yet. It fails with
// [EventServiceErrorAlreadyExists] otherwise. The caller owns the result and
// has to call [EventService.Close] on it.
func (b *EventServiceBuilder) Create() (*EventService, error) {
	return b.build(func(builder C.iox2_service_builder_event_h, handle *C.iox2_port_factory_event_h) C.int {
		return C.iox2_service_builder_event_create(builder, nil, handle)
	})
}

func (b *EventServiceBuilder) build(
	finish func(C.iox2_service_builder_event_h, *C.iox2_port_factory_event_h) C.int,
) (*EventService, error) {
	cName := C.CString(b.name)
	defer C.free(unsafe.Pointer(cName))
	var nameHandle C.iox2_service_name_h
	if rc := C.iox2_service_name_new(nil, cName, C.c_size_t(len(b.name)), &nameHandle); rc != C.IOX2_OK {
		return nil, SemanticStringError(rc)
	}
	defer C.iox2_service_name_drop(nameHandle)

	b.node.mu.Lock()
	defer b.node.mu.Unlock()
	if b.node.handle == nil {
		return nil, ErrClosed
	}

	serviceBuilder := C.iox2_node_service_builder(&b.node.handle, nil, C.iox2_cast_service_name_ptr(nameHandle))
	eventBuilder := C.iox2_service_builder_event(serviceBuilder)
	if b.maxNotifiers.set {
		C.iox2_service_builder_event_set_max_notifiers(&eventBuilder, C.c_size_t(b.maxNotifiers.value))
	}
	if b.maxListeners.set {
		C.iox2_service_builder_event_set_max_listeners(&eventBuilder, C.c_size_t(b.maxListeners.value))
	}
	if b.eventIDMaxValue.set {
		C.iox2_service_builder_event_set_event_id_max_value(&eventBuilder, C.c_size_t(b.eventIDMaxValue.value))
	}

	var handle C.iox2_port_factory_event_h
	if rc := finish(eventBuilder, &handle); rc != C.IOX2_OK {
		return nil, EventServiceError(rc)
	}
	return &EventService{name: b.name, handle: handle}, nil
}

// EventService is an open event service. Notifiers and listeners are created
// from it. It has to be released with [EventService.Close]. An EventService is
// safe for concurrent use.
type EventService struct {
	// name is immutable after creation.
	name string

	mu     sync.Mutex
	handle C.iox2_port_factory_event_h // nil once closed
}

// Name returns the name of the service.
func (s *EventService) Name() string {
	return s.name
}

// Close releases the service. Notifiers and listeners that were created from it
// stay usable. Calling Close more than once is safe and has no further effect.
func (s *EventService) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.handle != nil {
		C.iox2_port_factory_event_drop(s.handle)
		s.handle = nil
	}
}
