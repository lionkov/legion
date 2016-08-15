/* Copyright 2016 Stanford University, NVIDIA Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Event/UserEvent/Barrier implementations for Realm

#ifndef REALM_EVENT_IMPL_H
#define REALM_EVENT_IMPL_H

#include "event.h"
#include "id.h"
#include "nodeset.h"
#include "faults.h"

#include "fabric.h"

#include <vector>
#include <map>

namespace Realm {

#ifdef EVENT_TRACING
  // For event tracing
  struct EventTraceItem {
  public:
    enum Action {
      ACT_CREATE = 0,
      ACT_QUERY = 1,
      ACT_TRIGGER = 2,
      ACT_WAIT = 3,
    };
  public:
    unsigned time_units, event_id, event_gen, action;
  };
#endif

  extern Logger log_poison; // defined in event_impl.cc

  class EventWaiter {
  public:
    virtual ~EventWaiter(void) {}
    virtual bool event_triggered(Event e, bool poisoned) = 0;
    virtual void print(std::ostream& os) const = 0;
    virtual Event get_finish_event(void) const = 0;
  };

  // parent class of GenEventImpl and BarrierImpl
  class EventImpl {
  public:
    // test whether an event has triggered without waiting
    virtual bool has_triggered(Event::gen_t needed_gen, bool& poisoned) = 0;

    // causes calling thread to block until event has occurred
    //void wait(Event::gen_t needed_gen);

    virtual void external_wait(Event::gen_t needed_gen, bool& poisoned) = 0;

    virtual bool add_waiter(Event::gen_t needed_gen, EventWaiter *waiter/*, bool pre_subscribed = false*/) = 0;

    static bool add_waiter(Event needed, EventWaiter *waiter);

    static bool detect_event_chain(Event search_from, Event target, int max_depth, bool print_chain);
  };

  class GenEventImpl : public EventImpl {
  public:
    static const ID::ID_Types ID_TYPE = ID::ID_EVENT;

    GenEventImpl(void);

    void init(ID _me, unsigned _init_owner);

    static GenEventImpl *create_genevent(void);

    // get the Event (id+generation) for the current (i.e. untriggered) generation
    Event current_event(void) const;

    // helper to create the Event for an arbitrary generation
    Event make_event(Event::gen_t gen) const;

    // test whether an event has triggered without waiting
    virtual bool has_triggered(Event::gen_t needed_gen, bool& poisoned);

    virtual void external_wait(Event::gen_t needed_gen, bool& poisoned);

    virtual bool add_waiter(Event::gen_t needed_gen, EventWaiter *waiter);

    // creates an event that won't trigger until all input events have
    static Event merge_events(const std::set<Event>& wait_for,
			      bool ignore_faults);
    static Event merge_events(Event ev1, Event ev2,
			      Event ev3 = Event::NO_EVENT, Event ev4 = Event::NO_EVENT,
			      Event ev5 = Event::NO_EVENT, Event ev6 = Event::NO_EVENT);

    // record that the event has triggered and notify anybody who cares
    void trigger(Event::gen_t gen_triggered, int trigger_node, bool poisoned);

    // helper for triggering with an Event (which must be backed by a GenEventImpl)
    static void trigger(Event e, bool poisoned);

    // process an update message from the owner
    void process_update(Event::gen_t current_gen,
			const Event::gen_t *new_poisoned_generations,
			int new_poisoned_count);

  public: //protected:
    ID me;
    unsigned owner;
      
    // these state variables are monotonic, so can be checked without a lock for
    //  early-out conditions
    Event::gen_t generation, gen_subscribed;
    int num_poisoned_generations;
    bool has_local_triggers;

    bool is_generation_poisoned(Event::gen_t gen) const; // helper function - linear search

    // this is only manipulated when the event is "idle"
    GenEventImpl *next_free;

    // everything below here protected by this mutex
    FabMutex mutex;

    // local waiters are tracked by generation - an easily-accessed list is used
    //  for the "current" generation, whereas a map-by-generation-id is used for
    //  "future" generations (i.e. ones ahead of what we've heard about if we're
    //  not the owner)
    std::vector<EventWaiter *> current_local_waiters;
    std::map<Event::gen_t, std::vector<EventWaiter *> > future_local_waiters;

    // remote waiters are kept in a bitmask for the current generation - this is
    //  only maintained on the owner, who never has to worry about more than one
    //  generation
    NodeSet remote_waiters;

    // we'll set an upper bound on how many times any given event can be poisoned - this keeps
    // update messages from growing without bound
    static const int POISONED_GENERATION_LIMIT = 16;

    // note - we don't bother sorting the list below - the overhead of a binary search
    //  dominates for short lists
    // we also can't use an STL vector because reallocation prevents us from reading the
    //  list without the lock - instead we'll allocate the max size if/when we need
    //  any space
    Event::gen_t *poisoned_generations;

    // local triggerings - if we're not the owner, but we've triggered/poisoned events,
    //  we need to give consistent answers for those generations, so remember what we've
    //  done until our view of the distributed event catches up
    // value stored in map is whether generation was poisoned
    std::map<Event::gen_t, bool> local_triggers;
  };

  class BarrierImpl : public EventImpl {
  public:
    static const ID::ID_Types ID_TYPE = ID::ID_BARRIER;

    static const int BARRIER_TIMESTAMP_NODEID_SHIFT = 48;
    static Barrier::timestamp_t barrier_adjustment_timestamp;

    BarrierImpl(void);

    void init(ID _me, unsigned _init_owner);

    // get the Barrier (id+generation) for the current (i.e. untriggered) generation
    Barrier current_barrier(Barrier::timestamp_t timestamp = 0) const;

    // helper to create the Barrier for an arbitrary generation
    Barrier make_barrier(Event::gen_t gen, Barrier::timestamp_t timestamp = 0) const;

    static BarrierImpl *create_barrier(unsigned expected_arrivals, ReductionOpID redopid,
				       const void *initial_value = 0, size_t initial_value_size = 0);

    // test whether an event has triggered without waiting
    virtual bool has_triggered(Event::gen_t needed_gen, bool& poisoned);
    virtual void external_wait(Event::gen_t needed_gen, bool& poisoned);

    virtual bool add_waiter(Event::gen_t needed_gen, EventWaiter *waiter/*, bool pre_subscribed = false*/);

    // used to adjust a barrier's arrival count either up or down
    // if delta > 0, timestamp is current time (on requesting node)
    // if delta < 0, timestamp says which positive adjustment this arrival must wait for
    void adjust_arrival(Event::gen_t barrier_gen, int delta, 
			Barrier::timestamp_t timestamp, Event wait_on,
			NodeId sender, bool forwarded,
			const void *reduce_value, size_t reduce_value_size);

    bool get_result(Event::gen_t result_gen, void *value, size_t value_size);

  public: //protected:
    ID me;
    unsigned owner;
    Event::gen_t generation, gen_subscribed;
    Event::gen_t first_generation;
    BarrierImpl *next_free;

    FabMutex mutex; // controls which local thread has access to internal data (not runtime-visible event)

    // class to track per-generation status
    class Generation {
    public:
      struct PerNodeUpdates {
	Barrier::timestamp_t last_ts;
	std::map<Barrier::timestamp_t, int> pending;
      };

      int unguarded_delta;
      std::vector<EventWaiter *> local_waiters;
      std::map<int, PerNodeUpdates *> pernode;
      
	
      Generation(void);
      ~Generation(void);

      void handle_adjustment(Barrier::timestamp_t ts, int delta);
    };

    std::map<Event::gen_t, Generation *> generations;

    // a list of remote waiters and the latest generation they're interested in
    // also the latest generation that each node (that has ever subscribed) has been told about
    std::map<unsigned, Event::gen_t> remote_subscribe_gens, remote_trigger_gens;
    std::map<Event::gen_t, Event::gen_t> held_triggers;

    unsigned base_arrival_count;
    ReductionOpID redop_id;
    const ReductionOpUntyped *redop;
    char *initial_value;  // for reduction barriers

    unsigned value_capacity; // how many values the two allocations below can hold
    char *final_values;   // results of completed reductions
  };

  // active messages

  class EventSubscribeMessageType : public MessageType {
  public: 
  EventSubscribeMessageType()
    : MessageType(EVENT_SUBSCRIBE_MSGID, sizeof(RequestArgs), false, true) { }
    
    struct RequestArgs {
    RequestArgs(NodeId _node, Event _event, Event::gen_t _previous_subscribe_gen)
    : node(_node), event(_event), previous_subscribe_gen(_previous_subscribe_gen) { }
      NodeId node;
      Event event;
      Event::gen_t previous_subscribe_gen;
    };

    void request(Message* m);
    static void send_request(NodeId target, Event event, Event::gen_t previous_gen);
  };

  class EventSubscribeMessage : public Message {
  public:
  EventSubscribeMessage(NodeId dest, NodeId node, Event event,
			Event::gen_t previous_subscribe_gen)
    : Message(dest, EVENT_SUBSCRIBE_MSGID, &args, NULL),
      args(node, event, previous_subscribe_gen) { }

    EventSubscribeMessageType::RequestArgs args;
  };

  // EventTriggerMessage is used by non-owner nodes to trigger an event
  // EventUpdateMessage is used by the owner node to tell non-owner nodes about one or
  //   more triggerings of an event
  
  class EventTriggerMessageType : public MessageType {
  public:
  EventTriggerMessageType()
    : MessageType(EVENT_TRIGGER_MSGID, sizeof(RequestArgs), false, true) { }
    
    struct RequestArgs {
    RequestArgs(NodeId _node, Event _event, bool _poisoned)
    : node(_node), event(_event), poisoned(_poisoned) { }
      NodeId node;
      Event event;
      bool poisoned;
    };

    void request(Message* m);
    static void send_request(NodeId target, Event event, bool poisoned);
  };

  class EventTriggerMessage : public Message {
  public:
  EventTriggerMessage(NodeId dest, NodeId node, Event event, bool poisoned)
    : Message(dest, EVENT_TRIGGER_MSGID, &args, NULL),
      args(node, event, poisoned) { }
    EventTriggerMessageType::RequestArgs args;
  };
  
  class EventUpdateMessageType : public MessageType {
  public:
  EventUpdateMessageType()
    : MessageType(EVENT_UPDATE_MSGID, sizeof(RequestArgs), true, true) { }

    struct RequestArgs {
    RequestArgs(Event _event)
      : event(_event) { }
      Event event;
    };

    struct BroadcastHelper : RequestArgs {
    BroadcastHelper(Event _event, int num_poisoned,
		    const Event::gen_t* poisoned_generations,
		    int payload_mode)
      : RequestArgs(_event),
	payload(new FabContiguousPayload(payload_mode,
					 (void*) poisoned_generations,
					 num_poisoned)) { }
      
      FabContiguousPayload* payload;
      void apply(NodeId target);
      void broadcast(const NodeSet& targets);
    };

   
    void request(Message* m);
    static void send_request(NodeId target, Event event,
			     int num_poisoned, const Event::gen_t *poisoned_generations);
    
    static void broadcast_request(const NodeSet& targets, Event event,
				  int num_poisoned, const Event::gen_t* poisoned_generations);
  };

  class EventUpdateMessage : public Message {
  public:
  EventUpdateMessage(NodeId dest, Event event, FabPayload* payload)
    : Message(dest, EVENT_UPDATE_MSGID, &args, payload),
      args(event) { }
    EventUpdateMessageType::RequestArgs args;
  };

  class BarrierAdjustMessageType : public MessageType {
  public: 
  BarrierAdjustMessageType()
    : MessageType(BARRIER_ADJUST_MSGID, sizeof(RequestArgs), true, true) { }
      
    struct RequestArgs {
    RequestArgs(int _sender, int _delta, Barrier _barrier, Event _wait_on)
    : sender(_sender), delta(_delta), barrier(_barrier), wait_on(_wait_on) { }
      int sender;
      int delta;
      Barrier barrier;
      Event wait_on;
    };

    void request(Message* m);

    static void send_request(NodeId target, Barrier barrier, int delta, Event wait_on,
			     NodeId sender, bool forwarded,
			     const void *data, size_t datalen);
  };

  class BarrierAdjustMessage : public Message {
  public:
  BarrierAdjustMessage(NodeId dest,
		       int sender,
		       int delta,
		       Barrier barrier,
		       Event wait_on,
		       FabPayload* payload)
    : Message(dest, BARRIER_ADJUST_MSGID, &args, payload),
      args(sender, delta, barrier, wait_on) { }
    
    BarrierAdjustMessageType::RequestArgs args;
  };


  class BarrierSubscribeMessageType : public MessageType {
  public:
  BarrierSubscribeMessageType()
    : MessageType(BARRIER_SUBSCRIBE_MSGID, sizeof(RequestArgs), false, true) { }
    
    struct RequestArgs {
    RequestArgs(NodeId _subscriber, ID::IDType _barrier_id, Event::gen_t _subscribe_gen, bool _forwarded)
    : subscriber(_subscriber), barrier_id(_barrier_id), subscribe_gen(_subscribe_gen), forwarded(_forwarded) { }
      NodeId subscriber;
      ID::IDType barrier_id;
      Event::gen_t subscribe_gen;
      bool forwarded;
    };

    void request(Message* m);
    static void send_request(NodeId target, ID::IDType barrier_id,
			     Event::gen_t subscribe_gen,
			     NodeId subscriber, bool forwarded);
  };

  class BarrierSubscribeMessage : public Message {
  public:
  BarrierSubscribeMessage(NodeId dest, NodeId subscriber, ID::IDType barrier_id,
			  Event::gen_t subscribe_gen, bool forwarded)
    : Message(dest, BARRIER_SUBSCRIBE_MSGID, &args, NULL),
      args(subscriber, barrier_id, subscribe_gen, forwarded) { }
    BarrierSubscribeMessageType::RequestArgs args;
  };

  
  class BarrierTriggerMessageType : public MessageType {
  public:
  BarrierTriggerMessageType()
    : MessageType(BARRIER_TRIGGER_MSGID, sizeof(RequestArgs), true, true) { }
    
    struct RequestArgs {
    RequestArgs(NodeId _node, ID::IDType _barrier_id, Event::gen_t _trigger_gen,
		Event::gen_t _previous_gen, Event::gen_t _first_generation,
		ReductionOpID _redop_id, NodeId _migration_target, unsigned _base_arrival_count)
      : node(_node), barrier_id(_barrier_id), trigger_gen(_trigger_gen),
	previous_gen(_previous_gen), first_generation(_first_generation),
	redop_id(_redop_id), migration_target(_migration_target),
	base_arrival_count(_base_arrival_count) { }
      
      NodeId node;
      ID::IDType barrier_id;
      Event::gen_t trigger_gen;
      Event::gen_t previous_gen;
      Event::gen_t first_generation;
      ReductionOpID redop_id;
      NodeId migration_target;
      unsigned base_arrival_count;
    };

    void request(Message* m);
      
    static void send_request(NodeId target, ID::IDType barrier_id,
			     Event::gen_t trigger_gen, Event::gen_t previous_gen,
			     Event::gen_t first_generation, ReductionOpID redop_id,
			     NodeId migration_target, unsigned base_arrival_count,
			     const void *data, size_t datalen);
  };

  class BarrierTriggerMessage : public Message {
  public:
  BarrierTriggerMessage(NodeId dest, NodeId node, ID::IDType barrier_id, Event::gen_t trigger_gen,
			Event::gen_t previous_gen,  Event::gen_t first_gen,
			ReductionOpID redop_id, NodeId migration_target,
			unsigned base_arrival_count, FabPayload* payload)
    : Message(dest, BARRIER_TRIGGER_MSGID, &args, payload),
      args(node, barrier_id, trigger_gen, previous_gen, first_gen, redop_id,
	   migration_target, base_arrival_count) { }
    
    BarrierTriggerMessageType::RequestArgs args;    
  };

  class BarrierMigrationMessageType : public MessageType {
  public:
  BarrierMigrationMessageType()
    : MessageType(BARRIER_MIGRATE_MSGID, sizeof(RequestArgs), false, true) { }
      
    struct RequestArgs {
    RequestArgs(Barrier _barrier, NodeId _current_owner)
    : barrier(_barrier), current_owner(_current_owner) { }
      Barrier barrier;
      NodeId current_owner;
    };

    void request(Message* m);
    static void send_request(NodeId target, Barrier barrier, NodeId owner);
  };

  class BarrierMigrationMessage : public Message {
  public:
  BarrierMigrationMessage(NodeId dest, Barrier barrier, NodeId current_owner)
    : Message(dest, BARRIER_MIGRATE_MSGID, &args, NULL),
      args(barrier, current_owner) { }
    BarrierMigrationMessageType::RequestArgs args;
  };
    
}; // namespace Realm

#include "event_impl.inl"

#endif // ifndef REALM_EVENT_IMPL_H

