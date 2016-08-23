#ifndef RUNTIME_FABRIC_H
#define RUNTIME_FABRIC_H

// TEMPORARY -- remove this in case people want to compile without fabric
#define USE_FABRIC


// For now, fabric will depend on ActiveMessagIDs and
// Payload definitions from activemsg.h. When GASNET has
// been fully removed, this will be moved back in to fabric.h / msg.h
#include "fabric_types.h"
#include "collective.h"
#include "barrier.h"
#include "cmdline.h"
#include "payload.h"
#include "logging.h"
#include "event.h"
#include <cstdlib>
#include <sys/uio.h>
#include <stdint.h>
#include <cstring>

#define MAX_MESSAGE_TYPES 256

class Message;

class Mutex {
 public:
  virtual void lock() = 0;
  virtual void unlock() = 0;
};

class CondVar {
 public:
  CondVar(Mutex *);
  virtual void signal(void) = 0;
  virtual void broadcast(void) = 0;
  virtual void wait(void) = 0;

 protected:
  Mutex *lock;
};

class MessageType {
 public:
  MessageId	id;		// message id
  size_t	argsz;		// argument size
  bool		payload;	// true if the message can have payload
  bool		inorder;	// true if the message has to be delivered in order

 MessageType(MessageId msgid, size_t asz, bool hasPayload, bool inOrder)
   : id(msgid), argsz(asz), payload(hasPayload), inorder(inOrder) { }

  // called when a message of this type is received
  virtual void request(Message *m) = 0;
};

class Fabric {
 public:
  // all message types need to be added before init() is called
  Fabric() : log(NULL), num_msgs_added(0) { }
  virtual ~Fabric() { }

  // Refers to each registered message type
  MessageType* mts[MAX_MESSAGE_TYPES];
  std::string mdescs[MAX_MESSAGE_TYPES];
  
  // Initialization, shutdown
  // Each message type must be added before initialization
  virtual bool add_message_type(MessageType *mt, const std::string tag) = 0;
  // register_options must be called before init() for command
  // line parameters to take effect
  virtual void register_options(Realm::CommandLineParser& cp) = 0;
  virtual bool init(bool manually_set_addresses = false) = 0;
  virtual void shutdown() = 0;
  virtual void synchronize_clocks() = 0;
  // call on fatal error - clean up RT and exit
  virtual void fatal_shutdown(int code) = 0;

  // 'Registered' memory is for one-sided RDMA operations --
  // it's size is specifed via ll:rsize option, and does not change.
  // Put bytes into the registered block at given offset
  virtual void regmem_put(NodeId target, off_t offset, const void* src, size_t len) = 0;
  // Read bytes from the registered block at given offset
  virtual void regmem_get(NodeId target, off_t offset, void* dst, size_t len) = 0;
  // Get pointer to the registered buffer -- local node may use it like normal memory
  virtual void* get_regmem_ptr() = 0;
  // Wait for all RDMA events to complete asyncrhonously
  virtual void wait_for_rdmas() = 0;
 
  // Send messages 
  virtual int send(Message* m) = 0;

  // Collectives and barriers
  virtual Realm::Event* gather_events(Realm::Event& event, NodeId root) = 0;
  virtual void recv_gather_event(Realm::Event& event, NodeId sender) = 0;
  virtual void broadcast_events(Realm::Event& event, NodeId root) = 0;
  virtual void recv_broadcast_event(Realm::Event& event, NodeId sender) = 0;
  virtual void barrier_wait(uint32_t barrier_id) = 0;
  virtual void barrier_notify(uint32_t barrier_id) = 0;
  virtual void recv_barrier_notify(uint32_t barrier_id, NodeId sender) = 0;

  // Query fabric parameters
  virtual NodeId get_id() = 0;
  virtual uint32_t get_num_nodes() = 0;
  virtual size_t get_iov_limit() = 0;
  virtual size_t get_iov_limit(MessageId id) = 0;
  virtual int get_max_send() = 0;
  virtual void wait_for_shutdown() = 0;

  Realm::Logger* log;
  Realm::Logger& log_fabric() {
    static Realm::Logger log("fabric");
    return log;
  }

 protected:
  // Handles current in-progress Event gather. You need to initialize it once number of
  // nodes are known
  Gatherer<Realm::Event> event_gatherer;

  // Handles current Broadcast. Does not need to be initialized
  Broadcaster<Realm::Event> event_broadcaster;

  // Handles barriers. Must be initialized once number of nodes is known.
  BarrierWaiter barrier_waiter;
  
  uint32_t num_msgs_added; // Keeps track of number of messages added
};

// Global fabric singleton
extern Fabric* fabric;


class Message {
 public:
  MessageType*  mtype;		// message type
  NodeId	sndid;		// sender id
  NodeId	rcvid;		// receiver id
  MessageId     id; 
  void*         arg_ptr;
  FabPayload*	payload;

  virtual ~Message() {
    if (payload)
      delete payload;
    if (iov != siov && iov)
      delete iov;
  }
  
  // can be called by the request handler to send a reply
  // Commented out for now, I'm not sure yet if legion will actually use this
  //virtual int reply(MessageId id, void *args, Payload *payload, bool inOrder) = 0;
  struct iovec* iov;
  struct iovec siov[6];

  void* get_arg_ptr() { return arg_ptr; }
  void set_arg_ptr(void* a) { arg_ptr = a; }

  Message(NodeId dest, MessageId _id, void *a, FabPayload *p)
     : rcvid(dest), id(_id), arg_ptr(a), payload(p) {
    mtype = fabric->mts[id];
    rcvid = dest;
    sndid = fabric->get_id();
    iov = NULL;
  }
  
};   

// Message types

// EventGatherMessage -- register and incoming Event for a gather collective
class EventGatherMessageType : public MessageType {
 public:
 EventGatherMessageType()
   : MessageType(EVENT_GATHER_MSGID, sizeof(RequestArgs), false, true) { }

  struct RequestArgs {
  RequestArgs(Realm::Event& _event, NodeId _sender)
  : event(_event), sender(_sender) { }
    Realm::Event event;
    NodeId sender;
  };

  void request(Message* m);
  static void send_request(NodeId dest, Realm::Event& event);
};

class EventGatherMessage : public Message {
 public:
 EventGatherMessage(NodeId dest, Realm::Event& _event, NodeId sender)
   : Message(dest, EVENT_GATHER_MSGID, &args, NULL),
    args(_event, sender) { }

  EventGatherMessageType::RequestArgs args;
};

// EventBroadcastMessage -- register and incoming Event for broadcast
class EventBroadcastMessageType : public MessageType {
 public:
 EventBroadcastMessageType()
   : MessageType(EVENT_BROADCAST_MSGID, sizeof(RequestArgs), false, true) { }

  struct RequestArgs {
  RequestArgs(Realm::Event& _event, NodeId _sender)
  : event(_event), sender(_sender) { }
    Realm::Event event;
    NodeId sender;
  };

  void request(Message* m);
  static void send_request(NodeId dest, Realm::Event& event);
};

class EventBroadcastMessage : public Message {
 public:
 EventBroadcastMessage(NodeId dest, Realm::Event& _event, NodeId sender)
   : Message(dest, EVENT_BROADCAST_MSGID, &args, NULL),
    args(_event, sender) { }

  EventBroadcastMessageType::RequestArgs args;
};


class BarrierNotifyMessageType : public MessageType {
 public:
 BarrierNotifyMessageType()
   : MessageType(BARRIER_NOTIFY_MSGID, sizeof(RequestArgs), false, true) { }

  struct RequestArgs {
    RequestArgs(uint32_t _barrier_id, NodeId _sender)
      : barrier_id(_barrier_id), sender(_sender) { }
    uint32_t barrier_id;
    NodeId sender;
  };

  void request(Message* m);
  static void send_request(NodeId dest, uint32_t barrier_id);
};

class BarrierNotifyMessage : public Message {
 public:
  BarrierNotifyMessage(NodeId dest, uint32_t barrier_id, NodeId sender)
    : Message(dest, BARRIER_NOTIFY_MSGID, &args, NULL),
      args(barrier_id, sender) { }

  BarrierNotifyMessageType::RequestArgs args;
};


// extern FabricMemory *fabric_memory;

#endif
