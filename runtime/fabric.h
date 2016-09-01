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
  virtual void signal(void) = 0;
  virtual void broadcast(void) = 0;
  virtual void wait(void) = 0;
  virtual Mutex& get_mutex() = 0;
};

class AutoLock {
public:
  virtual void release() = 0;
  virtual void reacquire() = 0;
};

// Default mutex/cond implementations -- use pthreads
class FabMutex : public Mutex {
public:
  FabMutex(void) { pthread_mutex_init(&_lock, NULL); }
  FabMutex(FabMutex& other) { assert(false && "Cannot copy FabMutex"); }
  FabMutex(FabMutex&& rhs) { assert(false && "Cannot move FabMutex"); }
  ~FabMutex(void) { pthread_mutex_destroy(&_lock); }
    
  void lock(void) { pthread_mutex_lock(&_lock); }
  void unlock(void) { pthread_mutex_unlock(&_lock); }
    
protected:
  friend class FabCondVar;
  pthread_mutex_t _lock;

private:
  // Should never be copied
  FabMutex(const FabMutex& m) { assert(false); }
  FabMutex& operator=(const FabMutex &m) { assert(false); return *this; }
};

class FabCondVar : public CondVar {
public:
  FabCondVar(FabMutex &m) : mutex(m) { pthread_cond_init(&cond, NULL); }
  FabCondVar(FabCondVar& other) : mutex(other.mutex)
  { assert(false && "Cannot copy FabCondVar"); }
  FabCondVar(FabCondVar&& other) : mutex(other.mutex)
  { assert(false && "Cannot move FabCondVar"); }
  ~FabCondVar(void) { pthread_cond_destroy(&cond); }
  
  void signal(void) { pthread_cond_signal(&cond); }
  void broadcast(void) { pthread_cond_broadcast(&cond); }
  void wait(void) { pthread_cond_wait(&cond, &mutex._lock); }
  
  FabMutex& get_mutex() { return mutex; };
   
protected:
  friend class FabAutoLock;
  FabMutex &mutex;
  pthread_cond_t cond;
};

class FabAutoLock {
public:
  FabAutoLock(FabMutex& _mutex) : mutex(_mutex), held(true) { mutex.lock(); }
  FabAutoLock(FabCondVar& cond) : mutex(cond.mutex), held(true) { mutex.lock(); }
  FabAutoLock(FabAutoLock& other) : mutex(other.mutex) { assert(false && "Cannot copy FabAutoLock"); }
  FabAutoLock(FabAutoLock&& other): mutex(other.mutex) { assert(false && "Cannot move FabAutoLock"); }
  ~FabAutoLock();
  void release();
  void reacquire();

protected:
  FabMutex& mutex;
  bool held;
};


// A MessageType is a static instance of a message -- it describes message attributes
// that do not change, i.e. message parameters, the handler function (request()), and
// optionally a struct for containing message arguments. Each MessageType must be registered
// with the fabric before use via the add_message_type() method.
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
  
  Fabric() : log(NULL), num_msgs_added(0) { }
  virtual ~Fabric() { }

  // Holds references to each registered message type.
  MessageType* mts[MAX_MESSAGE_TYPES];
  std::string mdescs[MAX_MESSAGE_TYPES];
  
  // INITIALIZATION AND SHUTDOWN
  // Each message type must be added before initialization
  virtual bool add_message_type(MessageType *mt, const std::string tag) = 0;
  
  // register_options must be called before init() for command
  // line parameters to take effect
  virtual void register_options(Realm::CommandLineParser& cp) = 0;

  // Initialize the fabric. This must be called after adding all message types
  // and registering command line options. It must be called before any futher use of
  // the fabric.
  virtual bool init(bool manually_set_addresses = false) = 0;

  // Request that the fabric clean up and shut down
  virtual void shutdown() = 0;

  // Block until the fabric shuts down, either from a local shutdown() call
  // or a remote request
  virtual void wait_for_shutdown() = 0;
  
  // Called after init, this method exchanges messages between all nodes to
  // attempt clock synchronization.
  virtual void synchronize_clocks() = 0;
  
  // call on fatal error - clean up RT and exit immediately
  virtual void fatal_shutdown(int code) = 0;

  // REGISTERED MEMORY
  // 'Registered' memory is for one-sided RDMA operations --
  // it's size is specifed via ll:rsize option, and does not change.
  // Legion will use this region like a normal memory on the local node, and
  // will export it to other nodes as a RemoteMemory
  
  // Put bytes into the registered block at given offset
  virtual void put_bytes(NodeId target, off_t offset, const void* src, size_t len) = 0;
  
  // Read bytes from the registered block at given offset
  virtual void get_bytes(NodeId target, off_t offset, void* dst, size_t len) = 0;
  
  // Get pointer to the registered buffer -- local node may use it like normal memory
  virtual void* get_regmem_ptr() = 0;
  
  // Wait for all RDMA events to complete asynchronously
  virtual void wait_for_rdmas() = 0;

  // Get the size of the RDMA-able buffer in megabytes
  virtual size_t get_regmem_size_in_mb() = 0;

  // SENDING MESSAGES, COLLECTIVES
  
  // Send a message, created using new
  virtual int send(Message* m) = 0;

  // Gather events to the node root. The root nodes will receive an array of Event objects
  // which it now owns, and should deallocate with delete[]. All other nodes receive NULL.
  virtual Realm::Event* gather_events(Realm::Event& event, NodeId root) = 0;

  // Register an incoming gather event
  virtual void recv_gather_event(Realm::Event& event, NodeId sender) = 0;

  // Broadcast an event from the root node to all other nodes. The broadcasted data will
  // be recieved into event. The root node will send its data to other nodes; other nodes
  // will wait until the broadcast event is received.
  virtual void broadcast_events(Realm::Event& event, NodeId root) = 0;

  // Register an incoming broadcast event
  virtual void recv_broadcast_event(Realm::Event& event, NodeId sender) = 0;

  // Wait until all nodes in fabric have called barrier_notify with id barrier_id
  virtual void barrier_wait(uint32_t barrier_id) = 0;

  // Send barrier notification for this node on barrier barrier_id
  virtual void barrier_notify(uint32_t barrier_id) = 0;

  // Register a barrier notification
  virtual void recv_barrier_notify(uint32_t barrier_id, NodeId sender) = 0;

  // QUERYING FABRIC PARAMETERS
  
  // Get this node's ID
  virtual NodeId get_id() = 0;

  // Get the number of nodes in the fabric
  virtual uint32_t get_num_nodes() = 0;

  // Get the maximum number of IOVs that can be sent in a single message
  virtual size_t get_iov_limit() = 0;

  // Get the maximum number of IOVs that can be sent for a message of this type
  // -- this method may allow you to send extra IOVs for some message types
  virtual size_t get_iov_limit(MessageId id) = 0;

  // Get the maximum number of bytes that can be sent in a payload
  virtual size_t get_max_send() = 0;

  Realm::Logger* log;
  Realm::Logger& log_fabric() {
    static Realm::Logger log("fabric");
    return log;
  }

 protected:
  // Handles current in-progress Event gather. Must be initialized in init(), once
  // number of nodes is known
  Gatherer<Realm::Event> event_gatherer;

  // Handles current Broadcast. Does not need to be initialized
  Broadcaster<Realm::Event> event_broadcaster;

  // Handles barriers. Must be initialized once number of nodes is known.
  BarrierWaiter barrier_waiter;
  
  // Keeps track of number of messages added
  uint32_t num_msgs_added;
};

// Global fabric singleton
extern Fabric* fabric;

// The Message class represents an instance of a MessageType.
// It may contain arguments and an optional payload.
// The corresponding MessageType handler will be invoked when a
// Message is recieved.
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

// MESSAGE TYPES

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
