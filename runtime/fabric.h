#ifndef RUNTIME_FABRIC_H
#define RUNTIME_FABRIC_H

// For now, fabric will depend on ActiveMessagIDs and
// Payload definitions from activemsg.h. When GASNET has
// been fully removed, this will be moved back in to fabric.h / msg.h
#include "cmdline.h"
#include "activemsg.h"
#include "payload.h"
#include "logging.h"
#include "event.h"
#include <cstdlib>
#include <sys/uio.h>
#include <stdint.h>
#include <cstring>
#include <stdatomic.h>
#include <pthread.h>
#include <semaphore.h>

#define NELEM(x) (sizeof(x) / sizeof(x[0]))
#define MAX_MESSAGE_TYPES 256

enum MessageIds {
      FIRST_AVAILABLE = 140,
      NODE_ANNOUNCE_MSGID, 
      SPAWN_TASK_MSGID, 
      LOCK_REQUEST_MSGID,  
      LOCK_RELEASE_MSGID,  
      LOCK_GRANT_MSGID, 
      EVENT_SUBSCRIBE_MSGID, 
      EVENT_TRIGGER_MSGID, 
      EVENT_UPDATE_MSGID,  // TODO -- broadcast
      REMOTE_MALLOC_MSGID,  
      REMOTE_MALLOC_RPLID = 150,  
      CREATE_ALLOC_MSGID, 
      CREATE_ALLOC_RPLID,
      CREATE_INST_MSGID,  
      CREATE_INST_RPLID, 
      VALID_MASK_REQ_MSGID, 
      VALID_MASK_DATA_MSGID, 
      ROLL_UP_TIMER_MSGID,  
      ROLL_UP_TIMER_RPLID,  
      ROLL_UP_DATA_MSGID,
      CLEAR_TIMER_MSGID, 
      DESTROY_INST_MSGID = 160,  
      REMOTE_WRITE_MSGID,  
      REMOTE_REDUCE_MSGID, 
      REMOTE_SERDEZ_MSGID, 
      REMOTE_WRITE_FENCE_MSGID, 
      REMOTE_WRITE_FENCE_ACK_MSGID, 
      DESTROY_LOCK_MSGID,  
      REMOTE_REDLIST_MSGID,  
      MACHINE_SHUTDOWN_MSGID,  
      BARRIER_ADJUST_MSGID, 
      BARRIER_SUBSCRIBE_MSGID = 170, 
      BARRIER_TRIGGER_MSGID, 
      BARRIER_MIGRATE_MSGID, 
      METADATA_REQUEST_MSGID, 
      METADATA_RESPONSE_MSGID, // should really be a reply
      METADATA_INVALIDATE_MSGID,  // TODO -- broadcast
      METADATA_INVALIDATE_ACK_MSGID,  
      REGISTER_TASK_MSGID, 
      REGISTER_TASK_COMPLETE_MSGID, 
      EVENT_GATHER_MSGID
};

typedef uint8_t MessageId;
typedef uint32_t NodeId;

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
  Fabric() : log(NULL) { }
  ~Fabric() { }
  MessageType* mts[MAX_MESSAGE_TYPES];
  virtual bool add_message_type(MessageType *mt, const std::string tag) = 0;
  virtual bool init() = 0;
  virtual void shutdown() = 0;

  // Send messages / collectives  
  virtual int send(Message* m) = 0;
  virtual Realm::Event* gather(NodeId root);
  //virtual int broadcast(Message* m);

  // Query fabriic parameters
  virtual NodeId get_id() = 0;
  virtual uint32_t get_num_nodes() = 0;
  virtual size_t get_iov_limit() = 0;
  virtual size_t get_iov_limit(MessageId id) = 0;
  virtual int get_max_send() = 0;
  virtual void wait_for_shutdown() = 0;


  // virtual bool incoming(Message *) = 0;
  virtual void *memalloc(size_t size) = 0;
  virtual void memfree(void *) = 0;
  Realm::Logger* log;
  // Get the global Legion logger for fabric
  virtual void register_options(Realm::CommandLineParser& cp) = 0;
  Realm::Logger& log_fabric() {
    static Realm::Logger log("fabric");
    return log;
  }

 protected:
  // Handles current in-progress Event gather. You need to initialize it once number of
  // nodes are known
  Gatherer<Realm::Event> event_gatherer;
};

// Global fabric singleton
extern Fabric* fabric;


class Message {
 public:
  MessageType	*mtype;		// message type
  NodeId	sndid;		// sender id
  NodeId	rcvid;		// receiver id
  MessageId     id; 
  void* arg_ptr;
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

/* 
   Gathers incoming message contents into a single array on the 
   root node. Nonroot nodes should not use this class, they should instead
   simply send an EventGatherMessage (or other type) to the root.

   The Fabric will contain a single Gather object, which will be reused
   for each gather operation.

   Currently, only one gather may be in progress at a time on a given node, 
   otherwise behavior is undefined. The Gatherer attempts to enforce one-at-a-time
   behavior via the following rules:
   
   - If an entry apprears to be written twice, crash
   - If the wrong number of entries are received, crash
   - The object may be reused by calling reset(). However,
   if the object is reused before recoving the gather data, crash.

   The wait() function will block until all gather messages are recieved, 
   and returns a pointer to the filled gather buffer. This buffer must be 
   deallocated by the receiver using delete[].
*/

template <typename T> 
class Gatherer {
 public:
  Gatherer(size_t _num_nodes);
  ~Gatherer(); // Deallocates internal buffer only if it was never
  // returned by wait() on this object
  
  T* wait(); // Wait until all gather items have been recieved;
  // return pointer to filled buffer. The receiver of this buffer takes
  // ownership and is responsible for deallocating the gather buffer using delete[].

  void add_entry(T& entry, NodeId sender);  // register gather data from node sender
  
 protected:
  NodeId root; // ID of root / gathering node 
  size_t num_nodes; // Number of nodes in the fabric
  atomic_size_t num_recvd; // counter of number of entries recieved
  sem_t all_recvd_sem;
  T* buf; // gather buffer for all gather entries
  bool* recvd_flags; // tracks whether a given gather entry was recieved
  atomic_bool_t wait_complete; // true if all data was received and the buf pointer was returned
  bool initialized;
  void reset(); // Ready this object for a new gather. Invalid if the current gather is incomplete.
};

// extern FabricMemory *fabric_memory;

#endif
