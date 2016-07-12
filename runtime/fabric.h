#ifndef RUNTIME_FABRIC_H
#define RUNTIME_FABRIC_H

// For now, fabric will depend on ActiveMessagIDs and
// Payload definitions from activemsg.h. When GASNET has
// been fully removed, this will be moved back in to fabric.h / msg.h
#include "cmdline.h"
#include "activemsg.h"
#include <cstdlib>
#include <sys/uio.h>
#include <stdint.h>
#include <cstring>

#define NELEM(x) (sizeof(x) / sizeof(x[0]))
#define MAX_MESSAGE_TYPES 128


enum MessageIds {
  REMOTE_MALLOC_MSGID,
  REMOTE_MALLOC_RPLID = 150,
  CLEAR_TIMER_MSGID,
  ROLL_UP_TIMER_MSGID,
  ROLL_UP_TIMER_RPLID,
  NODE_ANNOUNCE_MSGID,
  SPAWN_TASK_MSGID,
  REGISTER_TASK_MSGID,
  REGISTER_TASK_COMPLETE_MSGID,
  METADATA_REQUEST_MSGID,
  METADATA_RESPONSE_MSGID,
  METADATA_INVALIDATE_MSGID, // TODO: BROADCAST
  METADATA_INVALIDATE_ACK_MSGID,
  EVENT_SUBSCRIBE_MSGID,
  EVENT_UPDATE_MSGID, // TODO: BROADCAST
  EVENT_TRIGGER_MSGID,
  BARRIER_ADJUST_MSGID,
  DESTROY_INST_MSGID = 160,
  BARRIER_TRIGGER_MSGID,
  BARRIER_SUBSCRIBE_MSGID = 170,
  BARRIER_MIGRATE_MSGID,
  LOCK_REQUEST_MSGID,
  LOCK_RELEASE_MSGID,
  LOCK_GRANT_MSGID,
  DESTROY_LOCK_MSGID,
  CREATE_INST_MSGID,
  CREATE_INST_RPLID,
  REMOTE_WRITE_MSGID
};

enum {
  FAB_PAYLOAD_ERROR,
  FAB_PAYLOAD_NONE, // no payload in packet
  FAB_PAYLOAD_KEEP, // use payload pointer, guaranteed to be stable
  FAB_PAYLOAD_FREE, // take ownership of payload, free when done
  FAB_PAYLOAD_COPY, // make a copy of the payload
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

class FabPayload {
 protected:
  int	mode;

  // when mode is PAYLOAD_COPY
  void*	buf;
  size_t	bufsz;
	

  int checkmode();
  ssize_t checkiovec(struct iovec* iov, size_t iovnum);
  ssize_t checkcopy(void *dest, size_t destsz);
	
 public:
  FabPayload(int m) : mode(m), buf(NULL), bufsz(0) {}
  virtual ~FabPayload(void);

  virtual ssize_t size(void) = 0;
  virtual void* ptr(void) = 0;
  virtual ssize_t copy(void *dest, size_t destsz) = 0;
  virtual ssize_t iovec(struct iovec *iov, size_t iovnum) = 0;
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

  int send(NodeId target);
  int send(NodeId target, void *args);
  int send(NodeId target, void *args, FabPayload *payload);
};

struct FabContiguousPayload : public FabPayload {
 protected:
  size_t sz;
  void* data;

 public:
  FabContiguousPayload(int mode, void *data, size_t s);
  virtual ~FabContiguousPayload(void);
  

  virtual ssize_t size(void);
  virtual void* ptr(void);
  virtual ssize_t copy(void *dest, size_t destsz);
  virtual ssize_t iovec(struct iovec *iov, size_t iovnum);
};

struct FabTwoDPayload : public FabPayload {
 protected:
  size_t		linesz;
  size_t		linecnt;
  ptrdiff_t	stride;
  void*		data;

 public:
  FabTwoDPayload(int m, void *data, size_t line_size, size_t line_count, ptrdiff_t line_stride);
  virtual ~FabTwoDPayload(void);

  virtual ssize_t size(void);
  virtual void* ptr(void);
  virtual ssize_t copy(void *dest, size_t destsz);
  virtual ssize_t iovec(struct iovec *iov, size_t iovnum);
};

typedef std::pair<void *, size_t> FabSpanListEntry;
typedef std::vector<FabSpanListEntry> FabSpanList;

struct FabSpanPayload : public FabPayload {
 protected:
  SpanList	spans;
  size_t		sz;
 public:
  FabSpanPayload(int m, SpanList &sl, size_t s);
  virtual ~FabSpanPayload(void);

  virtual ssize_t size(void);
  virtual void* ptr(void);
  virtual ssize_t copy(void *dest, size_t destsz);
  virtual ssize_t iovec(struct iovec *iov, size_t iovnum);
};

class Message {
 public:
  MessageType	*mtype;		// message type
  NodeId	sndid;		// sender id
  NodeId	rcvid;		// receiver id
  void*		args;
  FabPayload*	payload;

  virtual ~Message() {
    if (payload) delete payload;
    if (args) free(args);
  }
  
  // can be called by the request handler to send a reply
  // Commented out for now, I'm not sure yet if legion will actually use this
  //virtual int reply(MessageId id, void *args, Payload *payload, bool inOrder) = 0;
  struct iovec* iov;
  struct iovec siov[6];


 protected:
  Message(MessageId id, void *a, FabPayload *p):args(a), payload(p) { }  
};

class Fabric {
 public:
  // all message types need to be added before init() is called
  Fabric() { }
  ~Fabric() { }
  MessageType* mts[MAX_MESSAGE_TYPES];
  virtual bool add_message_type(MessageType *mt, const std::string tag) = 0;
  virtual bool init() = 0;
  virtual void shutdown() = 0;

  virtual NodeId get_id() = 0;
  virtual NodeId get_max_id() = 0;
  virtual int send(NodeId dest, MessageId id, void *args,
		   FabPayload *payload, bool inOrder) = 0;
  virtual int send(Message* m) = 0;
  virtual bool progress(bool wait) = 0;
  // virtual bool incoming(Message *) = 0;
  virtual void *memalloc(size_t size) = 0;
  virtual void memfree(void *) = 0;
  virtual void register_options(Realm::CommandLineParser& cp) = 0;
  virtual void wait_for_shutdown() = 0;
  virtual int get_max_send() = 0;

};

extern Fabric* fabric;

// extern FabricMemory *fabric_memory;

#endif
