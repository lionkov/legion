#ifndef RUNTIME_FABRIC_H
#define RUNTIME_FABRIC_H

// For now, fabric will depend on ActiveMessagIDs and
// Payload definitions from activemsg.h. When GASNET has
// been fully removed, this will be moved back in to fabric.h / msg.h
#include "activemsg.h"

#define NELEM(x) (sizeof(x) / sizeof(x[0]))

/* enum { PAYLOAD_NONE, // no payload in packet */
/*        PAYLOAD_KEEP, // use payload pointer, guaranteed to be stable */
/*        PAYLOAD_FREE, // take ownership of payload, free when done */
/*        PAYLOAD_COPY, // make a copy of the payload */
/* }; */

typedef uint8_t MessageId;
typedef uint32_t NodeId;
class Message;

class Mutex {
public:
	virtual void Lock()= 0;
	virtual void Unlock() = 0;
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

class Payload {
protected:
	int	mode;

	// when mode is PAYLOAD_COPY
	void*	buf;
	size_t	bufsz;

	int checkmode();
	
public:
	Payload(int m):mode(m), buf(NULL), bufsz(0) {}
	virtual ~Payload(void);

	virtual ssize_t size(void) = 0;
	virtual void* ptr(void) = 0;
	virtual ssize_t copy(void *dest, size_t destsz) = 0;
	virtual ssize_t iovec(struct iovec *iov, size_t iovnum) = 0;
};

class MessageType {
public:
	MessageId	id;		// message id
	size_t		argsz;		// argument size
	bool		payload;	// true if the message can have payload
	bool		inorder;	// true if the message has to be delivered in order

	MessageType(MessageId msgid, size_t asz, bool hasPayload, bool inOrder):id(msgid), argsz(asz), payload(hasPayload), inorder(inOrder) { }

	// called when a message of this type is received
	virtual void request(Message *m) = 0;

	int send(NodeId target);
	int send(NodeId target, void *args);
	int send(NodeId target, void *args, Payload *payload);
};

struct ContiguousPayload : public Payload {
protected:
	size_t	sz;
	void*	data;

public:
	ContiguousPayload(int mode, void *data, size_t sz);
	virtual ~ContiguousPayload(void);

	virtual ssize_t size(void);
	virtual void* ptr(void);
	virtual ssize_t copy(void *dest, size_t destsz);
	virtual ssize_t iovec(struct iovec *iov, size_t iovnum);
};

struct TwoDPayload : public Payload {
protected:
	size_t		linesz;
	size_t		linecnt;
	ptrdiff_t	stride;
	void*		data;

public:
	TwoDPayload(int m, void *data, size_t line_size, size_t line_count, ptrdiff_t line_stride);
	virtual ~TwoDPayload(void);

	virtual ssize_t size(void);
	virtual void* ptr(void);
	virtual ssize_t copy(void *dest, size_t destsz);
	virtual ssize_t iovec(struct iovec *iov, size_t iovnum);
};

typedef std::pair<const void *, size_t> SpanListEntry;
typedef std::vector<SpanListEntry> SpanList;

struct SpanPayload : public Payload {
protected:
	SpanList	spans;
	size_t		sz;
public:
	SpanPayload(int m, SpanList &sl, size_t s);
	virtual ~SpanPayload(void);

	virtual ssize_t size(void);
	virtual void* ptr(void);
	virtual ssize_t copy(void *dest, size_t destsz);
	virtual ssize_t iovec(struct iovec *iov, size_t iovnum);
};

class Message {
public:
	MessageType	*mtype;		// message type
	NodeId		sndid;		// sender id
	NodeId		rcvid;		// receiver id
	void*		args;
	Payload*	payload;

	virtual ~Message() { delete payload; free(args); }

	// can be called by the request handler to send a reply
	virtual int reply(MessageId id, void *args, Payload *payload, bool inOrder) = 0;

protected:
	Message(MessageId id, void *a, Payload *p):args(a), payload(p) { }
};

class Fabric {
public:
	// all message types need to be added before init() is called
	virtual bool add_message_type(MessageType *mt) = 0;
	virtual bool init() = 0;
	virtual void shutdown() = 0;

	virtual NodeId get_id() = 0;
	virtual NodeId get_max_id() = 0;
	virtual int send(NodeId dest, MessageId id, void *args,
			 Payload *payload, bool inOrder) = 0;
	virtual bool progress(int maxToSend, bool wait) = 0;
	virtual bool incoming(Message *) = 0;
	virtual void *memalloc(size_t size) = 0;
	virtual void memfree(void *) = 0;
};

extern Fabric *fabric;

#endif
