#ifndef FABRIC_LIBFABRIC_H
#define FABRIC_LIBFABRIC_H

#include "fabric.h"
#include "cmdline.h"
#include <vector>
#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_errno.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_rma.h>
#include <rdma/fi_tagged.h>

class FabMutex {
public:
	FabMutex(void) { pthread_mutex_init(&_lock, NULL); }
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

class FabCondVar {
public:
	FabCondVar(FabMutex &m):mutex(m) { pthread_cond_init(&cond, NULL); }
	~FabCondVar(void) { pthread_cond_destroy(&cond); }
	void signal(void) { pthread_cond_signal(&cond); }
	void broadcast(void) { pthread_cond_broadcast(&cond); }
	void wait(void) { pthread_cond_wait(&cond, &mutex.lock);

protected:
	FabMutex &mutex;
	pthread_cond_t cond;
};

class FabMessage : public Message {
protected:
	FabMessage(NodeId dest, MessageId id, void *args, Payload *payload, bool inOrder);

public:
	virtual ~FabMessage();
	virtual int reply(MessageId id, void *args, Payload *payload, bool inOrder);

protected:
	struct iovec *iov;
	struct iovec siov[6];
	friend class FabFabric;
};

class FabFabric : public Fabric {
public:
        virtual void register_options(Realm::CommandLineParser &cp);
	virtual bool add_message_type(MessageType *mt);
 	virtual bool init();
	virtual void shutdown();

	virtual NodeId get_id();
	virtual NodeId get_max_id();
	virtual int send(Message *, bool inOrder);
	virtual bool progress(int maxToSend, bool wait);
	virtual bool incoming(Message *);
	virtual void *memalloc(size_t size);
	virtual void memfree(void *);

protected:
	NodeId	id;
	NodeId	max_id;

	std::vector<MessageType*>	mts;
	struct fid_fabric fab;
	struct fid_domain dom;
	struct fid_eq eq;
	struct fid_cq cq;
	struct fid_ep ep;
	struct fid_av av;
	struct fi_context avctx;

	// parameters
	int	max_send;
	int	pend_num;

	friend class FabMessage;
};

typedef FabMutex Mutex;
typedef FabCondVar CondVar;
 
#endif // ifndef FABRIC_LIBFABRIC_H
