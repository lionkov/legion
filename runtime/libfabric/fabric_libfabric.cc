#include "fabric_libfabric.h"
#include "pmi.h"

FabFabric *fabric;

FabMessage::FabMessage(NodeId dest, MessageId id, void *args, Payload *payload):Message(id, a, p)
{
	mtype = fabric->mts[id];
	rcvid = dest;
}

FabMessage::~FabMessage()
{
	if (m->iov != m->siov)
		free(m->iov);

	delete(m->payload);
}

virtual int FabMessage::reply(MessageId id, void *args, Payload *payload, bool inOrder)
{
	FabMessage *r = new FabMessage(m->sndid, id, args, payload);
	return fabric->send(m, inOrder);
}

FabFabric::FabFabric():max_send(1024*1024), pend_num(16)
{
	PMI_Get_size(&max_id);
	PMI_Get_rank(&id);
}

void FabFabric::register_options(CommandLineParser &cp)
{
	cp.add_option_int("-ll:maxsend", maxsend);
	cp.add_option_int("-ll:pendnum", pendnum);
}

bool FabFabric::init()
{
	struct fi_info *hints, *fi;
	struct fi_cq_attr cqattr;
	struct fi_eq_attr eqattr;
	struct fi_av_attr av_attr;

	hints = fi_allocinfo();
	hints->ep_attr.type = FI_EP_RDM;
	hints.caps = FI_TAGGED | FI_MSG | FI_DIRECTED_RECV | FI_RMA;
	hints->mode = FI_CONTEXT | FI_LOCAL_MR;
	hints->domain_attr.mr_mode = FI_MR_BASIC;

	fi = NULL;
	ret = fi_getinfo(FI_VERSION(1, 0), NULL, NULL, 0, hints, &fi);
	if (ret != 0)
		goto error;

	ret = fi_fabric(fi->fabric_attr, &fab, NULL);
	if (ret != 0)
		goto error;

	memset(eqattr, 0, sizeof(eqattr));
	eqattr.size = FI_WAIT_UNSPEC;
	ret = fi_eq_open(fab, &eqattr, &eq, NULL);
	if (ret != 0)
		goto error;

	ret = fi_domain(fab, fi, &dom, NULL);
	if (ret != 0)
		goto error;


	ret = fi_endpoint(dom, fi, &ep, NULL);
	if (ret != 0)
		goto error;

	ret = fi_ep_bind(ep, &eq->fid, 0);
	if (ret != 0)
		goto error;

	memset(cqattr, 0, sizeof(cqattr));
	cqattr.format = FI_CQ_FORMAT_TAGGED;
	cqattr.wait_obj = FI_WAIT_UNSPEC;
	cqattr.wait_cond = FI_CQ_COND_NONE;
	cqattr.size = QueueSize;
	ret = fi_cq_open(dom, &cqattr, &cq, NULL);
	if (ret != 0)
		goto error;

	memset(avattr, 0, sizeof(avattr));
	avattr.type = fi->domain_attr->av_type?fi->domain_attr->av_type : FI_AV_MAP;
	avattr.count = max_id;
	avattr.name = NULL;
	ret = fi_av_open(dom, &avattr, &av, NULL);
	if (ret != 0)
		goto error;

	ret = fi_ep_bind(ep, &cq->fid, FI_SEND|FI_WRITE|FI_RECV);
	if (ret != 0)
		goto error;

	ret = fi_ep_bind(ep, &av->fid, 0);
	if (ret != 0)
		goto error;

	ret = fi_enable(ep);
	if (ret != 0)
		goto error;

	// get rank address
	fi_getname(&ep->fid, NULL, &addrlen);
	addr = malloc(addrlen);
	ret = fi_getname(&ep->fid, addr, &addrlen);
	if (ret != 0)
		goto error;

	addrs = malloc(max_id * addrlen);
	PMI_Allgather(addr, addrs, addrlen);

	fi_addrs = malloc(max_id * sizeof(fi_addr_t));
	ret = fi_av_insert(av, addrs, max_id, fi_addrs, 0, &avctx);
	if (ret != max_id)
		goto error;
	free(addr);

	// post tagged message for message types without payloads
	for(std::vector<MessageType*>::iterator it = mts.begin(); it != mts.end(); ++mts) {
		mt = *it;
		if (!mt->payload) {
			ret = post_tagged(mt);
			if (ret != 0)
				goto error;
		}
	}

	// post few untagged buffers for message types with payloads
	for(int i = 0; i < pend_num; i++) {
		ret = post_untagged()
		if (ret != 0)
			goto error;
	}

	fi_freeinfo(hints);
	fi_freeinfo(fi);
	return true;

error:
	fi_freeinfo(hints);
	fi_freeinfo(fi);
	return false;
}

virtual FabFabric::~Fabric()
{
	shutdown();
}

virtual bool FabFabric::add_message_type(MessageType *mt)
{
	if (mt->id == 0 || mts[mt->id] != NULL)
		return false;

	mts[mt->id] = mt;
	return true;
}

virtual void FabFabric::shutdown()
{
	if (ep) {
		fi_close(ep->fid);
		ep = NULL;
	}

	if (av) {
		fi_close(av->fid);
		av = NULL;
	}

	if (cq) {
		fi_close(cq->fid);
		rcq = NULL;
	}

	if (eq) {
		fi_close(eq->fid);
		eq = NULL;
	}

	if (dom) {
		fi_close(dom->fid);
		dom = NULL;
	}

	if (fab) {
		fi_close(fab->fid);
		fab = NULL;
	}
}

virtual NodeId FabFabric::get_id()
{
	return id;
}

virtual NodeId FabFabric:get_max_id()
{
	return max_id;
}

virtual int FabFabric::send(NodeId dest, MessageId id, void *args, Payload *payload, bool inOrder)
{
	FabMessage *m;

	m = new FabMessage(id, args, payload);
	m->sndid = id;
	m->rcvid = dest;

	return send(m);
}

int FabFabric::send(FabMessage *m)
{
	int ret, e, n;
	MessageType *mt;
	struct iovec *iov;

	mt = m->mtype;
	if (mt == NULL)
		return -EINVAL;

	if (!m->mtype->payload) {
		ret = fi_tsend(ep, m->args, m->mtype->argsz, NULL, fi_addrs[m->rcvid], m->mtype->id, m);
		if (ret != 0)
			return ret;
	} else {
		n = 0;
		m->iov = &m->siov[0];
		pidx = m->mtype->argsz==0?1:2;
		if (m->payload) {
			size_t sz;
			void *buf;

			e = NELEM(m->siov) - pidx;
			n = m->payload->iovec(&m->iov[pidx], e);
			if (n >= 0 && n > e) {
				// the payload needs more elements
				m->iov = malloc((n + pidx) * size(struct iovec));
				n = m->payload->iovec(&m->iov[pidx], n);
			}

			if (n < 0)
				return n;
		}

		// TODO: make it network order???
		m->iov[0].iov_base = &m->mtype->id;
		m->iov[0].iov_len = sizeof(m->mtype->id);
		n += pidx;
		if (m->mtype->argsz != 0) {
			m->iov[1].iov_base = args;
			m->iov[1].iov_len = m->mtype->argsz;
		}

		ret = fi_send(ep, m->iov, NULL, n, fi_addrs[m->rcvid], m);
		if (ret != 0)
			return ret;
	}

	return 0;
}

virtual bool FabFabric::progress(int maxToSend, bool wait)
{
	int ret, timeout;
	struct fi_addr_t src;
	struct fi_cq_tagged_entry ce;
	FabMessage *m;

	timeout = wait?1000:0;
	while (1) {
		ret = fi_cq_sreadfrom(cq, &ce, 1, &src, NULL /* is this correct??? */, timeout);
		if (ret == 0 && !wait)
			break;

		if (ret < 0) {
			if (ret == -FI_EAGAIN && wait)
				continue;
			else if (ret == -FI_EAVAIL) {
				struct fi_cq_err_entry cqerr;
				const char *errstr;

				ret = fi_cq_readerr(cq, &cqerr, 0);
				if (ret != 0) {
					// TODO: fix
					fprintf(stderr, "unknown error: %d\n", ret);
				}

				// TODO: fix
				errstr = fi_cq_strerror(cq, cqerr.prov_errno, cqerr.err_data, NULL, 0);
				fprintf(stderr, "%d %s\n", cqerr.err, fi_strerror(cqerr.err));
				fprintf(stderr, "prov_err: %s (%d)\n", errstr, cqerr.prov_errno);
			} else {
				// TODO: fix
				fprintf(stderr, "unknown error: %d\n", ret);
			}
			break;
		}

		m = (Message *) ce.op_context;
		if (m->rcvid == get_id()) {
			// the message was received
			m->iov[0].len = ce.len;
			incoming(m);
			// TODO
		} else {
			// the message was sent
			delete m;
		}
	}
}

virtual bool FabFabric::incoming(Message *m)
{
	if (m->mtype != NULL) {
		// tagged message
		post_tagged(m->mtype);
	} else {
		MessageId mtype;
		char *data;

		// untagged message
		post_untagged();
		data = (char *) m->iov[0].base;
		msgid = *(MessageId *) &data;
		data += sizeof(mtype);
		mtype = fabric->mts[msgid];
		if (mtype == NULL) {
			fprintf(stderr, "invalid message type: %d\n", msgid);
			return false;
		}

		if (m->mtype->argsz > 0) {
			m->args = data;
			data += mt->argsz;
		}

		m->payload = new ContiguousPayload(PAYLOAD_KEEP, data, m->iov[0].len);
	}

	m->mtype->request(m);
	// Anything else?

	return true;
}

virtual void *FabFabric::memalloc(size_t size)
{
	return malloc(size);
}

virtual void FabFabric::memfree(void *a)
{
	free(a);
}

int FabFabric::post_tagged(mt *MessageType)
{
	Message *m;
	void *args;

	args = malloc(mt->argsz);
	m = new FabMessage(get_id(), mt->id, args, NULL, false);
	return fi_trecv(ep, args, mt->argsz, NULL, FI_ADDR_UNSPEC, mt->id, 0, m);
}

int FabFabric::post_untagged()
{
	void *buf = malloc(maxsend);

	m = new FabMessage(get_id(), 0, NULL, NULL, false);
	m->iov[0].base = buf;
	m->iov[0].len = maxsend;
	return fi_recv(ep, buf, maxsend, NULL, FI_ADDR_UNSPEC, m);
}
