#include "fabric_libfabric.h"

FabMessage::FabMessage(NodeId dest, MessageId id, void *args, Payload *payload, bool inOrder)
  : Message(id, args, payload)
{
  mtype = fabric->mts[id];
  rcvid = dest;
}

FabMessage::~FabMessage()
{
  if (iov != siov)
    delete iov;

  delete payload;
}

int FabMessage::reply(MessageId id, void *args, Payload *payload, bool inOrder)
{
  FabMessage *r = new FabMessage(sndid, id, args, payload, inOrder);
  return fabric->send(r);
}

FabFabric::FabFabric():max_send(1024*1024), pend_num(16)
{
  // ASK -- need to sort out setting up PMI
  // PMI_Get_size(&max_id);
  // PMI_Get_rank(&id);

  
  
}

void FabFabric::register_options(Realm::CommandLineParser &cp)
{
  cp.add_option_int("-ll:max_send", max_send);
  cp.add_option_int("-ll:pend_num", pend_num);
}

bool FabFabric::init()
{
  struct fi_info *hints, *fi;
  struct fi_cq_attr cqattr;
  struct fi_eq_attr eqattr;
  struct fi_av_attr avattr;
  size_t addrlen;

  hints = fi_allocinfo();
  hints->ep_attr->type = FI_EP_RDM;
  hints->caps = FI_TAGGED | FI_MSG | FI_DIRECTED_RECV | FI_RMA;
  hints->mode = FI_CONTEXT | FI_LOCAL_MR;
  hints->domain_attr->mr_mode = FI_MR_BASIC;

  fi = NULL;
  int ret = fi_getinfo(FI_VERSION(1, 0), NULL, NULL, 0, hints, &fi);
  if (ret != 0)
    init_fail(hints, fi);
  
  ret = fi_fabric(fi->fabric_attr, (struct fid_fabric**) &fab, NULL);
  if (ret != 0)
    init_fail(hints, fi);

  std::memset(&eqattr, 0, sizeof(eqattr));
  eqattr.size = FI_WAIT_UNSPEC;
  ret = fi_eq_open(fab, &eqattr, (struct fid_eq**) &eq, NULL);
  if (ret != 0)
    init_fail(hints, fi);

  ret = fi_domain(fab, fi, (struct fid_domain**) &dom, NULL);
  if (ret != 0)
    init_fail(hints, fi);


  ret = fi_endpoint(dom, fi, (struct fid_ep**) &ep, NULL);
  if (ret != 0)
    init_fail(hints, fi);

  ret = fi_ep_bind(ep, &(eq->fid), 0);
  if (ret != 0)
    init_fail(hints, fi);

  std::memset(&cqattr, 0, sizeof(cqattr));
  cqattr.format = FI_CQ_FORMAT_TAGGED;
  cqattr.wait_obj = FI_WAIT_UNSPEC;
  cqattr.wait_cond = FI_CQ_COND_NONE;
  // ASK -- what should the queue size be? It's not defined
  cqattr.size = 0;

  
  ret = fi_cq_open(dom, &cqattr, (struct fid_cq**) &cq, NULL);
  if (ret != 0)
    init_fail(hints, fi);

  memset(&avattr, 0, sizeof(avattr));
  avattr.type = fi->domain_attr->av_type?fi->domain_attr->av_type : FI_AV_MAP;
  avattr.count = max_id;
  avattr.name = NULL;
  ret = fi_av_open(dom, &avattr, (struct fid_av**) &av, NULL);
  if (ret != 0)
    init_fail(hints, fi);

  ret = fi_ep_bind(ep, &(cq->fid), FI_SEND|FI_WRITE|FI_RECV);
  if (ret != 0)
    init_fail(hints, fi);

  ret = fi_ep_bind(ep, &(av->fid), 0);
  if (ret != 0)
    init_fail(hints, fi);

  ret = fi_enable(ep);
  if (ret != 0)
    init_fail(hints, fi);

  // get rank address
  fi_getname(&(ep->fid), NULL, &addrlen);
  void* addr = malloc(addrlen);
  ret = fi_getname(&(ep->fid), addr, &addrlen);
  if (ret != 0)
    init_fail(hints, fi);

  void* addrs = malloc(max_id * addrlen);
  // ASK -- I'm not sure what PMI_Allgather is or where it came from.
  //PMI_Allgather(addr, addrs, addrlen);

  fi_addrs = (fi_addr_t*) malloc(max_id * sizeof(fi_addr_t));
  ret = fi_av_insert(av, addrs, max_id, fi_addrs, 0, &avctx);
  if (ret != max_id)
    return init_fail(hints, fi);
  free(addr);

  // post tagged message for message types without payloads
  for(std::vector<MessageType*>::iterator it = mts.begin(); it != mts.end(); ++it) {
    MessageType* mt = *it;
    if (!mt->payload) {
      ret = post_tagged(mt);
      if (ret != 0)
	return init_fail(hints, fi);
    }
  }

  // post few untagged buffers for message types with payloads
  for(int i = 0; i < pend_num; i++) {
    ret = post_untagged();
      if (ret != 0)
	return init_fail(hints, fi);
  }

  fi_freeinfo(hints);
  fi_freeinfo(fi);
  return true;

  //error:
  //fi_freeinfo(hints);
  //fi_freeinfo(fi);
  //return false;
}


bool FabFabric::init_fail(fi_info* hints, fi_info* fi)
{
  fi_freeinfo(hints);
  fi_freeinfo(fi);
  return false;
}

FabFabric::~FabFabric()
{
  shutdown();
}

bool FabFabric::add_message_type(MessageType *mt)
{
  if (mt->id == 0 || mts[mt->id] != NULL)
    return false;

  mts[mt->id] = mt;
  return true;
}

void FabFabric::shutdown()
{
  fi_close(&(ep->fid));
  fi_close(&(av->fid));
  fi_close(&(cq->fid));
  fi_close(&(eq->fid));
  fi_close(&(dom->fid));
  fi_close(&(fab->fid)); 
}

NodeId FabFabric::get_id()
{
  return id;
}

NodeId FabFabric::get_max_id()
{
  return max_id;
}

int FabFabric::send(NodeId dest, MessageId id, void *args, Payload *payload, bool inOrder)
{
  FabMessage *m;

  m = new FabMessage(dest, id, args, payload, inOrder);
  m->sndid = id;
  m->rcvid = dest;

  return send(m);
}

int FabFabric::send(Message* m)
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
    int pidx = m->mtype->argsz==0 ? 1 : 2;
    if (m->payload) {
      size_t sz;
      void *buf;

      e = NELEM(m->siov) - pidx;
      n = m->payload->iovec(&m->iov[pidx], e);
      if (n >= 0 && n > e) {
	// the payload needs more elements
	m->iov = (struct iovec *) malloc((n + pidx) * sizeof(struct iovec));
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
      m->iov[1].iov_base = m->args; // CHECK -- not sure if this is the correct args
      m->iov[1].iov_len = m->mtype->argsz;
    }

    ret = fi_send(ep, m->iov, n, NULL, fi_addrs[m->rcvid], m);
    if (ret != 0)
      return ret;
  }

  return 0;
}

bool FabFabric::progress(int maxToSend, bool wait)
{
  int ret, timeout;
  fi_addr_t src;
  fi_cq_tagged_entry ce;
  FabMessage *m;

  timeout = wait ? 1000 : 0;
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

    m = (FabMessage *) ce.op_context;
    if (m->rcvid == get_id()) {
      // the message was received
      m->iov[0].iov_len = ce.len;
      incoming(m);
      // TODO
    } else {
      // the message was sent
      delete m;
    }
  }
  
  return true;
}

bool FabFabric::incoming(FabMessage *m)
{
  if (m->mtype != NULL) {
    // tagged message
    post_tagged(m->mtype);
  } else {
    MessageType* mtype;
    MessageId msgid;
    char *data;

    // untagged message
    post_untagged();
    data = (char *) m->iov[0].iov_base;
    // CHECK -- does this conversion actually work?
    msgid = *(MessageId *) &data;
    data += sizeof(mtype);
    mtype = fabric->mts[msgid];
    if (mtype == NULL) {
      fprintf(stderr, "invalid message type: %d\n", msgid);
      return false;
    }

    if (m->mtype->argsz > 0) {
      m->args = data;
      data += mtype->argsz;
    }

    m->payload = new ContiguousPayload(PAYLOAD_KEEP, data, m->iov[0].iov_len);
  }

  m->mtype->request(m);
  // Anything else?

  return true;
}

void *FabFabric::memalloc(size_t size)
{
  return malloc(size);
}

void FabFabric::memfree(void *a)
{
  free(a);
}

int FabFabric::post_tagged(MessageType* mt)
{
  Message *m;
  void *args;

  args = malloc(mt->argsz);
  m = new FabMessage(get_id(), mt->id, args, NULL, false);
  return fi_trecv(ep, args, mt->argsz, NULL, FI_ADDR_UNSPEC, mt->id, 0, m);
}

int FabFabric::post_untagged()
{
  void *buf = malloc(max_send);

  FabMessage* m = new FabMessage(get_id(), 0, NULL, NULL, false);
  m->iov[0].iov_base = buf;
  m->iov[0].iov_len = max_send;
  return fi_recv(ep, buf, max_send, NULL, FI_ADDR_UNSPEC, m);
}


FabAutoLock::~FabAutoLock()
{
  if (held)
    mutex.unlock();
}

void FabAutoLock::release()
{
  assert(held);
  held = false;
  mutex.unlock();
}

void FabAutoLock::reacquire()
{
  mutex.lock();
  held = true;
}

