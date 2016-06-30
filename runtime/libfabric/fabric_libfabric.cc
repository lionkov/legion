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

/*
int FabMessage::reply(MessageId id, void *args, Payload *payload, bool inOrder)
{
  FabMessage *r = new FabMessage(sndid, id, args, payload, inOrder);
  return fabric->send(r);
}
*/

FabFabric::FabFabric() : max_send(1024*1024), pend_num(16),
			 num_progress_threads(0), progress_threads(NULL) {
  for (int i = 0; i < MAX_MESSAGE_TYPES; ++i)
    mts[i] = NULL;
  atomic_init(&stop_atomic_flag, false);
}

void FabFabric::register_options(Realm::CommandLineParser &cp)
{
  cp.add_option_int("-ll:max_send", max_send);
  cp.add_option_int("-ll:pend_num", pend_num);
}

/*
  FabFabric::init():
   
   YOU MUST REGISTER ALL DESIRED MESSAGE TYPES BEFORE CALLING 
   THIS FUNCTION.

   Inputs: none

   Returns: true on success, false on failure.

   Initializes PMI and discovers rest of network. 

   Then initialalizes fabric communication system and posts
   buffers for all message types. 
*/ 

bool FabFabric::init() {
  
  // Initialize PMI and discover other nodes
  int ret;
  int spawned;
  
  ret = PMI_Init(&spawned);
  if (ret != PMI_SUCCESS) {
    std::cout << "ERROR -- PMI_Init failed with error code " << ret << std::endl;
    //    return false;
  }

  ret = PMI_Get_size((int*) &max_id);
  if (ret != PMI_SUCCESS) {
    std::cout << "ERROR -- PMI_Get_size failed with error code " << ret << std::endl;
    //return false;
  }

  ret = PMI_Get_rank((int*) &id);
  if (ret != PMI_SUCCESS) {
    std::cout << "ERROR -- PMI_Get_rank failed with error code " << ret << std::endl;
    //return false;
  }

  // Setting max_id to 1 for now, since PMI isn't setting up properly
  max_id = 1;
  
  // Initialize fabric
  struct fi_info *hints, *fi;
  struct fi_cq_attr cqattr; memset(&cqattr, 0, sizeof(cqattr));
  struct fi_eq_attr eqattr; memset(&eqattr, 0, sizeof(eqattr));
  struct fi_av_attr avattr; memset(&avattr, 0, sizeof(avattr));
  struct fi_cntr_attr cntrattr; memset(&cntrattr, 0, sizeof(avattr));

  hints = fi_allocinfo();
  hints->ep_attr->type = FI_EP_RDM;
  hints->caps = FI_TAGGED | FI_MSG | FI_DIRECTED_RECV | FI_RMA;
  hints->mode = FI_CONTEXT | FI_LOCAL_MR;
  hints->domain_attr->mr_mode = FI_MR_BASIC;
  //hints->fabric_attr->prov_name = strdup("psm2");
  // Temporary -- looping back to localhost
  //hints->addr_format = FI_SOCKADDR_IN;
  //char* src_addr_str = "127.0.0.1";

  fi = NULL;
  ret = fi_getinfo(FI_VERSION(1, 0), NULL, NULL, 0, hints, &fi);
  if (ret != 0)
    return init_fail(hints, fi, fi_error_str(ret, "fi_getinfo", __FILE__, __LINE__));

  
  ret = fi_fabric(fi->fabric_attr, (struct fid_fabric**) &fab, NULL);
  if (ret != 0)
    return init_fail(hints, fi, fi_error_str(ret, "fi_fabric", __FILE__, __LINE__));

  std::memset(&eqattr, 0, sizeof(eqattr));
  //eqattr.size = FI_WAIT_UNSPEC;
  eqattr.size = 64;
  eqattr.wait_obj = FI_WAIT_UNSPEC;
  ret = fi_eq_open(fab, &eqattr, (struct fid_eq**) &eq, NULL);
  if (ret != 0)
    return init_fail(hints, fi, fi_error_str(ret, "fi_eq_open", __FILE__, __LINE__));

  ret = fi_domain(fab, fi, (struct fid_domain**) &dom, NULL);
  if (ret != 0)
    return init_fail(hints, fi, fi_error_str(ret, "fi_domain", __FILE__, __LINE__));

  ret = fi_endpoint(dom, fi, (struct fid_ep**) &ep, NULL);
  if (ret != 0)
    return init_fail(hints, fi, fi_error_str(ret, "fi_endpoint", __FILE__, __LINE__));

  std::memset(&cqattr, 0, sizeof(cqattr));
  cqattr.format = FI_CQ_FORMAT_TAGGED;
  cqattr.wait_obj = FI_WAIT_UNSPEC;
  cqattr.wait_cond = FI_CQ_COND_NONE;
  // ASK -- what should the queue size be? It's not defined
  cqattr.size = 100;
  
  ret = fi_cq_open(dom, &cqattr, (struct fid_cq**) &cq, NULL);
  if (ret != 0)
    return init_fail(hints, fi, fi_error_str(ret, "fi_cq_open", __FILE__, __LINE__));

  std::memset(&cntrattr, 0, sizeof(cntrattr));
  cntrattr.events = FI_CNTR_EVENTS_COMP;
  cntrattr.wait_obj = FI_WAIT_UNSPEC;
  cntrattr.flags = 0;

  ret = fi_cntr_open(dom, &cntrattr, (struct fid_cntr**) &cntr, NULL);
  if (ret != 0)
      return init_fail(hints, fi, fi_error_str(ret, "fi_cntr_open", __FILE__, __LINE__));

  struct { char rar[16]; } addr;
  size_t addrlen;
  addrlen = sizeof(addr);
  
  ret = fi_getname((fid_t) ep, &addr, &addrlen);
  if (ret != 0)
    return init_fail(hints, fi, fi_error_str(ret, "fi_getname", __FILE__, __LINE__));

  std::memset(&avattr, 0, sizeof(avattr));
  //avattr.type = fi->domain_attr->av_type?fi->domain_attr->av_type : FI_AV_MAP;
  avattr.type = FI_AV_MAP;
  avattr.count = max_id;
  avattr.ep_per_node = 0; // 'unknown' number of endpoints, may be optimized later
  avattr.name = NULL;
  
  ret = fi_av_open(dom, &avattr, (struct fid_av**) &av, NULL);
  if (ret != 0) 
    return init_fail(hints, fi, fi_error_str(ret, "fi_av_open", __FILE__, __LINE__));

  ret = fi_ep_bind(ep, (fid_t) eq, 0);
  if (ret != 0)
    return init_fail(hints, fi, fi_error_str(ret, "fi_ep_bind", __FILE__, __LINE__));

  ret = fi_ep_bind(ep, (fid_t) cq, FI_TRANSMIT|FI_RECV);
  if (ret != 0)
    return init_fail(hints, fi, fi_error_str(ret, "fi_ep_bind", __FILE__, __LINE__));

  ret = fi_ep_bind(ep, (fid_t) cntr, FI_READ|FI_WRITE|FI_SEND|FI_RECV);
  if (ret != 0)
    return init_fail(hints, fi, fi_error_str(ret, "fi_ep_bind", __FILE__, __LINE__));

  ret = fi_ep_bind(ep, (fid_t) av, 0);
  if (ret != 0) 
    return init_fail(hints, fi, fi_error_str(ret, "fi_ep_bind", __FILE__, __LINE__));
  
  fi_addrs = (fi_addr_t*) malloc(max_id * sizeof(fi_addr_t));
  memset(fi_addrs, 0, sizeof(fi_addr_t)*max_id);
  // inserting only this address for now, since PMI_Allgather is not working
  ret = fi_av_insert(av, &addr, 1, fi_addrs, 0, NULL);
  if (ret <= 0) 
    return init_fail(hints, fi, fi_error_str(ret, "fi_av_insert", __FILE__, __LINE__));
  
  ret = fi_enable(ep);
  if (ret != 0)
    return init_fail(hints, fi, fi_error_str(ret, "fi_enable", __FILE__, __LINE__));


  /*
  // void* addrs = malloc(max_id * addrlen);

  // ASK -- most pmi.h implementations do not have PMI_Allgather,
  // do we really need this?
  
  //PMI_Allgather(addr, addrs, addrlen);

  // Hard code this node as fi_addrs[0], since PMI_Allgather isn't working yet
  //memcpy(&addrs, &addr, addrlen);
  //std::cout << (unsigned long) addrs[0] << std::endl;
  std::cout << addr << std::endl;
  std::cout << max_id << std::endl;

  // Temporyary buffer for addresses
  uint8_t addrbuf[4096];
  fi_addr_t fi_addr;
  int buflen = sizeof(addrbuf);
  //ret = av_create_address_list(src_addr_str, 0, 1, addrbuf, 0, buflen, addrlen);
  
  if (ret < 0)
    return init_fail(hints, fi, "ERROR -- av_create_address_list failed");
  
  if (!fi_addrs)
    return init_fail(hints, fi, "ERROR -- malloc fi_addrs failed");
  
  ret = fi_av_insert(av, addrbuf, max_id, fi_addrs, 0, &avctx);
  // Original code checked for number of entries inserted; as far as I can
  // tell fabric does not return this info
  if (ret < 0) 
    return init_fail(hints, fi, fi_error_str(ret, "fi_av_insert", __FILE__, __LINE__));
  free(addr);
  */

  // post tagged message for message types without payloads
  for(int i = 0; i < MAX_MESSAGE_TYPES; ++i) {
    MessageType* mt = mts[i];
    if (mt && !mt->payload) {
      ret = post_tagged(mt);
      if (ret != 0)
	return init_fail(hints, fi, fi_error_str(ret, "post_tagged", __FILE__, __LINE__));
    }
  }

  // post few untagged buffers for message types with payloads
  for(int i = 0; i < pend_num; i++) {
    ret = post_untagged();
    if (ret != 0)
      return init_fail(hints, fi, fi_error_str(ret, "post_untagged", __FILE__, __LINE__));
  }

  fi_freeinfo(hints);
  fi_freeinfo(fi);

  //start_progress_threads(1, 0);
  
  return true;

  //error:
  //fi_freeinfo(hints);
  //fi_freeinfo(fi);
  //return false;
}


bool FabFabric::init_fail(fi_info* hints, fi_info* fi, std::string message) {
  
  std::cerr << message << std::endl;
  std::cerr << "ERROR -- Fabric Init failed. " << std::endl;
    
  fi_freeinfo(hints);
  fi_freeinfo(fi);

  return false;
}

FabFabric::~FabFabric()
{
  free_progress_threads();
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
    ret = fi_tsend(ep, m->args, m->mtype->argsz, NULL,
		   fi_addrs[m->rcvid],
		   m->mtype->id, m);
    if (ret != 0) {
      std::cerr << fi_error_str(ret, "", "", 0);
      return ret;
    }
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

bool FabFabric::progress(bool wait)
{
  fprintf(stderr, "made a progress thread... \n");
  
  int ret, timeout;
  fi_addr_t src;
  fi_cq_tagged_entry ce;
  FabMessage *m;

  
  timeout = wait ? 1000 : 0;
  while (atomic_load(&stop_atomic_flag) == false) {
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

  std::cout << "Progress thread shutting down. " << std::endl;
  return true;
}

// For launching progress from Pthreads
void* FabFabric::bootstrap_progress(void* context) {
  ((FabFabric*) context)->progress(true);
  return 0;
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
  m->siov[0].iov_base = buf;
  m->siov[0].iov_len = max_send;
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

void FabFabric::print_fi_info(fi_info* fi) {
  fi_info* head = fi;
  while(head) {
    std::cout << "caps: " << head->caps << std::endl;
    std::cout << "mode: " << head->mode << std::endl;
    std::cout << "addr_format: " << head->addr_format << std::endl;
    std::cout << "src_addrlen: " << head->src_addrlen << std::endl;
    std::cout << "dest_addrlen: " << head->dest_addrlen << std::endl;

    head = head->next;
  }
}

// This is a temporary solution -- final version should use Legion's
// Create_kernel_thread methods

// The stack_size parameter is currently not used (again this will wait
// until integration with Legion runtime)

void FabFabric::start_progress_threads(int count, size_t stack_size) {
  num_progress_threads = count;
  progress_threads = new pthread_t[count];
  for (int i = 0; i < count; ++i) {
    pthread_create(&progress_threads[i], NULL, &FabFabric::bootstrap_progress, this);
  }
}

void FabFabric::free_progress_threads() {
  atomic_store(&stop_atomic_flag, true);
  for (int i = 0; i < num_progress_threads; ++i) 
    pthread_join(progress_threads[i], NULL);
  if (progress_threads)
    delete[] progress_threads;
}

// For testing purposes -- just wait for the progress threads to complete.
void FabFabric::wait_for_shutdown() {
  for (int i = 0; i < num_progress_threads; ++i)
    pthread_join(progress_threads[i], NULL);
  std::cout << "OK, all threads done" << std::endl;
}


/* Return error for a fabric completion queue */
std::string FabFabric::fi_cq_error_str(int ret, fid_cq* cq) {
  
  std::stringstream sstream;
  struct fi_cq_err_entry err;
  fi_cq_readerr(cq, &err, 0);
  
  sstream << "FABRIC ERROR " << ret << ": "
	  << fi_strerror(err.err) << " "
	  << fi_cq_strerror(cq, err.prov_errno, err.err_data, NULL, 0) << "\n";

  return sstream.str();
}


std::string FabFabric::fi_error_str(int ret, std::string call, std::string file, int line) {
  std::stringstream sstream;

  sstream << "ERROR " << -ret << " in "<< call << "() at "
	  << file << ":" << line << " -- "
	  << fi_strerror(-ret);

  return sstream.str();
}

/*
 * Create an address list
 */

int FabFabric::av_create_address_list(char *first_address, int base, int num_addr,
				      void *addr_array, int offset, int len, int addrlen)
{
	uint8_t *cur_addr;
	int ret;
	int i;
	// Assume format is FI_SOCKADDR
	if (len < addrlen * (offset + num_addr)) {
	  fprintf(stderr, "internal error, not enough room for %d addresses",
		  num_addr);
	  return -FI_ENOMEM;
	}

	cur_addr = (uint8_t*) addr_array;
	cur_addr += offset * addrlen;
	for (i = 0; i < num_addr; ++i) {
		ret = add_address(first_address, base + i, cur_addr);
		if (ret != 0) {
			return ret;
		}
		cur_addr += addrlen;
	}

	return cur_addr - (uint8_t *)addr_array;
}


int FabFabric::add_address(char* first_address, int index, void* addr) {
  
  	struct addrinfo hints;
	struct addrinfo *ai;
	struct sockaddr_in *sin;
	uint32_t tmp;
	int ret;

	memset(&hints, 0, sizeof(hints));

	/* return all 0's for invalid address */
	if (first_address == NULL) {
		memset(addr, 0, sizeof(*sin));
		return 0;
	}

	hints.ai_family = AF_INET;
	/* port doesn't matter, set port to discard port */
	ret = getaddrinfo(first_address, "discard", &hints, &ai);
	if (ret != 0) {
		fprintf(stderr, "getaddrinfo: %s", gai_strerror(ret));
		return -1;
	}

	sin = (struct sockaddr_in *)addr;
	*sin = *(struct sockaddr_in *)ai->ai_addr;

	tmp = ntohl(sin->sin_addr.s_addr);
	tmp += index;
	sin->sin_addr.s_addr = htonl(tmp);

	freeaddrinfo(ai);
	return 0;

  
}
