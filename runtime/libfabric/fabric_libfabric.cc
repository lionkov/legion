#include "fabric_libfabric.h"

// run
/*
  int Message::reply(MessageId id, void *args, Payload *payload, bool inOrder)
  {
  Message *r = new Message(sndid, id, args, payload, inOrder);
  return fabric->send(r);
  }
*/

FabFabric::FabFabric() : id(0), num_nodes(1), max_send(1024*1024), pend_num(16),
			 num_progress_threads(1),
			 progress_threads(NULL),
			 tx_handler_thread(NULL),
			 stacksize_in_mb(32),
			 stop_flag(false),
			 exchange_server_send_port(8080),
			 exchange_server_recv_port(8081),
			 exchange_server_host("127.0.0.1") {
  done_mutex.lock();
  for (int i = 0; i < MAX_MESSAGE_TYPES; ++i)
    mts[i] = NULL;
}

void FabFabric::register_options(Realm::CommandLineParser &cp)
{
  cp.add_option_int("-ll:max_send", max_send);
  cp.add_option_int("-ll:pend_num", pend_num);
  cp.add_option_int("-ll:num_nodes", num_nodes);
  cp.add_option_int("-ll:exchange_server_send_port", exchange_server_send_port);
  cp.add_option_int("-ll:exchange_server_recv_port", exchange_server_recv_port);
  cp.add_option_string("-ll:exchange_server_host", exchange_server_host);

  // progress / cleanup thread options
  cp.add_option_int("-ll:stacksize", stacksize_in_mb);
  cp.add_option_int("-ll:handlers", num_progress_threads);
}

/* 
   FabFabric::setup_pmi()

   Query PMI to find other nodes othe network. Not currently working. 
   
   Will set id and num_nodes. These fields will not be valid if this function fails.

   Returns 0 on success, -1 on failure.

   Currently PMI isn't working, so just hard code these values.
*/

/*
  int FabFabric::setup_pmi() {
  
  // Initialize PMI and discover other nodes
  int ret, spawned;
  
  ret = PMI_Init(&spawned);

  
  if (ret != PMI_SUCCESS) {
  std::cerr << "ERROR -- PMI_Init failed with error code " << ret << std::endl;
  return -1;
  }
  
  // Discover number of nodes, record in num_nodes
  ret = PMI_Get_size((int*) &num_nodes);
  if (ret != PMI_SUCCESS) {
  std::cerr << "ERROR -- PMI_Get_size failed with error code " << ret << std::endl;
  return -1;
  }

  // Discover ID of this node, record in id
  ret = PMI_Get_rank((int*) &id);
  if (ret != PMI_SUCCESS) {
  std::cerr << "ERROR -- PMI_Get_rank failed with error code " << ret << std::endl;
  return -1;
  }

  return 1;
  }
*/

/*
  FabFabric::init():
   
  YOU MUST REGISTER ALL DESIRED MESSAGE TYPES BEFORE CALLING 
  THIS FUNCTION.

  Inputs: manually_set_addresses -- if true, register this node only.
  You must use the set_address_vector call to manually pass in other addresses
  before using this fabric.

  Returns: true on success, false on failure.

  Initializes PMI and discovers rest of network. 

  Then initialalizes fabric communication system and posts
  buffers for all message types. 
*/ 

bool FabFabric::init(bool manually_set_addresses) {

  int ret;

  // QUERY PMI
  /*
    ret = setup_pmi();
    if (ret != 0) {
    std::cerr << "ERROR -- could not query PMI to determine network properties" << std::endl;
    return false;
    }*/

  // Add internal message types
  add_message_type(new EventGatherMessageType(), "Event Gather Message");
  add_message_type(new EventBroadcastMessageType(), "Event Broadcast Message");
  add_message_type(new BarrierNotifyMessageType(), "Barrier Notify Message");

  
  std::cout << "Initializing fabric... " << std::endl;

  // Init collective objects
  event_gatherer.init(num_nodes);
  barrier_waiter.init(num_nodes);
  
  struct fi_info *hints;
  struct fi_cq_attr rx_cqattr; memset(&rx_cqattr, 0, sizeof(rx_cqattr));
  struct fi_cq_attr tx_cqattr; memset(&tx_cqattr, 0, sizeof(tx_cqattr));
  struct fi_eq_attr eqattr; memset(&eqattr, 0, sizeof(eqattr));
  struct fi_av_attr avattr; memset(&avattr, 0, sizeof(avattr));
  struct fi_cntr_attr cntrattr; memset(&cntrattr, 0, sizeof(avattr));

  // SETUP HINTS
  hints = fi_allocinfo();
  hints->ep_attr->type = FI_EP_RDM; // should be RDM, but maybe MSG will work with socks
  hints->caps = FI_TAGGED | FI_MSG | FI_DIRECTED_RECV | FI_RMA;
  hints->mode = FI_CONTEXT | FI_LOCAL_MR;
  hints->domain_attr->mr_mode = FI_MR_BASIC;
  hints->addr_format = FI_FORMAT_UNSPEC;

  // SETUP FABRIC
  ret = fi_getinfo(FI_VERSION(1, 0), NULL, NULL, 0, hints, &fi);
  if (ret != 0)
    return init_fail(hints, fi, fi_error_str(ret, "fi_getinfo", __FILE__, __LINE__));
  
  ret = fi_fabric(fi->fabric_attr, (struct fid_fabric**) &fab, NULL);
  if (ret != 0)
    return init_fail(hints, fi, fi_error_str(ret, "fi_fabric", __FILE__, __LINE__));

  std::cout << "Creating fabric: \n" << tostr() << std::endl;


  // SETUP EQ
  //eqattr.size = FI_WAIT_UNSPEC;
  eqattr.size = 64;
  eqattr.wait_obj = FI_WAIT_UNSPEC;
  ret = fi_eq_open(fab, &eqattr, (struct fid_eq**) &eq, NULL);
  if (ret != 0)
    return init_fail(hints, fi, fi_error_str(ret, "fi_eq_open", __FILE__, __LINE__));

  // SETUP DOMAIN, EP
  ret = fi_domain(fab, fi, (struct fid_domain**) &dom, NULL);
  if (ret != 0)
    return init_fail(hints, fi, fi_error_str(ret, "fi_domain", __FILE__, __LINE__));

  ret = fi_endpoint(dom, fi, (struct fid_ep**) &ep, NULL);
  if (ret != 0)
    return init_fail(hints, fi, fi_error_str(ret, "fi_endpoint", __FILE__, __LINE__));

  // SETUP CQS FOR TX AND RX
  tx_cqattr.format = FI_CQ_FORMAT_TAGGED;
  tx_cqattr.wait_obj = FI_WAIT_UNSPEC;
  tx_cqattr.wait_cond = FI_CQ_COND_NONE;
  // ASK -- what should the queue size be? It's not defined
  tx_cqattr.size = fi->tx_attr->size;
  
  ret = fi_cq_open(dom, &tx_cqattr, (struct fid_cq**) &tx_cq, NULL);
  if (ret != 0)
    return init_fail(hints, fi, fi_error_str(ret, "fi_cq_open", __FILE__, __LINE__));
  
  rx_cqattr.format = FI_CQ_FORMAT_TAGGED;
  rx_cqattr.wait_obj = FI_WAIT_UNSPEC;
  rx_cqattr.wait_cond = FI_CQ_COND_NONE;
  // ASK -- what should the queue size be? It's not defined
  rx_cqattr.size = fi->rx_attr->size;
  
  ret = fi_cq_open(dom, &rx_cqattr, (struct fid_cq**) &rx_cq, NULL);
  if (ret != 0)
    return init_fail(hints, fi, fi_error_str(ret, "fi_cq_open", __FILE__, __LINE__));
  
  // SETUP COUNTER
  cntrattr.events = FI_CNTR_EVENTS_COMP;
  cntrattr.wait_obj = FI_WAIT_UNSPEC;
  cntrattr.flags = 0;

  ret = fi_cntr_open(dom, &cntrattr, (struct fid_cntr**) &cntr, NULL);
  if (ret != 0)
    return init_fail(hints, fi, fi_error_str(ret, "fi_cntr_open", __FILE__, __LINE__));

  //avattr.type = fi->domain_attr->av_type?fi->domain_attr->av_type : FI_AV_MAP;
  avattr.type = FI_AV_MAP;
  avattr.count = num_nodes;
  avattr.ep_per_node = 0; // 'unknown' number of endpoints, may be optimized later
  avattr.name = NULL;
  
  ret = fi_av_open(dom, &avattr, (struct fid_av**) &av, NULL);
  if (ret != 0) 
    return init_fail(hints, fi, fi_error_str(ret, "fi_av_open", __FILE__, __LINE__));

  // BIND EP TO EQ, CQs, CNTR, AV
  ret = fi_ep_bind(ep, (fid_t) eq, 0);
  if (ret != 0)
    return init_fail(hints, fi, fi_error_str(ret, "fi_ep_bind", __FILE__, __LINE__));
  
  ret = fi_ep_bind(ep, (fid_t) tx_cq, FI_TRANSMIT);
  if (ret != 0)
    return init_fail(hints, fi, fi_error_str(ret, "fi_ep_bind", __FILE__, __LINE__));
  
  ret = fi_ep_bind(ep, (fid_t) rx_cq, FI_RECV);
  if (ret != 0)
    return init_fail(hints, fi, fi_error_str(ret, "fi_ep_bind", __FILE__, __LINE__));
  
  ret = fi_ep_bind(ep, (fid_t) cntr, FI_READ|FI_WRITE|FI_SEND|FI_RECV);
  if (ret != 0)
    return init_fail(hints, fi, fi_error_str(ret, "fi_ep_bind", __FILE__, __LINE__));

  ret = fi_ep_bind(ep, (fid_t) av, 0);
  if (ret != 0) 
    return init_fail(hints, fi, fi_error_str(ret, "fi_ep_bind", __FILE__, __LINE__));

  ret = fi_av_bind(av, (fid_t) eq, 0);
  if (ret != 0) 
    return init_fail(hints, fi, fi_error_str(ret, "fi_ep_bind", __FILE__, __LINE__));

  ret = fi_enable(ep);
  if (ret != 0)
    return init_fail(hints, fi, fi_error_str(ret, "fi_enable", __FILE__, __LINE__));

 
  // GET ADDRESS FOR THIS NODE
  memset(addr, 0, sizeof(addr));
  addrlen = 64;

  // getname must be called after fi_enable
  // this call will set addr, and update addrlen to reflect the true address length
  ret = fi_getname((fid_t) ep, &addr, &addrlen);
  if (ret != 0)
    return init_fail(hints, fi, fi_error_str(ret, "fi_getname", __FILE__, __LINE__));
  
  // GET ADDRESSES AND INSERT INTO AV
  fi_addrs = (fi_addr_t*) malloc(num_nodes * sizeof(fi_addr_t));
  memset(fi_addrs, 0, sizeof(fi_addr_t*)*num_nodes);
  void* addrs;
  
  if(!manually_set_addresses) {
    // Contact the address change server, wait for all other nodes to post
    // their addresses, and load results into addrs array:
    std::cout << "Exchanging addresses... " << std::endl;
    addrs = exchange_addresses();
    if (ret < 0)
      return init_fail(hints, fi, "address exchange failed");
  } else {
    // Insert only this node's address. The complete AV will need to be set later
    // using set_address_vector.
    num_nodes = 1;
    std::cout << "WARNING -- address vector not set. You must set it manually to access other fabrics"
	      << std::endl;
    addrs = (void*) malloc(addrlen);
    memcpy(addrs, addr, addrlen);
  }
  
  // Load addresses into AV
  ret = fi_av_insert(av, addrs, num_nodes, fi_addrs, 0, NULL);
  if (ret < 0) 
    return init_fail(hints, fi, fi_error_str(ret, "fi_av_insert", __FILE__, __LINE__));
 
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
  free(addrs);
 
  start_progress_threads(num_progress_threads, 0);
  
  return true;
}


bool FabFabric::init_fail(fi_info* hints, fi_info* fi, const std::string message) const {
  std::cerr << message << std::endl;
  std::cerr << "ERROR -- Fabric Init failed. " << std::endl;
  
    
  fi_freeinfo(hints);
  fi_freeinfo(fi);

  return false;
}

FabFabric::~FabFabric()
{
  shutdown();
  if(fi_addrs)
    free(fi_addrs);
}

bool FabFabric::add_message_type(MessageType *mt, const std::string tag)
{
  log_fabric().debug("registered message type: %s", tag.c_str());

  
  if (mt->id == 0 || mts[mt->id] != NULL)
    return false;

  mts[mt->id] = mt;
  return true;
}

void FabFabric::shutdown()
{
  free_progress_threads();
  fi_close(&(ep->fid));
  fi_close(&(av->fid));
  fi_close(&(tx_cq->fid));
  fi_close(&(rx_cq->fid));
  fi_close(&(eq->fid));
  fi_close(&(dom->fid));
  fi_close(&(fab->fid));
  done_mutex.unlock();
}

NodeId FabFabric::get_id()
{
  return id;
}

uint32_t FabFabric::get_num_nodes()
{
  return num_nodes;
}

int FabFabric::send(Message* m)
{
  int ret, e, n;
  MessageType *mt;
  size_t sz = 0;
  
  mt = m->mtype;
  if (mt == NULL)
    return -EINVAL;


  if (!m->mtype->payload) {
    ret = fi_tsend(ep, m->get_arg_ptr(), m->mtype->argsz, NULL,		
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
      e = NELEM(m->siov) - pidx;
      n = m->payload->get_iovs_required();
      if (n < 0)
	return n; 
      
      if (n >= 0 && n > e) 
	m->iov = (struct iovec *) malloc((n + pidx) * sizeof(struct iovec));
      
      n = m->payload->iovec(&m->iov[pidx], n);
      if (n < 0)
	return n;
    }

    // TODO: make it network order???
    m->iov[0].iov_base = &m->mtype->id;
    m->iov[0].iov_len = sizeof(m->mtype->id);
    sz += m->iov[0].iov_len;
    
    if (m->mtype->argsz != 0) {
      m->iov[1].iov_base = m->get_arg_ptr(); 
      m->iov[1].iov_len = m->mtype->argsz;
      sz += m->iov[1].iov_len;
    }
    
    ret = fi_sendv(ep, m->iov, NULL, n+pidx, fi_addrs[m->rcvid], m);
    if (ret != 0) {
      std::cerr << fi_error_str(ret, "fi_sendv", __FILE__, __LINE__) << std::endl;            
      return ret;
    }
  }

  return 0;
}

// Poll the specified completion queue; return completion event in ce.
// Only one ce will be retrieved.

// Timeout is the number of milliseconds to wait.

// Returns error code of the cq read call. If a message was successfully received,
// this will be greated than 0. A code of 0 indicates no message recieved, while
// a negative code is an error.

int FabFabric::check_cq(fid_cq* cq, fi_cq_tagged_entry* ce, int timeout) {
  int ret;
  fi_addr_t src;

    
  ret = fi_cq_sreadfrom(cq, ce, 1, &src, NULL, timeout);
  if (ret >= 0)
    return ret;
  
  // else, an error occured

  if (ret == -FI_EAGAIN)
    return ret; // We need to try again, let caller decide how to handle this

  // A more serious error occured -- print it
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
 
  return ret;
}

// Receive messages one at a time from the receive queue
void FabFabric::progress(bool wait) {
  fi_cq_tagged_entry ce;
  Message *m;
  int ret;

  int timeout = wait ? 1000 : 0;

  while (stop_flag.load() == false) { 
    ret = check_cq(rx_cq, &ce, timeout);
    if (ret > 0) {
      // Received a message
      m = (Message *) ce.op_context;
      m->siov[0].iov_len = ce.len;
      incoming(m);
      continue;
    }

    if (ret == 0) { // Nothing to recieve
      if(!wait)
	return; 
    }
    
    if (ret < 0) {   // An error occured
      if (ret == -FI_EAGAIN && wait) 
	continue; // Try again only if we do not wait
      else
	return; // Return on all other errors
    }
  }
}

// Clean up completed message sends
void FabFabric::handle_tx(bool wait) {
  fi_cq_tagged_entry ce;
  Message *m;
  int ret;

  int timeout = wait ? 1000 : 0;

  while (stop_flag.load() == false) { 
    ret = check_cq(tx_cq, &ce, timeout);
    
    if (ret > 0) {
      // Received a message
      m = (Message *) ce.op_context;
      //if (m->rcvid != get_id()) // TODO : is this correct?
      delete m; // Ok to delete m now, as it's been successfully sent
      
    }

    if (ret == 0) { // Nothing to recieve
      if(!wait)
	return; 
    }
    else {   // An error occured
      if (ret == -FI_EAGAIN && !wait) 
	return; // Try again only if we do not wait
      else
	return; // Return on all other errors
    }
  }
}

// For launching progress from Pthreads
void* FabFabric::bootstrap_progress(void* context) {
  ((FabFabric*) context)->progress(true);
  return 0;
}

void* FabFabric::bootstrap_handle_tx(void* context) {
  ((FabFabric*) context)->handle_tx(true);
  return 0;
}


bool FabFabric::incoming(Message *m)
{
  if (m->mtype != NULL) {
    // tagged message
    if (mts[m->mtype->id] == NULL)
      std::cerr << "WARNING -- unknown message type received -- " << std::endl;
    
    post_tagged(m->mtype);
  } else {
    MessageType* mtype;
    MessageId msgid;
    char* data;
    size_t len;

    // untagged message
    post_untagged();

    data = (char *) m->siov[0].iov_base;
    len = m->siov[0].iov_len;
    
    msgid = *(MessageId *) data;
    mtype = mts[msgid];
    m->mtype = mtype;
    data += sizeof(msgid);
    len -= sizeof(msgid);
    
    if (mtype == NULL) {
      std::cerr << "WARNING -- unknown message type received" << std::endl;
      return false;
    }

    if (mtype->argsz > 0) {
      m->set_arg_ptr(data);
      data += mtype->argsz;
      len -= mtype->argsz;
    }
    
    m->payload = new FabContiguousPayload(FAB_PAYLOAD_KEEP, data, len);
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
  m = new Message(get_id(), mt->id, args, NULL);
  return fi_trecv(ep, args, mt->argsz, NULL, FI_ADDR_UNSPEC, mt->id, 0, m);
}

int FabFabric::post_untagged()
{
  void *buf = malloc(max_send);
  memset(buf, 0, sizeof(buf));

  Message* m = new Message(get_id(), 0, NULL, NULL);
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

void FabFabric::start_progress_threads(const int count, const size_t stack_size) {
  num_progress_threads = count;
  progress_threads = new pthread_t[count];
  pthread_attr_init(&thread_attrs);
  pthread_attr_setstacksize(&thread_attrs, stacksize_in_mb*1024*1024);
  
  for (int i = 0; i < count; ++i) {
    pthread_create(&progress_threads[i], &thread_attrs, &FabFabric::bootstrap_progress, this);
  }
  // tx handler thread will clean up messages that have been sent
  tx_handler_thread = new pthread_t;
  pthread_create(tx_handler_thread, &thread_attrs, &FabFabric::bootstrap_handle_tx, this);
}

void FabFabric::free_progress_threads() {
  stop_flag.store(true);
  if(progress_threads) { 
    for (int i = 0; i < num_progress_threads; ++i) 
      pthread_join(progress_threads[i], NULL);
    delete[] progress_threads;
    progress_threads = NULL;
  }

  if(tx_handler_thread) { 
    pthread_join(*tx_handler_thread, NULL);
    delete(tx_handler_thread);
    tx_handler_thread = NULL;
  }
}

// Wait for the RT to shut down
void FabFabric::wait_for_shutdown() {

  std::cout << "Waiting to shut down..." << std::endl;
  done_mutex.lock();
  std::cout << "OK, shutting down!" << std::endl;
}


/* Return error for a fabric completion queue */
std::string FabFabric::fi_cq_error_str(const int ret, fid_cq* cq) {
  
  std::stringstream sstream;
  struct fi_cq_err_entry err;
  fi_cq_readerr(cq, &err, 0);
  
  sstream << "FABRIC ERROR " << ret << ": "
	  << fi_strerror(err.err) << " "
	  << fi_cq_strerror(cq, err.prov_errno, err.err_data, NULL, 0) << "\n";

  return sstream.str();
}


std::string FabFabric::fi_error_str(const int ret, const std::string call,
				    const std::string file, const int line) {
  std::stringstream sstream;

  sstream << "ERROR " << -ret << " in "<< call << "() at "
	  << file << ":" << line << " -- "
	  << fi_strerror(-ret);

  return sstream.str();
}


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

int FabFabric::get_max_send() {
  return max_send;
}

// Return the number of iovecs that can be used for payload data
// for a single non-RMA send operation. This call is valid for any
// message type. However, it is conservative -- messages that do not
// have arguments can potentially send one extra iovec.
size_t FabFabric::get_iov_limit() {
  size_t limit = fi->tx_attr->iov_limit;
  return limit-2; // Subtract two to account for message id and arguments
}

// Return the number of iovecs that can be used for payload
// data for a message of a given type. This call takes into
// account whether the message type has arguments, so it may allow
// you to send and extra iovec.

// Returns -1 if an invalid message type queried.

size_t FabFabric::get_iov_limit(MessageId id) {
  size_t limit = fi->tx_attr->iov_limit;
  MessageType* mtype = mts[id];
 
  if (mtype == NULL)
    return -1;
  
  if (mtype->argsz == 0)
    return limit-1; // make space for msgid only
  else
    return limit-2; // make space for msgid, args
}


// Send this node's address to address exchange server;
// wait until all other nodes have also reported. Must
// know the total number of nodes before hand. Will find
// this nodes address, and place it and all other addresses
// in the addrs array. This node's ID will be assigned.

// returns a pointer to the addrs array on success, or NULL
// on failure.
void* FabFabric::exchange_addresses() {
  if(num_nodes == 1) {
    // single node mode, no need to exchange
    void* addrs = (void*) malloc(num_nodes*addrlen);
    memcpy(addrs, addr, addrlen);
    return addrs;
  }
     
  
  int ret;
  void* addrs = (void*) malloc(num_nodes*addrlen);
  
  void* context = zmq_ctx_new();
  void* sender = zmq_socket(context, ZMQ_PUSH);
  void* receiver = zmq_socket(context, ZMQ_PULL);

  size_t len = 256;
  char buf[256];
  std::cout << "My address: " << fi_av_straddr(av, addr, buf, &len) << std::endl;


  // Connect fan-in sender socket
  std::stringstream sstream;
  sstream << "tcp://" << exchange_server_host
	  << ":" << exchange_server_send_port;
  ret = zmq_connect(sender, sstream.str().c_str());
  assert(ret == 0);

  // Connect fan-out receiver socket
  sstream.str("");
  sstream.clear();
  sstream << "tcp://" << exchange_server_host
	  << ":" << exchange_server_recv_port;
  ret = zmq_connect(receiver, sstream.str().c_str());
  assert(ret == 0);
  
  // Send our Fabric address info to the server
  ret = zmq_send(sender, addr, addrlen, 0);
  assert(ret == addrlen);
  
  // Wait for the server to reply with out ID and a list of all
  // addresses.
  zmq_recv(receiver, addrs, addrlen*num_nodes, 0);
  
  // Search the received list of addresses to determine this node's ID.
  // Is there a better way to do this? Since messages could be sent in any
  // order, there's no way for ZMQ to associate an incoming reported address with
  // an outgoing send
  char* p = (char*) addrs;
  for(int i = 0; i < num_nodes; ++i) {
    if (memcmp(addr, p, addrlen) == 0) {
      id = i;
      break;
    }
    p += addrlen;
  }

  std::cout << "Exchanged, id assigned: " << id << std::endl;
  
  zmq_close(sender);
  zmq_close(receiver);
  zmq_ctx_destroy(context);
  
  return addrs;
}

// Dump the parameters of this Fabric to a string
std::string FabFabric::tostr() {
  std::stringstream sstream;
  sstream << "FabFabric object: \n"
	  << "    id: " << id << "\n"
	  << "    num_nodes: " << num_nodes << "\n"
	  << "    max_send: "  << max_send  << "\n"
	  << "    pend_num: "  << pend_num  << "\n"
	  << "    num_progress_threads: " << num_progress_threads << "\n"
	  << "    stacksize_in_mb: " << stacksize_in_mb << "\n"
	  << "    exchange_server_host: " << exchange_server_host << "\n"
    	  << "    exchange_server_send_port: " << exchange_server_send_port << "\n"
	  << "    exchange_server_recv_port: " << exchange_server_recv_port << "\n"
	  << "Fabric info: "
	  << fi_tostr((void*) fi->fabric_attr, FI_TYPE_FABRIC_ATTR) << "\n";

  return sstream.str();
}

// If called by root, will wait for all gather entries to complete and
// returns the gathered array.
// Otherwise, sends event data to root and returns NULL.
Realm::Event* FabFabric::gather_events(Realm::Event& event, NodeId root) {
  if (id == root) {
    event_gatherer.add_entry(event, id);
    return event_gatherer.wait();
  } else {
    send(new EventGatherMessage(root, event, id));
  }
  return NULL;
}

// Register and incoming gather event with the gatherer
void FabFabric::recv_gather_event(Realm::Event& event, NodeId sender) {
  event_gatherer.add_entry(event, sender);
}


// If called by root, sends the event to all other nodes in the fabric. The
// resulting event will be stored in the event parameter.
void FabFabric::broadcast_events(Realm::Event& event, NodeId root) {
  if (id == root) {
    for (NodeId i=0; i<num_nodes; ++i) {
      if (i != id) // No need to send to self
	fabric->send(new EventBroadcastMessage(i, event, id));
    }		    
  } else {
    event_broadcaster.wait(event, root);
  }   
}

void FabFabric::recv_broadcast_event(Realm::Event& event, NodeId sender) {
  event_broadcaster.add_entry(event, sender);
}

// Set the address vector to the one provided, and reset this Fabric's
// ID and node count to reflect its position in the new address vector.
//
// This function is currently intended for testing, where you may wish to pre-configure
// a fabric to test.
//
// Returns code of the av_insert call on success, will terminate on failure.
int FabFabric::set_address_vector(void* addrs, size_t addrlen, NodeId new_id, uint32_t new_num_nodes) {
  int ret;
  // Remove all addresses from the AV
  fi_av_remove(av, fi_addrs, num_nodes, 0);
  // Update fabric paramters
  id = new_id;
  num_nodes = new_num_nodes;

  free(fi_addrs);
  fi_addrs = (fi_addr_t*) malloc(num_nodes * sizeof(fi_addr_t));
  memset(fi_addrs, 0, sizeof(fi_addr_t*)*num_nodes);

  ret = fi_av_insert(av, addrs, num_nodes, fi_addrs, 0, NULL);

  if (ret < 0) {
    std::cerr << "ERROR -- set_address_vector() failed" << std::endl;
    std::cerr << "Fabric error: " << fi_error_str(ret, "fi_av_insert", __FILE__, __LINE__) << std::endl;
    exit(1); 
  }

  return ret;
}

// Write this node's address into buf. Returns this node's address length.
size_t FabFabric::get_address(char buf[64]) {
  memcpy(buf, addr, addrlen);
  return addrlen;
}


void FabFabric::barrier_wait(uint32_t barrier_id) {
  barrier_waiter.wait(barrier_id);
}

// Sends a barrier notification to everyone including self
void FabFabric::barrier_notify(uint32_t barrier_id) {
  for (NodeId i=0; i<num_nodes; ++i) {
    if (i != id) {
      fabric->send(new BarrierNotifyMessage(i, barrier_id, id));
    } else {
      barrier_waiter.notify(barrier_id, id);
    }
  }
}

void FabFabric::recv_barrier_notify(uint32_t barrier_id, NodeId sender) {
  barrier_waiter.notify(barrier_id, sender);
}

void FabFabric::synchronize_clocks() {
  // Use barriers to ATTEMPT synchronization. This is how Realm accomplished
  // synchronization previously. Could be improved?
  barrier_notify(CLOCK_SYNC_BARRIER_ID);
  barrier_wait(CLOCK_SYNC_BARRIER_ID);  
  barrier_notify(CLOCK_SYNC_BARRIER_ID+1);
  barrier_wait(CLOCK_SYNC_BARRIER_ID+1);
  Realm::Clock::set_zero_time();
  barrier_notify(CLOCK_SYNC_BARRIER_ID+2);
  barrier_wait(CLOCK_SYNC_BARRIER_ID+2);
}

// Shut down / clean up the RT and exit with requested code
void FabFabric::fatal_shutdown(int code) {
  shutdown();
  exit(code);
}

void* FabFabric::regmem_alloc(size_t size) {
  // TODO
  assert(false); 
  return NULL;
}

void FabFabric::regmem_free() {
  // TODO
  assert(false); 
}


void FabFabric::regmem_get(off_t offset, void* dst, size_t len) {
  // TODO
  assert(false);
}

void FabFabric::regmem_put(off_t offset, const void* src, size_t len) {
  // TODO
  assert(false);
}
