#include "gasnet_fabric.h"
#include "mem_impl.h"

GasnetFabric::GasnetFabric(int* argc, char*** argv) 
  : gasnet_mem_size_in_mb(0),
    reg_mem_size_in_mb(0),
    active_msg_worker_threads(1),
    active_msg_handler_threads(1),
    gasnet_hcount(0) {

  for (int i=0; i<MAX_MESSAGE_TYPES; ++i) 
    gasnet_adapters[i] = NULL;
  
  // Setup that needs to be done before calling init()
   
  char *orig_pmi_gni_cookie = getenv("PMI_GNI_COOKIE");
  if(orig_pmi_gni_cookie) {
    char new_pmi_gni_cookie[32];
    snprintf(new_pmi_gni_cookie, 32, "%d", 1+atoi(orig_pmi_gni_cookie));
    setenv("PMI_GNI_COOKIE", new_pmi_gni_cookie, 1 /*overwrite*/);
  }
  // SJT: another GASNET workaround - if we don't have GASNET_IB_SPAWNER set, assume it was MPI
  // (This is called GASNET_IB_SPAWNER for versions <= 1.24 and GASNET_SPAWNER for versions >= 1.26)
  if(!getenv("GASNET_IB_SPAWNER") && !getenv("GASNET_SPAWNER")) {
    setenv("GASNET_IB_SPAWNER", "mpi", 0 /*no overwrite*/);
    setenv("GASNET_SPAWNER", "mpi", 0 /*no overwrite*/);
  }

  // and one more... disable GASNet's probing of pinnable memory - it's
  //  painfully slow on most systems (the gemini conduit doesn't probe
  //  at all, so it's ok)
  // we can do this because in gasnet_attach() we will ask for exactly as
  //  much as we need, and we can detect failure there if that much memory
  //  doesn't actually exist
  // inconveniently, we have to set a PHYSMEM_MAX before we call
  //  gasnet_init and we don't have our argc/argv until after, so we can't
  //  set PHYSMEM_MAX correctly, but setting it to something really big to
  //  prevent all the early checks from failing gets us to that final actual
  //  alloc/pin in gasnet_attach ok
  {
    // the only way to control this is with environment variables, so set
    //  them unless the user has already set them (in which case, we assume
    //  they know what they're doing)
    // do handle the case where NOPROBE is set to 1, but PHYSMEM_MAX isn't
    const char *e = getenv("GASNET_PHYSMEM_NOPROBE");
    if(!e || (atoi(e) > 0)) {
      if(!e)
	setenv("GASNET_PHYSMEM_NOPROBE", "1", 0 /*no overwrite*/);
      if(!getenv("GASNET_PHYSMEM_MAX")) {
	// just because it's fun to read things like this 20 years later:
	// "nobody will ever build a system with more than 1 TB of RAM..."
	setenv("GASNET_PHYSMEM_MAX", "1T", 0 /*no overwrite*/);
      }
    }
  }

  // and yet another GASNet workaround: the Infiniband conduit seems to
  //  have a problem with AMRDMA mode, consuming receive buffers even for
  //  request targets that are in AMRDMA mode - disable the mode by default
#ifdef GASNET_CONDUIT_IBV
  if(!getenv("GASNET_AMRDMA_MAX_PEERS"))
    setenv("GASNET_AMRDMA_MAX_PEERS", "0", 0 /*no overwrite*/);
#endif
  
#ifdef DEBUG_REALM_STARTUP
      { // we don't have rank IDs yet, so everybody gets to spew
        char s[80];
        gethostname(s, 79);
        strcat(s, " enter gasnet_init");
        TimeStamp ts(s, false);
        fflush(stdout);
      }
#endif
      
      CHECK_GASNET( gasnet_init(argc, argv) );
      
#ifdef DEBUG_REALM_STARTUP
      { // once we're convinced there isn't skew here, reduce this to rank 0
        char s[80];
        gethostname(s, 79);
        strcat(s, " exit gasnet_init");
        TimeStamp ts(s, false);
        fflush(stdout);
      }
#endif
      
}

GasnetFabric::~GasnetFabric() {
  for(int i=0; i<MAX_MESSAGE_TYPES; ++i) {
    if (gasnet_adapters[i])
      delete gasnet_adapters[i];
  }
}

void GasnetFabric::register_options(Realm::CommandLineParser& cp) {
  cp.add_option_int("-ll:gsize", gasnet_mem_size_in_mb)
    .add_option_int("-ll:rsize", reg_mem_size_in_mb)
    .add_option_int("-ll:amsg", active_msg_worker_threads)
    .add_option_int("-ll:ahandlers", active_msg_handler_threads)
    .add_option_int("-ll:stacksize", stack_size_in_mb);
}


bool GasnetFabric::init(int argc, const char** argv, Realm::CoreReservationSet& core_reservations) {
  init_endpoints(gasnet_handlers, gasnet_hcount,
		 gasnet_mem_size_in_mb, reg_mem_size_in_mb,
		 core_reservations,
		 argc, argv);
  gasnet_coll_init(0,0,0,0,0);
  gasnet_set_waitmode(GASNET_WAIT_BLOCK);
  
  return true;
}


char* GasnetFabric::set_up_regmem() {
  // Set up registered memory
  if (reg_mem_size_in_mb > 0) {
    gasnet_seginfo_t *seginfos = new gasnet_seginfo_t[gasnet_nodes()];
    CHECK_GASNET( gasnet_getSegmentInfo(seginfos, gasnet_nodes()) );
    regmem_base = ((char *)(seginfos[gasnet_mynode()].addr)) + (gasnet_mem_size_in_mb << 20);
    delete[] seginfos;
    return regmem_base;
  } else {
    return NULL;
  }
}

void GasnetFabric::shutdown() {
  stop_activemsg_threads();
  shutdown_complete = true;
  shutdown_mutex.unlock();
  shutdown_cond.notify_all();
}

void GasnetFabric::wait_for_shutdown() {
  std::unique_lock<std::mutex> lk(shutdown_mutex);
  shutdown_cond.wait(lk, [this] { return this->shutdown_complete; });
}

void GasnetFabric::synchronize_clocks() {
  // GasnetFabric does this in init_endpoints
  return;
}

void GasnetFabric::fatal_shutdown(int code) {
  shutdown();
  wait_for_shutdown();
  gasnet_exit(code);
}

void GasnetFabric::get_bytes(NodeId target, off_t offset, void* dst, size_t len) {
  void* srcptr = ((char*) regmem_base) + offset;
  gasnet_get(dst, target, srcptr, len);
}

void* GasnetFabric::get_regmem_ptr() {
  assert((reg_mem_size_in_mb > 0) && "Error -- can't get regmem ptr, no registered memory was created.");
  return regmem_base;
}

void GasnetFabric::wait_for_rdmas() {
  // This functionality isn't used by GASNet fabric (could be implemented?)
  assert (false && "GASNet wait_for_rdmas not implemented");
}

size_t GasnetFabric::get_regmem_size_in_mb() { return reg_mem_size_in_mb; }

int GasnetFabric::send(Message* m) {
  // Check the payload type of the message to figure out which handler we need
  FabPayload* p = m->payload;
  if (p == NULL) {
    log_fabric().debug() << "Sending message of type: " << mdescs[m->id] << ", carrying no payload";
    gasnet_adapters[m->id]->request(m->rcvid, m->get_arg_ptr());
    return 0;
  }

  FabContiguousPayload* c = dynamic_cast<FabContiguousPayload*>(p);
  if (c) {
    log_fabric().debug() << "Sending message of type: " << mdescs[m->id] << ", carrying a ContiguousPayload";
    gasnet_adapters[m->id]->request(m->rcvid,
				    m->get_arg_ptr(),
				    c->ptr(),
				    c->size(),
				    c->get_mode());
				    
    return 0;
  }

  FabTwoDPayload* d = dynamic_cast<FabTwoDPayload*>(p);
  if (d) {
    log_fabric().debug() << "Sending message of type: " << mdescs[m->id] << ", carrying a TwoDPayload";
    gasnet_adapters[m->id]->request(m->rcvid,
				    m->get_arg_ptr(),
				    d->ptr(),
				    d->get_linesz(),
				    d->get_stride(),
				    d->get_linecnt(),
				    d->get_mode());
    return 0;
  }

  FabSpanPayload* s = dynamic_cast<FabSpanPayload*>(p);
  if (c) {
    log_fabric().debug() << "Sending message of type: " << mdescs[m->id] << ", carrying a SpanPayload";
    gasnet_adapters[m->id]->request(m->rcvid,
				    m->get_arg_ptr(),
				    *(s->get_spans()),
				    s->size(),
				    s->get_mode());
    return 0;
  }

  assert(false && "Couldn't identify payload type");
  return -1; // Error, couldn't figure out payload type
}


Realm::Event* GasnetFabric::gather_events(Realm::Event& event, NodeId root) {
  Realm::Event* all_events = NULL;
  if (root == gasnet_mynode()) {
    all_events = new Realm::Event[gasnet_nodes()];
    gasnet_coll_gather(GASNET_TEAM_ALL, root, all_events, &event, sizeof(Realm::Event), GASNET_COLL_FLAGS);
  } else {
    gasnet_coll_gather(GASNET_TEAM_ALL, root, 0, &event, sizeof(Realm::Event), GASNET_COLL_FLAGS);
  }
  return all_events;
}

void GasnetFabric::recv_gather_event(Realm::Event& event, NodeId sender) {
  return; // GasNet uses build-in collectives -- this is not needed
}
void GasnetFabric::broadcast_events(Realm::Event& event, NodeId root) {
  if (root == gasnet_mynode()) 
    gasnet_coll_broadcast(GASNET_TEAM_ALL, &event, root, &event, sizeof(Realm::Event), GASNET_COLL_FLAGS);
  else
    gasnet_coll_broadcast(GASNET_TEAM_ALL, &event, root, 0, sizeof(Realm::Event), GASNET_COLL_FLAGS);
}
void GasnetFabric::recv_broadcast_event(Realm::Event& event, NodeId sender) {
  return; // GasNet uses build-in collectives -- this is not needed
}
void GasnetFabric::barrier_wait(uint32_t barrier_id) {
  gasnet_barrier_wait(0, GASNET_BARRIERFLAG_ANONYMOUS);
}
void GasnetFabric::barrier_notify(uint32_t barrier_id) {
  gasnet_barrier_notify(0, GASNET_BARRIERFLAG_ANONYMOUS);
}
void GasnetFabric::recv_barrier_notify(uint32_t barrier_id, NodeId sender) {
    return; // GasNet uses build-in collectives -- this is not needed
}

NodeId GasnetFabric::get_id() {
  return gasnet_mynode();
}

uint32_t GasnetFabric::get_num_nodes() {
  return gasnet_nodes();
}

size_t GasnetFabric::get_iov_limit() {
  assert(false);
  return 0;  
}
size_t GasnetFabric::get_iov_limit(MessageId id) {
  assert(false && "Gasnet Fabrics shouldn't use IOVs");
  return 0;
}
size_t GasnetFabric::get_max_send(Realm::Memory mem) {
  return get_lmb_size(Realm::ID(mem).memory.owner_node);
}
