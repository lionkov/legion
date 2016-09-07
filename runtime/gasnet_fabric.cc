#include "gasnet_fabric.h"

void doNothing(MessageType* mt, const void* buf, size_t len) {
  return;
}

GasnetFabric::GasnetFabric(int* argc, char*** argv) 
  : gasnet_mem_size_in_mb(0),
    reg_mem_size_in_mb(0),
    active_msg_worker_threads(1),
    active_msg_handler_threads(1),
    gasnet_hcount(0) {
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
}

void GasnetFabric::register_options(Realm::CommandLineParser& cp) {
  cp.add_option_int("-ll:gsize", gasnet_mem_size_in_mb)
    .add_option_int("-ll:rsize", reg_mem_size_in_mb)
    .add_option_int("-ll:amsg", active_msg_worker_threads)
    .add_option_int("-ll:ahandlers", active_msg_handler_threads);
}

// Gasnet requires two handlers -- one to deal with the actual message request
// (as usual), and one to deal with unpacking the appropriate message type.
bool GasnetFabric::add_message_type(MessageType* mt, const std::string tag) {
  // Register handler for the message request
  Fabric::add_message_type(mt, tag);
  
  // Register handler for unpacking short/medium message type
  if (mt->payload == true) {
    // If there's a payload, this is a medium message
    //ActiveMessageMediumNoReply<mt->id, decltype(mt->RequestArgs), doNothing) ActiveMessage;
    ;
  }
  
}

bool GasnetFabric::init(bool manually_set_addresses) {
  
  

  return true;
}
