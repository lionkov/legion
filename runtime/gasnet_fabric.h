// Provides an interface for original GASNet / ActiveMessage communication

#ifndef GASNET_FABRIC_H
#define GASNET_FABRIC_H

#include "fabric_types.h"
#include "fabric.h"
#include "payload.h"
#include "activemsg.h"
#include <stdint.h>
#include <mutex>
#include <condition_variable>

// Adapts Gasnet / AM style message classes to be used with Fabric-
// style interfaces. GasnetMessageAdapterBase is non-templated and for
// use in collections.

// Adapters are templated on the message type and generate functions for
// packing / unpacking arguments into format expected by GASNet, as well
// creating the required ActiveMessage object.

class GasnetMessageAdapterBase {
public:
  virtual ~GasnetMessageAdapterBase() { }
  virtual void request(NodeId dest, void* args, const void* payload, size_t payload_len,
			      int payload_mode) = 0;
};

static const int GASNET_COLL_FLAGS = GASNET_COLL_IN_MYSYNC | GASNET_COLL_OUT_MYSYNC | GASNET_COLL_LOCAL;

template <MessageId MSGID, typename MSGTYPE> 
class GasnetMessageAdapterShort : public GasnetMessageAdapterBase {
public:
  
  static void handle_request(typename MSGTYPE::RequestArgs args) {
    // Pack the received data back into a Message and call its handler
    Message m(fabric->get_id(), MSGID, &args, NULL);
    m.mtype->request(&m);
  }

  void request(NodeId dest, void* args, const void* payload, size_t payload_len,
		      int payload_mode) {
    std::cout << "Sending a short message... " << std::endl;
    typename MSGTYPE::RequestArgs* r_args = (typename MSGTYPE::RequestArgs*) args;
    ActiveMessage::request(dest, *r_args);
  }
  
  typedef ActiveMessageShortNoReply<MSGID,  
				    typename MSGTYPE::RequestArgs,
				    handle_request> ActiveMessage;
};

template <MessageId MSGID, typename MSGTYPE> 
class GasnetMessageAdapterMedium : public GasnetMessageAdapterBase {
public:
  
  // For medium messages, the RequestArgs must inherit from BaseMedium and have a default constructor
  struct RequestArgsBaseMedium : public MSGTYPE::RequestArgs, public BaseMedium {
    RequestArgsBaseMedium() { }
    RequestArgsBaseMedium(const typename MSGTYPE::RequestArgs& other) : MSGTYPE::RequestArgs(other) { }
  }; 

  static void handle_request(RequestArgsBaseMedium args, const void* data, size_t datalen) {
    // Pack the received data back into a Message and call its handler
    Message m(fabric->get_id(), MSGID, &args,
	      new FabContiguousPayload(FAB_PAYLOAD_KEEP, (void*) data, datalen));
    m.mtype->request(&m);
  }

  void request(NodeId dest, void* args, const void* payload, size_t payload_len,
	       int payload_mode) {
    std::cout << "Sending a medium message... " << std::endl;
    typename MSGTYPE::RequestArgs* r_args = (typename MSGTYPE::RequestArgs*) args;
    RequestArgsBaseMedium* basemedium = new RequestArgsBaseMedium(*r_args);
    std::cout << std::hex << basemedium->MESSAGE_ID_MAGIC << std::endl;
    ActiveMessage::request(dest, *basemedium, payload, payload_len, payload_mode);
  }
  
  typedef ActiveMessageMediumNoReply<MSGID,
				     RequestArgsBaseMedium,
				     handle_request> ActiveMessage;
};

// Allows lookup of RequestArgs type for messages
template <typename T>
typename T::RequestArgs get_requestargs_type() { return T::RequestArgs(); }
	  
// Gasnet MUTEX_T types are defined in activemsg.h
class GasnetFabric : public Fabric {
public: 
  GasnetFabric(int* argc, char*** argv);
  ~GasnetFabric();

  
  template <MessageId MSGID, typename MSGTYPE> 
  bool add_message_type(MSGTYPE* mt, std::string tag) {
    bool ret;
    // Register handler for the message request
    ret = Fabric::add_message_type((MessageType*) mt, tag);
  
    // Register handler for unpacking short/medium message type
    if (mt->payload == true) {
      GasnetMessageAdapterMedium<MSGID, MSGTYPE>
	::ActiveMessage::add_handler_entries(&gasnet_handlers[gasnet_hcount], tag.c_str());
      if (MSGID == 0 || gasnet_adapters[MSGID] != NULL) 
	assert(false && "Attempted to add invalid message type");
      gasnet_adapters[MSGID] = (GasnetMessageAdapterBase*) new GasnetMessageAdapterMedium<MSGID, MSGTYPE>(); 
    } else {
      GasnetMessageAdapterShort<MSGID, MSGTYPE>
	::ActiveMessage::add_handler_entries(&gasnet_handlers[gasnet_hcount], tag.c_str());
      if (MSGID == 0 || gasnet_adapters[MSGID] != NULL) 
	assert(false && "Attempted to add invalid message type");
      gasnet_adapters[MSGID] = (GasnetMessageAdapterBase*) new GasnetMessageAdapterShort<MSGID, MSGTYPE>();
    }
    
    ++gasnet_hcount;
    return true;
  }
  
  virtual void register_options(Realm::CommandLineParser& cp);
  virtual bool init(int argc, const char** argv, Realm::CoreReservationSet& core_reservations);
  virtual void shutdown();
  virtual void wait_for_shutdown();  
  virtual void synchronize_clocks();
  virtual void fatal_shutdown(int code);
  
  // RDMA operations cannot be used with a single-node fabric!
  virtual void get_bytes(NodeId target, off_t offset, void* dst, size_t len);
  virtual void* get_regmem_ptr();
  virtual void wait_for_rdmas();
  virtual size_t get_regmem_size_in_mb();

  virtual int send(Message* m);
  virtual Realm::Event* gather_events(Realm::Event& event, NodeId root);
  virtual void recv_gather_event(Realm::Event& event, NodeId sender);
  virtual void broadcast_events(Realm::Event& event, NodeId root);
  virtual void recv_broadcast_event(Realm::Event& event, NodeId sender);
  virtual void barrier_wait(uint32_t barrier_id);
  virtual void barrier_notify(uint32_t barrier_id);
  virtual void recv_barrier_notify(uint32_t barrier_id, NodeId sender);
  virtual NodeId get_id();
  virtual uint32_t get_num_nodes();
  virtual size_t get_iov_limit();
  virtual size_t get_iov_limit(MessageId id);
  virtual size_t get_max_send();


protected:
  size_t gasnet_mem_size_in_mb;
  size_t reg_mem_size_in_mb;
  size_t active_msg_worker_threads;
  size_t active_msg_handler_threads;
  size_t stack_size_in_mb;

  gasnet_handlerentry_t gasnet_handlers[MAX_MESSAGE_TYPES];
  size_t gasnet_hcount; // gasnet handler entry count
  GasnetMessageAdapterBase* gasnet_adapters[MAX_MESSAGE_TYPES];

  bool shutdown_complete = false;
  std::mutex shutdown_mutex;
  std::condition_variable shutdown_cond;
  char* regmem_base = 0;
  
  
};





#endif // GASNET_FABRIC_H
