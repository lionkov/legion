// Provides an interface for original GASNet / ActiveMessage communication

#ifndef GASNET_FABRIC_H
#define GASNET_FABRIC_H

#include "activemsg.h"
#include "fabric_types.h"
#include "fabric.h"
#include "payload.h"
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
  // You'll have to check the message / payload type to figure out which request() to call
  virtual void request(NodeId dest, void* args) = 0; // no payload
  virtual void request(NodeId dest, void* args, const void* payload, size_t payload_len,
		       int payload_mode) = 0; // ContiguousPayload
  virtual void request(NodeId dest, void* args, const void* payload, size_t line_len,
		       off_t line_stride, size_t line_count, int payload_mode) = 0; // TwoDPayloa
  virtual void request(NodeId dest, void* args, const SpanList& spans, size_t datalen,
		       int payload_mode) = 0; // SpanPayload
};

static const int GASNET_COLL_FLAGS = GASNET_COLL_IN_MYSYNC | GASNET_COLL_OUT_MYSYNC | GASNET_COLL_LOCAL;

template <MessageId MSGID, typename MSGTYPE> 
class GasnetMessageAdapterShort : public GasnetMessageAdapterBase {
public:
  
  static void handle_request(typename MSGTYPE::RequestArgs args) {
    // Pack the received data back into a Message and call its handler
    Message m(fabric->get_id(), MSGID, &args, NULL);
    fabric->log_fabric().debug() << "Received short message of type: " << fabric->mdescs[MSGID];
    m.mtype->request(&m);
  }

  void request(NodeId dest, void* args) {
    typename MSGTYPE::RequestArgs* r_args = (typename MSGTYPE::RequestArgs*) args;
    ActiveMessage::request(dest, *r_args);
  }

  void request(NodeId dest, void* args, const void* payload, size_t payload_len,
	       int payload_mode) {    
    assert (false && "Wrong request function for this message type");
  }
  void request(NodeId dest, void* args, const void* payload, size_t line_len,
		       off_t line_stride, size_t line_count, int payload_mode) {
    assert (false && "Wrong request function for this message type");
  }
  void request(NodeId dest, void* args, const SpanList& spans, size_t datalen,
		       int payload_mode) {
    assert (false && "Wrong request function for this message type");
  }
  typedef ActiveMessageShortNoReply<MSGID,  
				    typename MSGTYPE::RequestArgs,
				    handle_request> ActiveMessage;
};

template <MessageId MSGID, typename MSGTYPE> 
class GasnetMessageAdapterMedium : public GasnetMessageAdapterBase {
public:
  
  // For medium messages, the RequestArgs must inherit from BaseMedium and have a default constructor

  /*
  struct RequestArgsBaseMedium : public BaseMedium, public MSGTYPE::RequestArgs {
    RequestArgsBaseMedium() { }
    RequestArgsBaseMedium(const typename MSGTYPE::RequestArgs& other) : MSGTYPE::RequestArgs(other) {
      std::cout << "PROCS" << num_procs << std::endl;
    }
  };
  */
  static void handle_request(typename MSGTYPE::RequestArgs args, const void* data, size_t datalen) {
    // Pack the received data back into a Message and call its handler    
    Message m(fabric->get_id(), MSGID, &args,
	      new FabContiguousPayload(FAB_PAYLOAD_COPY, (void*) data, datalen));
    fabric->log_fabric().debug() << "Received medium message of type: " << fabric->mdescs[MSGID];
    m.mtype->request(&m);
  }

  void request(NodeId dest, void* args) { assert (false && "Wrong request function for this message type"); }
  
  void request(NodeId dest, void* args, const void* payload, size_t payload_len,
	       int payload_mode) {
    //typename MSGTYPE::RequestArgs* r_args = (typename MSGTYPE::RequestArgs*) args;
    //RequestArgsBaseMedium* basemedium = new RequestArgsBaseMedium(*r_args);
    //std::cout << std::hex << basemedium->MESSAGE_ID_MAGIC << std::endl;
    typename MSGTYPE::RequestArgs* r_args = (typename MSGTYPE::RequestArgs*) args;
    ActiveMessage::request(dest,
			   //*basemedium,
			   *r_args,
			   payload,
			   payload_len,
			   payload_mode);
  }

  void request(NodeId dest, void* args, const void* payload, size_t line_len,
		       off_t line_stride, size_t line_count, int payload_mode) {

    typename MSGTYPE::RequestArgs* r_args = (typename MSGTYPE::RequestArgs*) args;
    ActiveMessage::request(dest,
			   *r_args,
			   payload,
			   line_len,
			   line_stride,
			   line_count,
			   payload_mode);

  }
  
  void request(NodeId dest, void* args, const SpanList& spans, size_t datalen,
		       int payload_mode) {
    typename MSGTYPE::RequestArgs* r_args = (typename MSGTYPE::RequestArgs*) args;
    ActiveMessage::request(dest,
			   *r_args,
			   spans,
			   datalen,
			   payload_mode);
  }
  
  typedef ActiveMessageMediumNoReply<MSGID,
				     typename MSGTYPE::RequestArgs,
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


  // These functions use SNIFAE to determine whether to generate
  // a medium or short AM stub
  
  template <MessageId MSGID, typename MSGTYPE>
  bool add_message_type_snifae(MSGTYPE* mt, std::string tag, typename MSGTYPE::has_payload placeholder) {
    bool ret = Fabric::add_message_type((MessageType*) mt, tag);
    assert ((mt->payload == true) && "This message type must have a payload -- check that it derives from the correct class");
    GasnetMessageAdapterMedium<MSGID, MSGTYPE>
      ::ActiveMessage::add_handler_entries(&gasnet_handlers[gasnet_hcount], tag.c_str());
    if (MSGID == 0 || gasnet_adapters[MSGID] != NULL) 
      assert(false && "Attempted to add invalid message type");
    gasnet_adapters[MSGID] = (GasnetMessageAdapterBase*) new GasnetMessageAdapterMedium<MSGID, MSGTYPE>(); 
    ++gasnet_hcount;
    return ret;
  }
  
  template <MessageId MSGID, typename MSGTYPE>
  bool add_message_type_snifae(MSGTYPE* mt, std::string tag, ... ) {
    bool ret;
    assert((mt->payload == false) && "This message type does not have a payload -- check if it derives from PayloadMessageType");
    ret = Fabric::add_message_type((MessageType*) mt, tag);
    GasnetMessageAdapterShort<MSGID, MSGTYPE>
      ::ActiveMessage::add_handler_entries(&gasnet_handlers[gasnet_hcount], tag.c_str());
    if (MSGID == 0 || gasnet_adapters[MSGID] != NULL) 
      assert(false && "Attempted to add invalid message type");
    gasnet_adapters[MSGID] = (GasnetMessageAdapterBase*) new GasnetMessageAdapterShort<MSGID, MSGTYPE>(); 
    ++gasnet_hcount;
    return ret;
  }
  
  
  
  template <MessageId MSGID, typename MSGTYPE> 
  bool add_message_type(MSGTYPE* mt, std::string tag) {
    // Uses SNIFAE to select whether a medium or short AM is generated.
    // All messages with payloads should derive from the PayloadMessageType class.
    // This class contains a typedef of the has_payload type, so we should
    // route to the correct function if MSGTYPE is a PayloadMessageType.
    // Third argument is bogus an not used for anything except SNIFAE.
    char c;
    return add_message_type_snifae<MSGID, MSGTYPE>(mt, tag, &c);
  }
  
  virtual void register_options(Realm::CommandLineParser& cp);
  virtual bool init(int argc, const char** argv, Realm::CoreReservationSet& core_reservations);
  void start_polling();
  char* set_up_regmem();
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
  virtual size_t get_max_send(Realm::Memory mem);
  
  size_t gasnet_mem_size_in_mb;
  size_t reg_mem_size_in_mb;
  
protected:
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
