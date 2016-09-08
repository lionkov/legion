// Provides an interface for original GASNet / ActiveMessage communication

#ifndef GASNET_FABRIC_H
#define GASNET_FABRIC_H

#include "fabric_types.h"
#include "fabric.h"
#include "activemsg.h"
#include <stdint.h>

template <typename T>
void doNothingShort(T RequestArgs) {
  return;
}
template <typename T>
void doNothingLong(T RequestArgs, const void* buf, unsigned long size) {
  return;
}

// Allows lookup of RequestArgs type for messages
template <typename T>
typename T::RequestArgs get_requestargs_type() { return T::RequestArgs(); }
	  
// Gasnet Mutex types are defined in activemsg.h
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
      // If there's a payload, this is a medium message
      //ActiveMessageMediumNoReply<MSGID,
      //			 typename MSGTYPE::RequestArgs,
      //&doNothingLong<MSGTYPE>> ActiveMessage;
      ;
    } else {
      // No payload, so use a short AM
      
      //ActiveMessageShortNoReply<MSGID,
      //			typename MSGTYPE::RequestArgs,
      //			doNothingShort<MSGTYPE>> ActiveMessage;
      ;
    }
    return true;
  }
  
  virtual void register_options(Realm::CommandLineParser& cp);
  virtual bool init();
  virtual void shutdown();
  virtual void wait_for_shutdown();  
  virtual void synchronize_clocks();
  virtual void fatal_shutdown(int code);
  
  // RDMA operations cannot be used with a single-node fabric!
  virtual void put_bytes(NodeId target, off_t offset, const void* src, size_t len);
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


private:
  size_t gasnet_mem_size_in_mb;
  size_t reg_mem_size_in_mb;
  size_t active_msg_worker_threads;
  size_t active_msg_handler_threads;

  gasnet_handlerentry_t gasnet_handlers[MAX_MESSAGE_TYPES];
  size_t gasnet_hcount; // gasnet handler entry count
};

#endif // GASNET_FABRIC_H
