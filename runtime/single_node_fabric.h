
// Fabric for single-node / non-networked use.

#ifndef SINGLE_NODE_FABRIC_H
#define SINGLE_NODE_FABRIC_H

#include "fabric.h"
#include "cmdline.h"
#include "timers.h"
#include <cstring>
#include <string>
#include <iostream>
#include <atomic>

class SingleNodeFabric : public Fabric {
public:
  SingleNodeFabric() {}
  ~SingleNodeFabric();

  virtual void register_options(Realm::CommandLineParser& cp);
  virtual bool init(bool manually_set_addresses = false) = 0;
  virtual void shutdown() = 0;
  virtual void wait_for_shutdown() = 0;  
  virtual void synchronize_clocks() = 0;
  virtual void fatal_shutdown(int code) = 0;
  
  // RDMA operations cannot be used with a single-node fabric!
  virtual void put_bytes(NodeId target, off_t offset, const void* src, size_t len) = 0;
  virtual void get_bytes(NodeId target, off_t offset, void* dst, size_t len) = 0;
  virtual void* get_regmem_ptr() = 0;
  virtual void wait_for_rdmas() = 0;
  virtual size_t get_regmem_size_in_mb() = 0;

  virtual int send(Message* m) = 0;
  virtual Realm::Event* gather_events(Realm::Event& event, NodeId root) = 0;
  virtual void recv_gather_event(Realm::Event& event, NodeId sender) = 0;
  virtual void broadcast_events(Realm::Event& event, NodeId root) = 0;
  virtual void recv_broadcast_event(Realm::Event& event, NodeId sender) = 0;
  virtual void barrier_wait(uint32_t barrier_id) = 0;
  virtual void barrier_notify(uint32_t barrier_id) = 0;
  virtual void recv_barrier_notify(uint32_t barrier_id, NodeId sender) = 0;
  virtual NodeId get_id() = 0;
  virtual uint32_t get_num_nodes() = 0;
  virtual size_t get_iov_limit() = 0;
  virtual size_t get_iov_limit(MessageId id) = 0;
  virtual size_t get_max_send() = 0;
  
private:

};
 
#endif // SINGLE_NODE_FABRIC_H
