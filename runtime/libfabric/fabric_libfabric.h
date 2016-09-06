#ifndef FABRIC_LIBFABRIC_H
#define FABRIC_LIBFABRIC_H
#include "fabric.h"
#include "cmdline.h"
#include "timers.h"
//#include "address_exchange.h"
#include <iostream>
#include <cstdio>
#include <pthread.h>

#include <atomic>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netdb.h>
#include <zmq.h>
#include <cstring>
#include <cerrno>
#include <string>
#include <sstream>
#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_errno.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_rma.h>
#include <rdma/fi_tagged.h>

#define FT_PRINTERR(call, retv)						\
  do { fprintf(stderr, call "(): %s:%d, ret=%d (%s)\n", __FILE__, __LINE__, \
	       (int) retv, fi_strerror((int) -retv)); } while (0)

class FabFabric : public Fabric {
public:
  FabFabric();
  virtual ~FabFabric();
    
  void register_options(Realm::CommandLineParser &cp);
  bool init(bool manually_set_addresses = false);
  void shutdown();
  void synchronize_clocks();
  void fatal_shutdown(int code);
  NodeId get_id();
  uint32_t get_num_nodes();
  void put_bytes(NodeId target, off_t offset, const void* src, size_t len);
  void get_bytes(NodeId target, off_t offset, void* dst, size_t len);
  void* get_regmem_ptr();
  size_t get_regmem_size_in_mb();
  int send(Message* m);
  Realm::Event* gather_events(Realm::Event& event, NodeId root);
  void broadcast_events(Realm::Event& event, NodeId root);
  void recv_gather_event(Realm::Event& event, NodeId sender);
  void recv_broadcast_event(Realm::Event& event, NodeId sender);
  void barrier_wait(uint32_t barrier_id);
  void barrier_notify(uint32_t barrier_id);
  void recv_barrier_notify(uint32_t barrier_id, NodeId sender);
  void recv_rdma_info(NodeId sender, uint64_t key, uint64_t desc);
  void wait_for_rdmas();

  bool incoming(Message *);
  void *memalloc(size_t size);
  void memfree(void *);
  void print_fi_info(fi_info* fi);
  void wait_for_shutdown();
    
  static std::string fi_cq_error_str(const int ret, fid_cq* cq);
  static std::string fi_error_str(const int ret, const std::string call,
				  const std::string file, const int line);
  size_t get_max_send(); 
  size_t get_iov_limit();
  virtual size_t get_iov_limit(MessageId id);
  std::string tostr();

  // Reset the ENTIRE address vector with a new one. Primarily for
  // testing
  int set_address_vector(void* addrs, size_t addrlen, NodeId new_id, uint32_t new_num_nodes);
  size_t get_address(char buf[64]);

protected:
  // parameters
  NodeId	id;
  uint32_t	num_nodes;
  int	        max_send;
  int	        pend_num;
  size_t        regmem_size_in_mb;

    
  // Fabric objects
  bool initialized;
  struct fid_fabric* fab;
  struct fid_domain* dom;
  struct fid_eq* eq;
  struct fid_cq* rx_cq;
  struct fid_cq* tx_cq;
  struct fid_cntr* cntr;
  struct fid_ep* ep;
  struct fid_av* av;
  struct fi_context* avctx;
  struct fi_info* fi;
  struct fid_mr* rdma_mr;
  struct fi_context fi_ctx_write;
  struct fi_context fi_ctx_read;
  std::atomic<size_t> rdmas_initiated; // synchronizes with completion events
  
  fi_addr_t* fi_addrs; // array of addresses in fabric format
  char addr[64];    // this node's address
  size_t addrlen;   // length of addresses in this fabric
  void* regmem_buf; // registered RDMA buffer -- local node may access directly, otherers may RDMA
  uint64_t* keys;   // array of Libfabric memory keys for each node's registered mem
  uint64_t* mr_addrs; // virtual address of each RDMA buffer
  std::atomic<uint64_t> rdma_keys_recvd; // tracks incoming RDMA exchange info

  // Threads -- progress threads execute handlers,
  // tx_handler_thread cleans up sent messages
  int num_progress_threads;
  pthread_attr_t thread_attrs;
  pthread_t* progress_threads;
  pthread_t* tx_handler_thread;
  size_t handler_stacksize_in_mb;
    
  std::atomic<bool> stop_flag;
  bool shutdown_complete;
  std::mutex done_mutex;
    
  int exchange_server_send_port;
  int exchange_server_recv_port;
  std::string exchange_server_host;

  static int check_cq(fid_cq* cq, fi_cq_tagged_entry* ce, int timeout);
    
  int post_tagged(MessageType* mt);
  int post_untagged();
    
  bool init_fail(fi_info* hints, fi_info* qfi, const std::string message) const;
  int setup_pmi();

  void start_progress_threads(const int count, const size_t stack_size);
  void free_progress_threads();
  void progress(bool wait);
  void handle_tx(bool wait);

  static void* bootstrap_progress(void* context);
  static void* bootstrap_handle_tx(void* context);
  static int av_create_address_list(char *first_address, int base, int num_addr,
				    void *addr_array, int offset, int len, int addrlen);
  static int add_address(char* first_address, int index, void* addr);
  void* exchange_addresses();
  void exchange_rdma_info();

  // Uses PMI for address exchange
  PMIAddressExchange pmi_exchange;
  friend class FabMessage;
    
};

class RDMAExchangeMessageType : public MessageType {
public:
  RDMAExchangeMessageType()
    : MessageType(RDMA_EXCHANGE_MSGID, sizeof(RequestArgs), false, true) { }

  struct RequestArgs {
    RequestArgs(NodeId _sender, uint64_t _key, uint64_t _addr)
      : sender(_sender), key(_key), addr(_addr) { }
    NodeId sender;
    uint64_t key;
    uint64_t addr;
  };
  
  void request(Message* m);
};

class RDMAExchangeMessage : public Message {
public:
  RDMAExchangeMessage(NodeId dest, NodeId sender, uint64_t key, uint64_t addr)
    : Message(dest, RDMA_EXCHANGE_MSGID, &args, NULL),
      args(sender, key, addr) { }

  RDMAExchangeMessageType::RequestArgs args;  
};


//typedef FabMutex Mutex;
//typedef FabCondVar CondVar;

#endif // ifndef FABRIC_LIBFABRIC_H
