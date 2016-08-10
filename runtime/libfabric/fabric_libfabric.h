#ifndef FABRIC_LIBFABRIC_H
#define FABRIC_LIBFABRIC_H
#include "fabric.h"
#include "cmdline.h"
#include <iostream>
#include <cstdio>
#include <pthread.h>
//#include <stdatomic.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netdb.h>
#include <zmq.h>

//#include "pmi.h"
//#include "pmix.h"
#include <cstring>
#include <vector>
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

#define FT_PRINTERR(call, retv) \
	do { fprintf(stderr, call "(): %s:%d, ret=%d (%s)\n", __FILE__, __LINE__, \
			(int) retv, fi_strerror((int) -retv)); } while (0)


class FabMutex {
 public:
  FabMutex(void) { pthread_mutex_init(&_lock, NULL); } 
    ~FabMutex(void) { pthread_mutex_destroy(&_lock); }
    
    void lock(void) { pthread_mutex_lock(&_lock); }
    void unlock(void) { pthread_mutex_unlock(&_lock); }
    
  protected:
    friend class FabCondVar;
    pthread_mutex_t _lock;

  private:
    // Should never be copied
    FabMutex(const FabMutex& m) { assert(false); }
    FabMutex& operator=(const FabMutex &m) { assert(false); return *this; }
  };

  class FabCondVar {
  public:
    FabCondVar(FabMutex &m) : mutex(m) { pthread_cond_init(&cond, NULL); }
    ~FabCondVar(void) { pthread_cond_destroy(&cond); }
    void signal(void) { pthread_cond_signal(&cond); }
    void broadcast(void) { pthread_cond_broadcast(&cond); }
    void wait(void) { pthread_cond_wait(&cond, &mutex._lock); }
    FabMutex& get_mutex() { return mutex; };
    

  protected:
    FabMutex &mutex;
    pthread_cond_t cond;
    
  };


  // AutoLocks wrap a mutex. On creation, the mutex is automatically acquired,
  // when the AutoLock is destroyed, the mutex is released.
  class FabAutoLock {
  public:
  FabAutoLock(FabMutex& _mutex) : mutex(_mutex), held(true) {}
    ~FabAutoLock();
    void release();
    void reacquire();

  protected:
    FabMutex& mutex;
    bool held;
  };


  class FabFabric : public Fabric {
  public:
    FabFabric();
    ~FabFabric();
    
    void register_options(Realm::CommandLineParser &cp);
    bool add_message_type(MessageType *mt, const std::string tag);
    bool init(bool manually_set_addresses = false);
    void shutdown();
    NodeId get_id();
    uint32_t get_num_nodes();
    int send(Message* m);
    Realm::Event* gather_events(Realm::Event& event, NodeId root);
    void broadcast_events(Realm::Event& event, NodeId root);
    void recv_gather_event(Realm::Event& event, NodeId sender);
    void recv_broadcast_event(Realm::Event& event, NodeId sender);
    bool incoming(Message *);
    void *memalloc(size_t size);
    void memfree(void *);
    void print_fi_info(fi_info* fi);
    void wait_for_shutdown();
    
    static std::string fi_cq_error_str(const int ret, fid_cq* cq);
    static std::string fi_error_str(const int ret, const std::string call,
				    const std::string file, const int line);
    int get_max_send(); 
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

    
    // Fabric objects
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
    fi_addr_t* fi_addrs; // array of addresses in fabric format
    char addr[64]; // this node's address
    size_t addrlen; // length of addresses in this fabric

    // Threads -- progress threads execute handlers,
    // tx_handler_thread cleans up sent messages
    int num_progress_threads;
    pthread_attr_t thread_attrs;
    pthread_t* progress_threads;
    pthread_t* tx_handler_thread;
    size_t stacksize_in_mb;
    
    bool stop_flag;
    
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
    friend class FabMessage;
    
  };

  //typedef FabMutex Mutex;
  //typedef FabCondVar CondVar;

#endif // ifndef FABRIC_LIBFABRIC_H
