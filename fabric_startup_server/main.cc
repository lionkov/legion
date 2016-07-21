// Temporary server for exchanging Fabric addresses.
// All runtimes should connect to this server, which will
// wait until the expected number of clients have connected
// and then send out addresses.

#include <zmq.h>
#include <iostream>
#include <string>
#include <sstream>
#include <cstdlib>
#include <cstring>
#include <cassert>
#include <csignal>

#define RECV_PORT 8080
#define SEND_PORT 8081

using namespace std;

void* context;
void* receiver;
void* sender;

void unexpected_shutdown(int signum) {
  printf("INTERRUPTED, SHUTTING DOWN\n");
  zmq_close(sender);
  zmq_close(receiver);
  zmq_ctx_destroy(context);
  printf("DISCONNECTED OK\n");
  exit(signum);
}

int main(int argc, char* argv[]) {
  int nclients = 2; 
  if (argc > 1) {
    nclients = strtol(argv[1], NULL, 10);
  }
  
  signal(SIGINT, unexpected_shutdown);
  int ret;
  char* addrs = NULL;
  int addrlen;

  context  = zmq_ctx_new();
  receiver = zmq_socket(context, ZMQ_PULL);
  sender   = zmq_socket(context, ZMQ_PUSH);
    
  stringstream sstream;
  sstream << "tcp://*:" << RECV_PORT;
  zmq_bind(receiver, sstream.str().c_str());
  std::cout << "Listening on: " << sstream.str().c_str() << std::endl;
  
  sstream.str("");
  sstream.clear();
  sstream << "tcp://*:" << SEND_PORT;
  zmq_bind(sender, sstream.str().c_str());
  std::cout << "Sending on: " << sstream.str().c_str() << std::endl;
  while(true) {
    int count = 0;
    cout << "Waiting for " << nclients << " runtimes..." << endl;
    

    // Wait for all clients to post their addresses
    while(count < nclients) {
      char buf[256];
      ret = zmq_recv(receiver, buf, 256, 0);
      assert(ret >= 0);
      
      // Let first client decide the address length. If any
      // clients disagree, something has gone wrong
      if (count == 0) {
	addrlen = ret;
	addrs = (char*) malloc(addrlen*nclients);
      } else if (ret != addrlen) {
	std::cerr << "ERROR -- address lengths do not agree. Expected: "
		  << addrlen << " Saw: " << ret << std::endl;
	unexpected_shutdown(ret);
      }
   
      memcpy(addrs+count*addrlen, buf, addrlen);
      
      cout << "Received request from runtime: " << count << endl;
      ++count;
    }

    // Send completed address info to each client
    for(int i = 0; i < nclients; ++i) {
      zmq_send(sender, addrs, addrlen*nclients, 0);
    }

    if(addrs)
      free(addrs);
 
    zmq_close(sender);
    zmq_close(receiver);
    zmq_ctx_destroy(context);
  
    return 0;
  }
}
