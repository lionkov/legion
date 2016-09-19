/* 
   Henry Cooney <hacoo36@gmail.com> <Github: hacoo>
   27 June 2016
   legion/runtime/libfabric/fabric_libfabric_tests.h

   Simple tests for libfabric implementation. Tests fabric only,
   not interaction with Legion. 

   Each FabTester represents a single testing runtime. You can pass in the NodeId 
   on initialization and set address vectors to manually create a local fabric,
   or allow a standard Fabric address exchange if running on multiple nodes.
   
*/


#include <iostream>
#include <cassert>
#include <unistd.h>
#include <string>
#include <vector>
#include "fabric.h"
#ifdef USE_GASNET
#include "gasnet_fabric.h"
#endif
#include "libfabric/fabric_libfabric.h"
#include "cmdline.h"
#include "threads.h"

void print_strided(void* buf, int linesz, int linecnt, int stride);
size_t fill_spans(SpanList& sl);

class FabTester {
public:
  FabTester() { };
  ~FabTester() { };
 
  int run();

  // Initialize network automatically using the exchange server
  int init(std::vector<std::string> cmdline,
	   bool manually_set_address, int argc, char* argv[]);
  
  void testFabTwoDPayload();
  int test_message_loopback();
  int test_message_pingpong(int runs);
  int test_gather(int runs);
  int test_broadcast(int runs);
  int test_barrier(int runs);
  int test_rdma(int runs);
  void wait_for_shutdown() {
    fabric->wait_for_shutdown();
    delete fabric;
  }
  
protected:
  void add_message_types();
};

class TestMessageType : public MessageType {
public: 
  TestMessageType()
    : MessageType(TEST_MSGID, /* msgId */
		  sizeof(RequestArgs),
		  false, /* has payload */
		  true /*in order */ ){ }
  
  struct RequestArgs {
    char string[64];
  };

  void request(Message* m);
};

class TestMessage : public Message {
public:
  TestMessage(NodeId dest, char* s)
    : Message(dest, TEST_MSGID, &args, NULL) {
    strncpy(args.string, s, 64);
  }

  TestMessageType::RequestArgs args;
};


class TestPayloadMessageType : public PayloadMessageType {
public: 
  TestPayloadMessageType()
    : PayloadMessageType(TEST_PAYLOAD_MSGID, /* msgId */
			 sizeof(RequestArgs),
			 true, /* has payload */
			 true /*in order */ ) { }
  
  struct RequestArgs : public BaseMedium {
    //char string[64];
    int bogus;
    int bogosity;
  };
  
  void request(Message* m);
};

class TestPayloadMessage : public Message {
public:
  TestPayloadMessage(NodeId dest, char* s, FabPayload* payload)
    : Message(dest, TEST_PAYLOAD_MSGID, &args, payload) {
  }
  TestPayloadMessageType::RequestArgs args;
};


class TestTwoDPayloadMessageType : public PayloadMessageType {
public: 
  TestTwoDPayloadMessageType()
    : PayloadMessageType(TEST_TWOD_MSGID, /* msgId */
			 sizeof(RequestArgs),
			 true, /* has payload */
			 true /*in order */ ){ }

  struct RequestArgs : public BaseMedium {
    size_t linesz;
    size_t linecnt;
    ptrdiff_t stride;
  };

  void request(Message* m);
};

class TestTwoDPayloadMessage : public Message {
public:
  TestTwoDPayloadMessage(NodeId dest,
			 size_t linesz,
			 size_t linecnt,
			 ptrdiff_t stride,
			 FabPayload* payload)
    : Message(dest, TEST_TWOD_MSGID, &args, payload) {
    args.linesz = linesz;
    args.linecnt = linecnt;
    args.stride = stride;    
  }
  
  TestTwoDPayloadMessageType::RequestArgs args;
};


class TestSpanPayloadMessageType : public PayloadMessageType {
public: 
  TestSpanPayloadMessageType()
    : PayloadMessageType(TEST_SPAN_MSGID, /* msgId */
			 sizeof(RequestArgs),
			 true, /* has payload */
			 true /*in order */ ){ }

  struct RequestArgs : public BaseMedium{
    size_t spans;
    NodeId sender;
  };

  void request(Message* m);
};

class TestSpanPayloadMessage : public Message {
public:
  TestSpanPayloadMessage(NodeId dest,
			 size_t spans,
			 NodeId sender,
			 FabPayload* payload)
    : Message(dest, TEST_SPAN_MSGID, &args, payload) {
    args.spans = spans;
    args.sender = sender;
  }
  TestSpanPayloadMessageType::RequestArgs args;
};


class PingPongMessageType : public PayloadMessageType {
public: 
  PingPongMessageType()
    : PayloadMessageType(TEST_PINGPONG_MSGID, /* msgId */
			 sizeof(RequestArgs),
			 true, /* has payload */
			 true /*in order */ ){ }

  struct RequestArgs : public BaseMedium {
    NodeId sender;
    bool* ack_table;
  };
  
  void request(Message* m);
};

class PingPongMessage : public Message {
public:
  PingPongMessage(NodeId dest, NodeId sender, bool* ack_table, FabPayload* payload)
    : Message(dest, TEST_PINGPONG_MSGID, &args, payload) {
    args.sender = sender;
    args.ack_table = ack_table;
  }

  PingPongMessageType::RequestArgs args;
};



class PingPongAckType : public PayloadMessageType {
public: 
  PingPongAckType()
    : PayloadMessageType(TEST_PINGPONG_ACK_MSGID, /* msgId */
			 sizeof(RequestArgs),
			 true, /* has payload */
			 true /*in order */ ){ }

  struct RequestArgs : public BaseMedium {
    NodeId sender;
    bool* ack_table;
  };

  void request(Message* m);
};

class PingPongAck : public Message {
public:
  PingPongAck(NodeId dest, NodeId sender, bool* ack_table, FabPayload* payload)
    : Message(dest, TEST_PINGPONG_ACK_MSGID, &args, payload) {
    args.sender = sender;
    args.ack_table = ack_table;
  }

  PingPongAckType::RequestArgs args;
};
