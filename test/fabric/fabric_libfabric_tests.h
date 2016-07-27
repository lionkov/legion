/* 
   Henry Cooney <hacoo36@gmail.com> <Github: hacoo>
   27 June 2016
   legion/runtime/libfabric/fabric_libfabric_tests.h

   Simple tests for libfabric implementation. Tests fabric only,
   not interaction with Legion. */


#include <iostream>
#include <cassert>
#include <unistd.h>
#include <string>
#include <vector>
#include "fabric.h"
#include "libfabric/fabric_libfabric.h"
#include "cmdline.h"


void print_strided(void* buf, int linesz, int linecnt, int stride);

class FabTester {

public:
  FabTester() {}
  ~FabTester() {}
 
 int run();
 int init(std::vector<std::string> cmdline);
 void testFabTwoDPayload();
 size_t fill_spans(SpanList& sl);
 
private:

};

class TestMessageType : public MessageType {
 public: 
 TestMessageType()
   : MessageType(1, /* msgId */
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
   : Message(dest, 1, &args, NULL) {
    strncpy(args.string, s, 64);
  }

  TestMessageType::RequestArgs args;
};


class TestPayloadMessageType : public MessageType {
 public: 
 TestPayloadMessageType()
   : MessageType(2, /* msgId */
		 sizeof(RequestArgs),
		 true, /* has payload */
		 true /*in order */ ){ }
  struct RequestArgs {
    char string[64];
  };
  
  void request(Message* m);
};

class TestPayloadMessage : public Message {
 public:
 TestPayloadMessage(NodeId dest, char* s, FabPayload* payload)
   : Message(dest, 2, &args, payload) {
    strncpy(args.string, s, 64);
  }
  TestPayloadMessageType::RequestArgs args;
};


class TestTwoDPayloadMessageType : public MessageType {
 public: 
 TestTwoDPayloadMessageType()
   : MessageType(3, /* msgId */
		 sizeof(RequestArgs),
		 true, /* has payload */
		 true /*in order */ ){ }

  struct RequestArgs {
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
   : Message(dest, 3, &args, payload) {
    args.linesz = linesz;
    args.linecnt = linecnt;
    args.stride = stride;    
  }
  
  TestTwoDPayloadMessageType::RequestArgs args;
};

class TestArglessTwoDPayloadMessageType : public MessageType {
 public: 
 TestArglessTwoDPayloadMessageType()
   : MessageType(4, /* msgId */
		 0,
		 true, /* has payload */
		 true /*in order */ ){ }

  void request(Message* m);
};


class TestArglessTwoDPayloadMessage : public Message {
 public:
 TestArglessTwoDPayloadMessage(NodeId dest, FabPayload* payload)
    : Message(dest, 4, NULL, payload) { }
};


class TestSpanPayloadMessageType : public MessageType {
 public: 
 TestSpanPayloadMessageType()
   : MessageType(5, /* msgId */
		 sizeof(RequestArgs),
		 true, /* has payload */
		 true /*in order */ ){ }

  struct RequestArgs {
    RequestArgs(size_t _spans, NodeId _sender)
    : spans(_spans), sender(_sender) { }
    
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
   : Message(dest, 5, &args, payload), args(spans, sender){ }

  TestSpanPayloadMessageType::RequestArgs args;
};
