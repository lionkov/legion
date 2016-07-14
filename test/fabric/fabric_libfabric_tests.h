/* 
   Henry Cooney <hacoo36@gmail.com> <Github: hacoo>
   27 June 2016
   legion/runtime/libfabric/fabric_libfabric_tests.h

   Simple tests for libfabric implementation. Tests fabric only,
   not interaction with Legion. */


#include <iostream>
#include <cassert>
#include <unistd.h>
#include "fabric.h"
#include "libfabric/fabric_libfabric.h"


class FabTester {

public:
FabTester() {}
~FabTester() {}

int run();
int init();

private:

};

class TestMessageType : public MessageType {
 public: 
 TestMessageType()
   : MessageType(1, /* msgId */
		 64, /* arg size */
		 false, /* has payload */
		 true /*in order */ ){ }

  void request(Message* m);
};

class TestMessage : public FabMessage {
 public:
 TestMessage(NodeId dest, void* args)
   : FabMessage(dest, 1, args, NULL) { }
};


class TestPayloadMessageType : public MessageType {
 public: 
 TestPayloadMessageType()
   : MessageType(2, /* msgId */
		 64, 
		 true, /* has payload */
		 true /*in order */ ){ }

  void request(Message* m);
};

class TestPayloadMessage : public FabMessage {
 public:
 TestPayloadMessage(NodeId dest, void* args, FabPayload* payload)
   : FabMessage(dest, 2, args, payload) { }
};
