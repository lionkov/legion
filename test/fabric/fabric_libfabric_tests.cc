/* 
   Henry Cooney <hacoo36@gmail.com> <Github: hacoo>
   27 June 2016
   legion/runtime/libfabric/fabric_libfabric_tests.cc

   Simple tests for libfabric implementation. Tests fabric only,
   not interaction with Legion. */



#include "fabric_libfabric_tests.h"

/* Create a fabric object, register messages, run a test.
   
   Inputs: none
   Returns: 0 on success, otherwise an error code.

   Error codes:

*/


// global from fabric.h
Fabric* fabric = NULL;

int FabTester::init() {
  fabric = new FabFabric();

  std::cout << "Adding message types... " << std::endl;
  fabric->add_message_type(new TestMessageType(), "Test Message");
  fabric->add_message_type(new TestPayloadMessageType(), "Test Payload Message");
  
  bool ret;
  ret = fabric->init();
  
  if (!ret) {
    std::cout << "ERROR -- Fabric init failed." << std::endl;
    assert(false);
  }
  
  std::cout << "Test Fabric object created." << std::endl;
    
  return 0;
}


/* Run tests on the fabric object. Must be called after init.

   inputs: none
   returns: 0 on success, otherwise an error code.

   Error codes:
   
*/
   
int FabTester::run() {
  int ret;
  std::cout << "Attempting to send a message. You should see some output. " << std::endl << std::endl;

  int st = 0;
  int count = 0;
  
  while (count < 1) {
    char buf[64];
    strcpy(buf, "I'm an arg.");

    char paybuf[64];
    strcpy(paybuf, "This is a payload.");
    FabContiguousPayload payload(FAB_PAYLOAD_COPY, &paybuf, sizeof(paybuf));
    
    std::cout << "Sending payload message..." << std::endl;
    ret = fabric->send(new TestPayloadMessage(fabric->get_id(), &buf, &payload));
    std::cout << "retcode: " << ret << std::endl;
    sleep(st);
    
    std::cout << "Sending test message... " << std::endl;
    ret = fabric->send(new TestMessage(fabric->get_id(), &buf));
    std::cout << "retcode: " << ret << std::endl;
    sleep(st);

    ++count;
  }

  fabric->shutdown();
  fabric->wait_for_shutdown();
  
  std::cout << std::endl << std::endl << "Done." << std::endl;
  return 0;
}

void TestMessageType::request(Message* m) {
  std::cout << "TestMessageType::request() called" << std::endl;
  std::cout << "Args: " << (char*) m->args << std::endl;
}

void TestPayloadMessageType::request(Message* m) {
  std::cout << "TestPayloadMessageType::request called" << std::endl;
  std::cout << "Args: " << (char*) m->args << std::endl;
  std::cout << "Payload: " << (char*) m->payload->ptr()
	    << std::endl;
}



