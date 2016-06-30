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
  fabric = new FabFabric;

  std::cout << "Adding message types... " << std::endl;
  fabric->add_message_type(new TestMessageType());
  
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

  while (true) {
    std::cout << "Sending... " << std::endl;
    ret = fabric->send(new TestMessage(fabric->get_id(), NULL));
    std::cout << "retcode: " << ret << std::endl;
    sleep(1);
  }

  fabric->wait_for_shutdown();
  
  std::cout << std::endl << std::endl << "Done." << std::endl;
  return 0;
}

void TestMessageType::request(Message* m) {
  std::cout << "THIS IS A TEST" << std::endl;\
  std::cout << "HI I'M A MESSAGE!!" << std::endl;
}



