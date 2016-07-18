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


void print_strided(void* buf, int linesz, int linecnt, int stride) {
  assert(stride != 0); 
  char* p = (char*) buf;
  
  for (int i=0; i<linecnt; i+=stride) {
    for (int j=0; j<linesz; ++j) {
      std::cout << (char) p[i*linesz+j];
    }
    std::cout << std::endl;
  }
}


// global from fabric.h
Fabric* fabric = NULL;

int FabTester::init() {
  fabric = new FabFabric();

  std::cout << "Adding message types... " << std::endl;
  fabric->add_message_type(new TestMessageType(), "Test Message");
  fabric->add_message_type(new TestPayloadMessageType(), "Test Payload Message");
  fabric->add_message_type(new TestTwoDPayloadMessageType(), "Test 2D Payload Message");
  fabric->add_message_type(new TestArglessTwoDPayloadMessageType(), "Test Argless 2D Payload Message");
  
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
  std::cout << "iov_limit: " << fabric->get_iov_limit() << std::endl;
  std::cout << "iov_limit: " << fabric->get_iov_limit(fabric->mts[4]) << std::endl;

  int st = 0;
  int count = 0;
  
  while (count < 10) {
    char buf[64];
    strcpy(buf, "I'm an arg.");

    void* paybuf = malloc(64);
    strcpy((char*) paybuf, "This is a payload.");

    char* twodbuf = new char[64];
    for (int i=0; i < 8; ++i) {
      for (int j=0; j < 4; ++j) {
	twodbuf[i*4+j] = 48+i;
      }
    }

    TestTwoDPayloadMessageType::RequestArgs* twodargs
      = new TestTwoDPayloadMessageType::RequestArgs();
    
    twodargs->linesz = 4;
    twodargs->linecnt = 6;
    twodargs->stride = 1;
    
    int mode = FAB_PAYLOAD_FREE;
    switch (mode) { 
    case FAB_PAYLOAD_KEEP:
      std::cout << "MODE: KEEP" << std::endl;
      break;
    case FAB_PAYLOAD_COPY:
      std::cout << "MODE: COPY" << std::endl;
      break;
    case FAB_PAYLOAD_FREE:
      std::cout << "MODE: FREE" << std::endl;
      break;
    }
    
    FabContiguousPayload* payload
      = new FabContiguousPayload(mode, (void*) paybuf, 64);

    FabTwoDPayload* twodpayload
      = new FabTwoDPayload(mode, twodbuf,
			   twodargs->linesz,
			   twodargs->linecnt,
			   twodargs->stride);
    
    std::cout << "Sending payload message..." << std::endl;
    ret = fabric->send(new TestTwoDPayloadMessage(fabric->get_id(), (void*) twodargs, twodpayload));
    std::cout << "retcode: " << ret << std::endl;

    std::cout << "Sending argless payload message..." << std::endl;
    ret = fabric->send(new TestArglessTwoDPayloadMessage(fabric->get_id(), twodpayload));
    std::cout << "retcode: " << ret << std::endl;

    sleep(st);
    /*
    std::cout << "Sending test message... " << std::endl;
    ret = fabric->send(new TestMessage(fabric->get_id(), &buf));
    std::cout << "retcode: " << ret << std::endl;
    sleep(st);
    */
    ++count;
  }

  //fabric->shutdown();
  fabric->wait_for_shutdown();
  
  std::cout << std::endl << std::endl << "Done." << std::endl;
  return 0;
}

void FabTester::testFabTwoDPayload() {
  char* buf = new char[64];
  for (int i=0; i < 8; ++i) {
    for (int j=0; j < 4; ++j) {
      buf[i*4+j] = 48+i;
    }
  }
  
  //print_strided(buf, 4, 8, 1);
  FabTwoDPayload* payload = new FabTwoDPayload(FAB_PAYLOAD_COPY,
					       buf,
					       4,
					       8,
					       1);

  print_strided(payload->ptr(), 4, 8, 1);
    
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

void TestTwoDPayloadMessageType::request(Message* m) {
  RequestArgs* args = (RequestArgs*) m->args;
  
  std::cout << "TestTwoDPayloadMessageType::request called" << std::endl;
  std::cout << "linesize: " << args->linesz << "\n"
	    << "linecnt: " << args->linecnt << "\n"
	    << "stride: " << args->stride << std::endl;
  std::cout << "Payload: " << (char*) m->payload->ptr()
	    << std::endl;
}

void TestArglessTwoDPayloadMessageType::request(Message* m) {
  std::cout << "TestArglessTwoDPayloadMessageType::request called" << std::endl;
  std::cout << "Payload: " << (char*) m->payload->ptr()
	    << std::endl;
}
