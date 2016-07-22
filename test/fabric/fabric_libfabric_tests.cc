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

int FabTester::init(std::vector<std::string> cmdline) {
  fabric = new FabFabric();
  Realm::CommandLineParser cp;
  fabric->register_options(cp);
  bool cmdline_ok = cp.parse_command_line(cmdline);

  if (!cmdline_ok) {
    std::cout << "ERROR -- failed to parse command line options" << std::endl;
    exit(1);
  }
  

  std::cout << "Adding message types... " << std::endl;
  fabric->add_message_type(new TestMessageType(), "Test Message");
  fabric->add_message_type(new TestPayloadMessageType(), "Test Payload Message");
  fabric->add_message_type(new TestTwoDPayloadMessageType(), "Test 2D Payload Message");
  fabric->add_message_type(new TestArglessTwoDPayloadMessageType(), "Test Argless 2D Payload Message");
  fabric->add_message_type(new TestSpanPayloadMessageType(), "Test Span Payload Message");
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
  std::cout << "iov_limit: " << fabric->get_iov_limit(4) << std::endl;

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
    twodargs->linecnt = 6 ;
    twodargs->stride = 1;

    
    TestSpanPayloadMessageType::RequestArgs* spanargs
      = new TestSpanPayloadMessageType::RequestArgs();

    spanargs->spans = 3;
    spanargs->sender = fabric->get_id();
    
    
    int mode = FAB_PAYLOAD_KEEP;
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

    SpanList* sl = new SpanList();
    fill_spans(*sl);
    FabSpanPayload* spanpayload =
      new FabSpanPayload(mode, *sl);

    NodeId target = (fabric->get_id() + 1) % fabric->get_num_nodes();

    /*
    std::cout << "Sending Contiguous payload message... " << std:: endl;
    ret = fabric->send(new TestPayloadMessage(fabric->get_id(), (void*) buf, payload));
    std::cout << "retcode: " << ret << std::endl << std::endl;
    
    std::cout << "Sending 2D payload message..." << std::endl;
    ret = fabric->send(new TestTwoDPayloadMessage(fabric->get_id(), (void*) twodargs, twodpayload));
    std::cout << "retcode: " << ret << std::endl << std::endl;

    std::cout << "Sending argless 2D payload message..." << std::endl;
    ret = fabric->send(new TestArglessTwoDPayloadMessage(fabric->get_id(), twodpayload));
    std::cout << "retcode: " << ret << std::endl << std::endl;
    */
    std::cout << "Node " << fabric->get_id() << " sending to: " << target << "..." << std::endl;
    ret = fabric->send(new TestSpanPayloadMessage(target, spanargs, spanpayload));
    std::cout << "retcode: " << ret << std::endl << std::endl;


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

// Puts some junk in a span list
void FabTester::fill_spans(SpanList& sl) {
  char* buf1 = new char[64];
  char* buf2 = new char[32];
  char* buf3 = new char[16];

  strcpy(buf1, "This is span 1.");
  strcpy(buf2, "This is span 2.");
  strcpy(buf3, "This is span 3.");

  sl.push_back(FabSpanListEntry(buf1, 64));
  sl.push_back(FabSpanListEntry(buf2, 32));
  sl.push_back(FabSpanListEntry(buf3, 16));
}

void TestMessageType::request(Message* m) {
  std::cout << "TestMessageType::request() called" << std::endl;
  std::cout << "Args: " << (char*) m->args << std::endl << std::endl;
}

void TestPayloadMessageType::request(Message* m) {
  std::cout << "TestPayloadMessageType::request called" << std::endl;
  std::cout << "Args: " << (char*) m->args << std::endl;
  std::cout << "Payload: " << (char*) m->payload->ptr()
	    << std::endl << std::endl;
}

void TestTwoDPayloadMessageType::request(Message* m) {
  RequestArgs* args = (RequestArgs*) m->args;
  
  std::cout << "TestTwoDPayloadMessageType::request called" << std::endl;
  std::cout << "linesize: " << args->linesz << "\n"
	    << "linecnt: " << args->linecnt << "\n"
	    << "stride: " << args->stride << std::endl;
  std::cout << "Payload: " << (char*) m->payload->ptr()
	    << std::endl << std::endl;
}

void TestArglessTwoDPayloadMessageType::request(Message* m) {
  std::cout << "TestArglessTwoDPayloadMessageType::request called" << std::endl;
  std::cout << "Payload: " << (char*) m->payload->ptr()
	    << std::endl << std::endl;
}


void TestSpanPayloadMessageType::request(Message* m) {
  RequestArgs* args = (RequestArgs*) m->args;
  
  std::cout << "TestSpanPayloadMessageType::request called" << std::endl;
  std::cout << "spans (args): " << args->spans << std::endl;
  std::cout << "sender (args): " << args->sender << std::endl;
  std::cout << "size: " << m->payload->size() << std::endl;
  std::cout << "Payload:" << std::endl;
  std::cout << (char*) m->payload->ptr() << std::endl;
  std::cout << (char*) m->payload->ptr()+64 << std::endl;
  std::cout << (char*) m->payload->ptr()+96 << std::endl;
  std::cout << std::endl;
}
