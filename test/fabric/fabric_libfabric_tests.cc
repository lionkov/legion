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

Fabric* fabric = NULL;

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

// Initialize the fabric. If manually_set_addresses is true, instead of exchanging addresses,
// you will need to pass in an address vector before using this fabric.
int FabTester::init(std::vector<std::string> cmdline, bool manually_set_addresses) {
  fabric = new FabFabric();
  Realm::CommandLineParser cp;
  fabric->register_options(cp);
  bool cmdline_ok = cp.parse_command_line(cmdline);

  if (!cmdline_ok) {
    std::cout << "ERROR -- failed to parse command line options" << std::endl;
    exit(1);
  }
  
  add_message_types();
  bool ret;
  ret = fabric->init(manually_set_addresses);
  
  if (!ret) {
    std::cout << "ERROR -- Fabric init failed." << std::endl;
    assert(false);
  }
    std::cout << "Test Fabric object created." << std::endl;
    
  return 0;
}


void FabTester::add_message_types() {
  fabric->add_message_type(new TestMessageType(), "Test Message");
  fabric->add_message_type(new TestPayloadMessageType(), "Test Payload Message");
  fabric->add_message_type(new TestTwoDPayloadMessageType(), "Test 2D Payload Message");
  fabric->add_message_type(new TestArglessTwoDPayloadMessageType(), "Test Argless 2D Payload Message");
  fabric->add_message_type(new TestSpanPayloadMessageType(), "Test Span Payload Message");
}

/*
  Run tests on the fabric object. Must be called after init.

   inputs: none
   returns: 0 on success, otherwise returns the number of failed tests
   
*/
   
int FabTester::run() {
  int errors = 0;
  /*
  std::cout << std::endl << std::endl << "running: test_message_loopback" << std::endl;
  if (test_message_loopback() != 0) {
    errors += 1;
    std::cout << "ERROR -- test_message_loopback -- FAILED" << std::endl;    
  } else {
    std::cout << "test_message_loopback -- OK" << std::endl;    
  }
  */
  std::cout << std::endl << std::endl << "running: test_gather_local" << std::endl;
  if (test_gather() != 0) {
    errors += 1;
    std::cout << "ERROR -- test_gather_local -- FAILED" << std::endl;    
  } else {
    std::cout << "test_gather_local -- OK" << std::endl;    
  }
    
  return errors;
}

int FabTester::test_message_loopback() {
  
  int ret;
  std::cout << "Attempting to send a message. You should see some output. " << std::endl << std::endl;
  std::cout << "iov_limit: " << fabric->get_iov_limit() << std::endl;
  std::cout << "iov_limit: " << fabric->get_iov_limit(4) << std::endl;

  int st = 0;
  int count = 0;
  
  while (count < 10) {
    void* paybuf = malloc(64);
    strcpy((char*) paybuf, "This is a payload.");
    
    char* twodbuf = new char[64];
    for (int i=0; i < 8; ++i) {
      for (int j=0; j < 4; ++j) {
	twodbuf[i*4+j] = 48+i;
      }
    }
    int mode = FAB_PAYLOAD_COPY;
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

    size_t linesz = 4;
    size_t linecnt = 6;
    ptrdiff_t stride = 1;
    FabTwoDPayload* twodpayload
      = new FabTwoDPayload(mode, twodbuf,
    			   linesz,
    			   linecnt,
    			   stride);


    SpanList* sl = new SpanList();
    size_t nspans = fill_spans(*sl);
    FabSpanPayload* spanpayload =
      new FabSpanPayload(mode, *sl);

    NodeId target = (fabric->get_id() + 1) % fabric->get_num_nodes();

    
    std::cout << "Node " << fabric->get_id() << " sending to: " << target << "..." << std::endl;
    ret = fabric->send(new TestMessage(fabric->get_id(), "I'm an arg!"));
    std::cout << "retcode: " << ret << std::endl << std::endl;

    std::cout << "Node " << fabric->get_id() << " sending to: " << target << "..." << std::endl;
    ret = fabric->send(new TestPayloadMessage(fabric->get_id(), "I'm an arg!", payload));
    std::cout << "retcode: " << ret << std::endl << std::endl;
     
    std::cout << "Node " << fabric->get_id() << " sending to: " << target << "..." << std::endl;
    ret = fabric->send(new TestTwoDPayloadMessage(fabric->get_id(), linesz, linecnt,
    stride, twodpayload));
    std::cout << "retcode: " << ret << std::endl << std::endl;
   
    std::cout << "Node " << fabric->get_id() << " sending to: " << target << "..." << std::endl;
    ret = fabric->send(new TestSpanPayloadMessage(target, nspans, fabric->get_id(),
						  spanpayload));
    std::cout << "retcode: " << ret << std::endl << std::endl;


    sleep(st);
    ++count;
    if (mode == FAB_PAYLOAD_COPY) {
      free(paybuf);
      delete[] twodbuf;
      
      // deallocate sl contents
      for(SpanList::const_iterator it = sl->begin(); it != sl->end(); it++) {
	if (it->first)
	  free((void*) it->first);
      }

      delete sl;
    }
  }

  //fabric->shutdown();
  sleep(3);
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

// Puts some junk in a span list, returns number of spans added
size_t fill_spans(SpanList& sl) {
  char* buf1 = (char*) malloc(64);
  char* buf2 = (char*) malloc(64);
  char* buf3 = (char*) malloc(64);

  strcpy(buf1, "This is span 1.");
  strcpy(buf2, "This is span 2.");
  strcpy(buf3, "This is span 3.");

  sl.push_back(FabSpanListEntry(buf1, 64));
  sl.push_back(FabSpanListEntry(buf2, 32));
  sl.push_back(FabSpanListEntry(buf3, 16));

  return 3;
}

void TestMessageType::request(Message* m) {
  std::cout << "TestMessageType::request() called" << std::endl;
  RequestArgs* args = (RequestArgs*) m->get_arg_ptr();
  std::cout << "Args: " << args->string << std::endl << std::endl;
}

void TestPayloadMessageType::request(Message* m) {
  std::cout << "TestPayloadMessageType::request called" << std::endl;
  RequestArgs* args = (RequestArgs*) m->get_arg_ptr();
  std::cout << "Args: " << args->string << std::endl;
  std::cout << "Payload: " << (char*) m->payload->ptr()
	    << std::endl << std::endl;
}

void TestTwoDPayloadMessageType::request(Message* m) {
  RequestArgs* args = (RequestArgs*) m->get_arg_ptr();
  
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
  RequestArgs* args = (RequestArgs*) m->get_arg_ptr();
  
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


// Perform an Event gather from all other nodes in the system
// onto node 0.
int FabTester::test_gather() {
  Realm::Event e;
  e.id = fabric->get_id();
  std::cout << "Created event with ID: " << e.id << std::endl;
  Realm::Event* events = fabric->gather_events(e, 0);
  std::cout << "Node " << fabric->get_id() << ": gather complete" << std::endl;
  
  
    
  return 1;
}
