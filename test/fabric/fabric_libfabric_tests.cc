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

int FabTester::init() {
  fabric = new FabFabric;
  fabric->init();

  std::cout << "Test Fabric object created." << std::endl;
  return 0;
}


/* Run tests on the fabric object. Must be called after init.

   inputs: none
   returns: 0 on success, otherwise an error code.

   Error codes:
   
*/
   
int FabTester::run() {
  
  
  
  return 0;
}



