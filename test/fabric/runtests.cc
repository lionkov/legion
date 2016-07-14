/* 
   Henry Cooney <hacoo36@gmail.com> <Github: hacoo>
   27 June 2016
   legion/test/fabric/runtests.cc

   Simple tests for libfabric implementation. Tests fabric only,
   not interaction with Legion. */

#include <iostream>
#include "fabric_libfabric_tests.h"

int main() {
  
  FabTester tester;
  tester.init();
  tester.run();
    
  return 0;
}






