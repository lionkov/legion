/* 
   Henry Cooney <hacoo36@gmail.com> <Github: hacoo>
   27 June 2016
   legion/test/fabric/runtests.cc

   Simple tests for libfabric implementation. Tests fabric only,
   not interaction with Legion. */

#include <iostream>
#include <string>
#include <vector>

#define NUM_FABRICS 3


#include "fabric.h"
#include "libfabric/fabric_libfabric.cc"
#include "fabric_libfabric_tests.h"

int main(int argc, char* argv[]) {

  std::vector<std::string> cmdline;
  /*
  if(*argc > 1) {
	cmdline.resize(*argc - 1);
	for(int i = 1; i < *argc; i++)
	  cmdline[i - 1] = (*argv)[i];
  }
  */
  /*
  if (argc < 2) {
    std::cout << "Usage: run_tests [y/n] <legion options>" << std::endl;
    exit(1);
  }

  if (strncmp(argv[1], "y", 1) == 0)
    run = true;
  else if (strncmp(argv[1], "n", 1) == 0)
    run = false;
  else {
    std::cout << "Usage: run_tests [y/n] <legion options>" << std::endl;
    exit(1);
  }
  */
  
  FabTester tester;
  
  for (int i = 1; i < argc; ++i)
    cmdline.push_back(argv[i]);

  tester.init(cmdline, false);
  
  tester.run();
  
  tester.wait_for_shutdown();
  
  return 0;
}
