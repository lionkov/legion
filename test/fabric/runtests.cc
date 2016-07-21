/* 
   Henry Cooney <hacoo36@gmail.com> <Github: hacoo>
   27 June 2016
   legion/test/fabric/runtests.cc

   Simple tests for libfabric implementation. Tests fabric only,
   not interaction with Legion. */

#include <iostream>
#include <string>
#include <vector>

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

  for (int i = 1; i < argc; ++i)
    cmdline.push_back(argv[i]);
  
  FabTester tester;
  tester.init(cmdline);
  tester.run();
    
  return 0;
}






