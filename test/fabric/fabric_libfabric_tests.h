/* 
   Henry Cooney <hacoo36@gmail.com> <Github: hacoo>
   27 June 2016
   legion/runtime/libfabric/fabric_libfabric_tests.h

   Simple tests for libfabric implementation. Tests fabric only,
   not interaction with Legion. */


#include <iostream>
#include <cassert>
#include "fabric.h"
#include "libfabric/fabric_libfabric.h"


class FabTester {

public:
FabTester() : fabric(NULL) {};
~FabTester() {};

int run();
int init();

private:
FabFabric* fabric;

};





