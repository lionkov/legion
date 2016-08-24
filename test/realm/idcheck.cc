// Copyright 2016 Stanford University
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

// test for Realm's IDs

#include "realm/id.h"

#include <iostream>
#include <cassert>
#include <map>
#include <string>

using namespace Realm;

std::map<ID::IDType, ID::IDType> ranges;
std::map<ID::IDType, std::string> names;

int main(int argc, const char *argv[])
{
  bool verbose = true;

  // first check - IDs should always be 8 bytes
  assert(sizeof(ID) == 8);

  assert(ID((ID::IDType)0).is_null());

  // event
  {
    ID lo = ID::make_event(0, 0, 0);
    ID hi = ID::make_event(-1U, -1U, -1U);
    assert(ranges.count(lo.id) == 0);
    ranges[lo.id] = hi.id;
    names[lo.id] = "event";
    assert(lo.is_event());
    assert(hi.is_event());
  }

  // barrier
  {
    ID lo = ID::make_barrier(0, 0, 0);
    ID hi = ID::make_barrier(-1U, -1U, -1U);
    assert(ranges.count(lo.id) == 0);
    ranges[lo.id] = hi.id;
    names[lo.id] = "barrier";
    assert(lo.is_barrier());
    assert(hi.is_barrier());
  }

  // reservation
  {
    ID lo = ID::make_reservation(0, 0);
    ID hi = ID::make_reservation(-1U, -1U);
    assert(ranges.count(lo.id) == 0);
    ranges[lo.id] = hi.id;
    names[lo.id] = "reservation";
    assert(lo.is_reservation());
    assert(hi.is_reservation());
  }

  // memory
  {
    ID lo = ID::make_memory(0, 0);
    ID hi = ID::make_memory(-1U, -1U);
    assert(ranges.count(lo.id) == 0);
    ranges[lo.id] = hi.id;
    names[lo.id] = "memory";
    assert(lo.is_memory());
    assert(hi.is_memory());
  }

  // instance
  {
    ID lo = ID::make_instance(0, 0, 0, 0);
    ID hi = ID::make_instance(-1U, -1U, -1U, -1U);
    assert(ranges.count(lo.id) == 0);
    ranges[lo.id] = hi.id;
    names[lo.id] = "instance";
    assert(lo.is_instance());
    assert(hi.is_instance());
  }

  // processor
  {
    ID lo = ID::make_processor(0, 0);
    ID hi = ID::make_processor(-1U, -1U);
    assert(ranges.count(lo.id) == 0);
    ranges[lo.id] = hi.id;
    names[lo.id] = "processor";
    assert(lo.is_processor());
    assert(hi.is_processor());
  }

  // procgroup
  {
    ID lo = ID::make_procgroup(0, 0, 0);
    ID hi = ID::make_procgroup(-1U, -1U, -1U);
    assert(ranges.count(lo.id) == 0);
    ranges[lo.id] = hi.id;
    names[lo.id] = "procgroup";
    assert(lo.is_procgroup());
    assert(hi.is_procgroup());
  }

  // idxspace
  {
    ID lo = ID::make_idxspace(0, 0, 0);
    ID hi = ID::make_idxspace(-1U, -1U, -1U);
    assert(ranges.count(lo.id) == 0);
    ranges[lo.id] = hi.id;
    names[lo.id] = "idxspace";
    assert(lo.is_idxspace());
    assert(hi.is_idxspace());
  }

  // allocator
  {
    ID lo = ID::make_allocator(0, 0, 0);
    ID hi = ID::make_allocator(-1U, -1U, -1U);
    assert(ranges.count(lo.id) == 0);
    ranges[lo.id] = hi.id;
    names[lo.id] = "allocator";
    assert(lo.is_allocator());
    assert(hi.is_allocator());
  }

  ID::IDType prev = 0;
  for(std::map<ID::IDType, ID::IDType>::const_iterator it = ranges.begin(); it != ranges.end(); it++) {
    if(verbose)
      std::cout << names[it->first] << ": " << std::hex << it->first << " -> " << it->second << std::dec << "\n";
    assert(it->first >= (1ULL << 60)); // all ids should be nonzero in the first nibble
    assert(it->first >= prev);
    prev = it->second + 1;
  }
}
