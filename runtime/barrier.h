// Henry Cooney <email: hacoo36@gmail.com> <Github: hacoo>
// 10 Aug. 2016
// 
// legion/runtime/barrier.h

// Implements classes for coordinating barriers. Implementation is similar to collectives,
// however, multiple barriers may proceed at once as long as barriers are given different identifiers.

#ifndef BARRIER_H
#define BARRIER_H

#include "fabric_types.h"
#include <atomic>
#include "atomicops.h"
#include "readerwriterqueue.h"
#include <iostream>
#include <utility>
#include <map>
#include <stdint.h>
#include <cstring>
#include <cassert>
#include <unistd.h>



/*
  class BarrierWaiterEntry 
   
   A single barrier in progress. BarrierWaiter will manage one BarrierWaiterEntry for 
   each open barrier ID. 
*/

class BarrierWaiterEntry {
public:
  BarrierWaiterEntry(uint32_t _barrier_id, uint32_t _num_nodes);
  ~BarrierWaiterEntry();
  
  // Spinwait until all notifications are received
  void wait();
  void notify(NodeId sender); 
  
protected:
  uint32_t num_nodes; // Number of nodes in the fabric
  std::atomic<std::uint32_t> num_recvd; // counter of number of entries recieved
  bool* recvd_flags; // tracks whether a given gather entry was recieved
  
  std::atomic<bool> wait_complete; // true if all data was received and the buf pointer was returned
  std::atomic<bool> all_recvd; // True when all entries have been recieved
  void reset(); // Ready this object for a new gather. Invalid if the current gather is incomplete.
};

/* 
   class BarrierWaiter:

   Coordinates waits on Barriers. Will store incoming BarrierNotifyMessages 
   and categorize them by their barrier ID. When waiting on this object, the 
   thread will spinwait until all nodes have sent in a Notify message on the 
   corresponding barrier ID.

   Simultaneous barrier operations sharing the same ID will result in undefined
   behavior. Either use unique IDs for each barrier, or, if using an anonymous barrier, 
   do not allow multiple barrier to take place at once.

   Because Legion uses Barriers to synchronize clocks, it is preferable to spinwait for
   notifications rather than block. Although spinwaiting may be less efficient, it should 
   hopefully mean more consistent wakeup times, which is good for clock syncing.

*/    


class BarrierWaiter {
public:
  // Default constructor -- will not initialize
  BarrierWaiter();

  ~BarrierWaiter();

  // Initialize on construction
  BarrierWaiter(uint32_t _num_nodes);

  // Initialize to given node count
  void init(uint32_t _num_nodes);
  void wait(uint32_t barrier_id);
  void notify(unint32_t barrier_id, NodeId sender);
  
protected:
  std::atomic<bool> initialized;
  uint32_t num_nodes;
  std::map<uint32_t, BarrierWaiterEntry> barriers_in_progress;
};




#endif // BARRIER_H
