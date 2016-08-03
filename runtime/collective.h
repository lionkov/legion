// Henry Cooney <email: hacoo36@gmail.com> <Github: hacoo>
// 3 Aug. 2016
// 
// legion/runtime/collective.h
// 
// Classes for coordinating collective Broadcast and Gather collective
// operations.

#ifndef COLLECTIVE_H
#define COLLECTIVE_H

#include <cstring>
#include <iostream>
#include <stdint.h>

/* 
   Gathers incoming message contents into a single array on the 
   root node. Nonroot nodes should not use this class, they should instead
   simply send an EventGatherMessage (or other type) to the root.

   The Fabric will contain a single Gather object, which is created 
   when a gather is initiated, and persists until the gather completes.
   
   Currently, only one gather may be in progress at a time on a given node, 
   otherwise behavior is undefined.

   The wait() function will block until all gather messages are recieved, 
   and returns a pointer to the filled gather buffer.
   
   This class should be able to handle gathers on any type of message content.
   However, currently Realm only uses gather/broadcasts on Event objects, 
   so you should probably create a Gatherer<Event>. 
*/

template <typename T> 
class Gatherer {
 public:
  Gatherer(NodeId _root, 
		size_t _num_nodes);
  ~Gatherer(); 
  T* wait(); // Wait until all gather items have been recieved;
  // return pointer to filled buffer. The receiver of this buffer takes
  // ownership and is responsible for deallocating the gather buffer.

  // Add an event to the gather buffer
  void add_event(T& t, NodeId sender);
  
 protected:
  NodeId root; // ID of root / gathering node 
  size_t num_nodes; // Number of nodes in the fabric
  T* buf; // gather buffer for all events
};

#endif // COLLECTIVE_H
