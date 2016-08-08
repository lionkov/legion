// Henry Cooney <email: hacoo36@gmail.com> <Github: hacoo>
// 3 Aug. 2016
// 
// legion/runtime/collective.h

// Implementation of classes for storing and manging collective operations.
// Objects represent a collective in-progress.

#ifndef COLLECTIVE_H
#define COLLECTIVE_H

#include "fabric_types.h"
#include <iostream>
#include <stdint.h>
#include <cstring>
#include <cassert>
#include <stdatomic.h>
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>

/* 
   Gathers incoming message contents into a single array on the 
   root node. It is used internally by Fabrics.

   The Fabric will contain a single Gather object, which will be reused
   for each gather operation.

   Currently, only one gather may be in progress at a time on a given node, 
   otherwise behavior is undefined. The Gatherer will attempt to detect violations
   of this rule via the following mechanisms:
   
   - If an entry apprears to be written twice, crash
   - If the wrong number of entries are received, crash
   - The object may be reused by calling reset(). However,
     if reset() is called when a gather is not yet complete, crash.

   The wait() function will block until all gather messages are recieved, 
   and returns a pointer to the filled gather buffer. This buffer must be 
   deallocated by the receiver using delete[].
*/

template <typename T> 
class Gatherer {
 public:      
  // Default constructor -- will not initialize
  Gatherer();

  // Initializes to a fabric with _num_nodes nodes
  Gatherer(uint32_t _num_nodes);
  
  // Deallocates internal buffer only if it was never
  // returned by wait() on this object
  ~Gatherer();
  
  // must be called if default constructor is used
  void init(uint32_t _num_nodes);

  // Wait until all gather items have been recieved;
  // return pointer to filled buffer. The receiver of this buffer takes
  // ownership and is responsible for deallocating the gather buffer using delete[].
  T* wait();
  
  // Destroy this gather object -- it may be reinitialized later.
  // Invalid if gather is in-progress.
  void destroy();
  
  // register gather data from node sender
  void add_entry(T& entry, NodeId sender);
  
 protected:
  NodeId root; // ID of root / gathering node 
  uint32_t num_nodes; // Number of nodes in the fabric
  atomic_uint_fast32_t num_recvd; // counter of number of entries recieved
  T* buf; // gather buffer for all gather entries
  bool* recvd_flags; // tracks whether a given gather entry was recieved
  atomic_bool wait_complete; // true if all data was received and the buf pointer was returned
  atomic_bool all_recvd; // True when all entries have been recieved
  atomic_bool initialized; // True if initialized
  void reset(); // Ready this object for a new gather. Invalid if the current gather is incomplete.
};

template <typename T>
Gatherer<T>::Gatherer(uint32_t _num_nodes) {
  init(_num_nodes);
}

// Use if you want to initialize later
template <typename T>
Gatherer<T>::Gatherer()
  : num_nodes(0), initialized(false) {
  atomic_store(&initialized, false);
}


// Initialize this object to accomodate gathers from a network
// of _num_nodes size
template <typename T>
void Gatherer<T>::init(uint32_t _num_nodes) {
  assert((atomic_load(&initialized) == false) && "Gather object was already initialized.");
  num_nodes = _num_nodes;
  
  atomic_init(&all_recvd, false);
  atomic_init(&num_recvd, 0);
  atomic_init(&wait_complete, false);
  
  buf = new T[num_nodes];

  recvd_flags = new bool[num_nodes];
  for(int i=0; i<num_nodes; ++i)
    recvd_flags[i] = false;

  atomic_store(&initialized, true);
}

template <typename T>
Gatherer<T>::~Gatherer() {
  destroy();
}

// Destroy this gatherer. If no one else has taken ownership of the
// gather buffer, it is destroyed. Destroying a gather which is in-progress
// may result in undefined behavior. You may re-initialize a gatherer after destroying it.
template <typename T>
void Gatherer<T>::destroy() {
  if (atomic_load(&initialized)) {
    
    if (atomic_load(&wait_complete))
      delete[] buf;
  
    delete[] recvd_flags;
  }

  atomic_store(&initialized, false);
}


// add gather data for node sender to the buffer
template <typename T>
void Gatherer<T>::add_entry(T& entry, NodeId sender) {
  assert(atomic_load(&initialized) && "Gather must be initialized before adding entries");
  assert((sender >= 0) && (sender < num_nodes) && "Sender ID out of range");
  // Sanity check -- try to detect if this entry has already
  // been written. Not guaranteed to detect this condition,
  // since recv_flags is not protected by a lock!
  assert((recvd_flags[sender] == false) && "Gather entry for this sender was already received");
  buf[sender] = entry;
  recvd_flags[sender] = true;
  uint32_t old = atomic_fetch_add(&num_recvd, 1);
  
  // Another sanity check -- see if too many entries have been recorded
  assert((old < num_nodes) && "More gather entries received than nodes in this fabric");
  if(old == num_nodes-1)
    atomic_store(&all_recvd, true);
}

// Block until all gather entries are received. Return pointer to the
// recieved entries on completion. This buffer is not owned by the calling
// function and must be deallocated using delete[].
template <typename T>
T* Gatherer<T>::wait() {
  assert(atomic_load(&initialized) && "Cannot wait on uninitialized gather object");
  // Spin wait until all entries have arrived
  while(atomic_load(&all_recvd) == false)
    ;
  assert (atomic_load(&num_recvd) == num_nodes && "Wrong number of entries were recieved on gather");
  T* temp = buf; // reset will reassign buf
  reset(); 
  return temp;
}

// Resets this gather, readying it to accept new data.
// The previous gather MUST have completed before calling this
// function.
template <typename T>
void Gatherer<T>::reset() {
  assert(atomic_load(&initialized) && "Cannot reset uninitialized gather object");
  assert((atomic_exchange(&wait_complete, false) == false) && "Cannot reset a gather in-progress");
  for(int i=0; i<num_nodes; ++i) 
    recvd_flags[i] = false;
  buf = new T[num_nodes];
  atomic_store(&wait_complete, false);
  atomic_store(&all_recvd, false);
}

/* 
   Coordinates Broadcast requests. The Broadcast root will send data to each 
   other node in the fabric; all other nodes will wait until broadcast data is 
   received. All calls will return data of type T.
   
   The Fabric will contain a single Broadcaster object, which will be reused
   for each broadcast operation.

   Currently, only one broadcast may be in progress at a time on a given node, 
   otherwise behavior is undefined. The Broadcaster will attempt to enforce 
   this by crashing if a Broadcast is recieved from the wrong node.

   The wait() function will block until a Broadcast messages are recieved, 
   and returns data within.
   
   The Broadcaster does not need to be initialized, and does not return any
   dynamically allocated data.

 */
template <typename T>
class Broadcaster {
public:
  Broadcaster();
  ~Broadcaster() { };
  // Wait for a broadcast to arrive from node Sender
  T wait(NodeId _sender);
  void add_entry(T& entry, NodeId _sender);
  void reset();
  
private:
  // Records the currently held broadcast data
  T data;
  // Records who sent the current broadcast data
  NodeId sender;
  
  atomic_bool wait_complete;
  atomic_bool data_recvd;
};


template <typename T>
Broadcaster<T>::Broadcaster() {
  atomic_init(&wait_complete, false);
  atomic_init(&data_recvd, false);
}

template <typename T>
void Broadcaster<T>::add_entry(T& entry, NodeId _sender) {
  assert((atomic_load(&data_recvd) == false) && "Can't add data to a broadcast that hasn't finished"); 
  sender = _sender;
  data = entry;
  atomic_store(&data_recvd, true);
}

// Wait for a broadcast from a given sender.
template <typename T>
T Broadcaster<T>::wait(NodeId _sender) {
  // spin wait until data is recevied
  while(atomic_load(&data_recvd) == false)
    ;
  // Sanity check -- did we get data from the right sender?
  // If not, there are multiple broadcasts going on
  assert((sender == _sender) && "Receieved broadcast from unexpected root");
  atomic_store(&wait_complete, true);
  return data;
}

// Reset this Broadcaster -- must be called in between each Broadcast event
template <typename T>
void Broadcaster<T>::reset() {
  assert((atomic_load(&wait_complete == true)) && "Cannot reset incomplete broadcast");
  atomic_store(&wait_complete, false);
  atomic_store(&data_recvd, false);
}

#endif // COLLECTIVE_H
