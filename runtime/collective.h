// Henry Cooney <email: hacoo36@gmail.com> <Github: hacoo>
// 3 Aug. 2016
// 
// legion/runtime/collective.h

// Implementation of classes for storing and manging collective operations.
// Objects represent a collective in-progress.

#ifndef COLLECTIVE_H
#define COLLECTIVE_H

#include "fabric_types.h"
#include <stdint.h>
#include <cstring>
#include <cassert>
#include <stdatomic.h>
#include <pthread.h>
#include <semaphore.h>


/* 
   Gathers incoming message contents into a single array on the 
   root node. Nonroot nodes should not use this class, they should instead
   simply send an EventGatherMessage (or other type) to the root.

   The Fabric will contain a single Gather object, which will be reused
   for each gather operation.

   Currently, only one gather may be in progress at a time on a given node, 
   otherwise behavior is undefined. The Gatherer attempts to enforce one-at-a-time
   behavior via the following rules:
   
   - If an entry apprears to be written twice, crash
   - If the wrong number of entries are received, crash
   - The object may be reused by calling reset(). However,
   if the object is reused before recoving the gather data, crash.

   The wait() function will block until all gather messages are recieved, 
   and returns a pointer to the filled gather buffer. This buffer must be 
   deallocated by the receiver using delete[].
*/

template <typename T> 
class Gatherer {
 public:
  Gatherer(); // Default constructor -- will not initialize
  Gatherer(uint32_t _num_nodes); 
  ~Gatherer(); // Deallocates internal buffer only if it was never
  // returned by wait() on this object
  void init(uint32_t _num_nodes); // must be called if default constructor is used
  T* wait(); // Wait until all gather items have been recieved;
  // return pointer to filled buffer. The receiver of this buffer takes
  // ownership and is responsible for deallocating the gather buffer using delete[].

  void add_entry(T& entry, NodeId sender);  // register gather data from node sender
  
 protected:
  NodeId root; // ID of root / gathering node 
  uint32_t num_nodes; // Number of nodes in the fabric
  atomic_uint_fast32_t num_recvd; // counter of number of entries recieved
  sem_t all_recvd_sem;
  T* buf; // gather buffer for all gather entries
  bool* recvd_flags; // tracks whether a given gather entry was recieved
  atomic_bool wait_complete; // true if all data was received and the buf pointer was returned
  bool initialized;
  void reset(); // Ready this object for a new gather. Invalid if the current gather is incomplete.
};

template <typename T>
Gatherer<T>::Gatherer(uint32_t _num_nodes) {
  init(_num_nodes);
}

// Use if you want to initialize later
template <typename T>
Gatherer<T>::Gatherer()
  : num_nodes(0), initialized(false) { }

template <typename T>
void Gatherer<T>::init(uint32_t _num_nodes) {
  num_nodes = _num_nodes;
   // Will allow waiter to proceed once all entries complete
  assert(sem_init(&all_recvd_sem, 0, 0) == 0);
  
  atomic_init(&num_recvd, 0);

  atomic_init(&wait_complete, false);
  
  buf = new T[num_nodes];

  recvd_flags = new bool[num_nodes];
  for(int i=0; i<num_nodes; ++i)
    recvd_flags[i] = false;

  initialized = true;
}

// Destructor -- if the wait completed, then buf is not owned
// by someone else. Otherwise, this object still owns it,
// so deallocate buf.
template <typename T>
Gatherer<T>::~Gatherer() {
  if (initialized) { 
    if (atomic_load(&wait_complete))
      delete[] buf;
  
    delete[] recvd_flags;
  }
}

// add gather data for node sender to the buffer
template <typename T>
void Gatherer<T>::add_entry(T& entry, NodeId sender) {
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
    sem_post(&all_recvd_sem);
    
}

// Block until all gather entries are received. Return pointer to the
// recieved entries on completion. This buffer is not owned by the calling
// function and must be deallocated using delete[].
template <typename T>
T* Gatherer<T>::wait() {
  assert(initialized && "Cannot wait on uninitialized gather object");
  sem_wait(&all_recvd_sem);
  assert (atomic_load(&num_recvd) == num_nodes && "Wrong number of entries were recieved on gather");
  atomic_store(&wait_complete, true);
  T* temp = buf; // reset will reassign buf
  reset(); 
  return temp;
}

// Resets this gather, readying it to accept new data.
// The previous gather MUST have completed before calling this
// function.
template <typename T>
void Gatherer<T>::reset() {
  assert(initialized && "Cannot reset uninitialized gather object");
  assert((atomic_exchange(&wait_complete, false) == true) && "Cannot reset a gather in-progress");
  for(int i=0; i<num_nodes; ++i) 
    recvd_flags[i] = false;
  buf = new T[num_nodes];
}

#endif // COLLECTIVE_H
