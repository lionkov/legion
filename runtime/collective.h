// Henry Cooney <email: hacoo36@gmail.com> <Github: hacoo>
// 3 Aug. 2016
// 
// legion/runtime/collective.h

// Implementation of classes for storing and manging collective operations.
// Objects represent a collective in-progress.

// Makes use of lockfree reader-writer queues from: https://github.com/cameron314/readerwriterqueue.git
// See atomicops.h and readerwriterqueue.h for licensing information

#ifndef COLLECTIVE_H
#define COLLECTIVE_H 
//#include "fabric_types.h"
#include <atomic>
#include "atomicops.h"
#include "readerwriterqueue.h"
#include <iostream>
#include <utility>
#include <stdint.h>
#include <cstring>
#include <cassert>
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
  std::atomic<std::uint32_t> num_recvd; // counter of number of entries recieved
  T* buf; // gather buffer for all gather entries
  bool* recvd_flags; // tracks whether a given gather entry was recieved
  
  std::atomic<bool> wait_complete; // true if all data was received and the buf pointer was returned
  std::atomic<bool> all_recvd; // True when all entries have been recieved
  std::atomic<bool> initialized; // True if initialized
  void reset(); // Ready this object for a new gather. Invalid if the current gather is incomplete.
};

template <typename T>
Gatherer<T>::Gatherer(uint32_t _num_nodes) {
  init(_num_nodes);
}

// Use if you want to initialize later
template <typename T>
Gatherer<T>::Gatherer()
  : num_nodes(0) {
  initialized.store(false);
}

// Initialize this object to accomodate gathers from a network
// of _num_nodes size
template <typename T>
void Gatherer<T>::init(uint32_t _num_nodes) {
  assert((atomic_load(&initialized) == false) && "Gather object was already initialized.");
  num_nodes = _num_nodes;
  all_recvd.store(false);
  wait_complete.store(false);
  num_recvd.store(0);
  buf = new T[num_nodes];
  recvd_flags = new bool[num_nodes];
  
  for(int i=0; i<num_nodes; ++i)
    recvd_flags[i] = false;

  initialized.store(true);
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
  if (initialized.load()) {
    if (wait_complete) {
      delete[] buf;
    }
    delete[] recvd_flags;
    initialized.store(false);
  }
}


// add gather data for node sender to the buffer
template <typename T>
void Gatherer<T>::add_entry(T& entry, NodeId sender) {
  assert(initialized.load() && "Gather must be initialized before adding entries");
  assert((sender >= 0) && (sender < num_nodes) && "Sender ID out of range");
  // Sanity check -- try to detect if this entry has already
  // been written. Not guaranteed to detect this condition,
  // since recv_flags is not protected by a lock!
  assert((recvd_flags[sender] == false) && "Gather entry for this sender was already received");
  buf[sender] = entry;
  recvd_flags[sender] = true;
  uint32_t old = num_recvd.fetch_add(1);
  
  // Another sanity check -- see if too many entries have been recorded
  assert((old < num_nodes) && "More gather entries received than nodes in this fabric");
  if(old == num_nodes-1)
    all_recvd.store(true);
}

// Block until all gather entries are received. Return pointer to the
// recieved entries on completion. This buffer is not owned by the calling
// function and must be deallocated using delete[].
template <typename T>
T* Gatherer<T>::wait() {
  assert(initialized.load() && "Cannot wait on uninitialized gather object");
  // Spin wait until all entries have arrived
  while(atomic_load(&all_recvd) == false)
    ;
  assert (num_recvd.load() == num_nodes && "Wrong number of entries were recieved on gather");
  T* temp = buf; // reset will reassign buf
  reset(); 
  return temp;
}

// Resets this gather, readying it to accept new data.
// The previous gather MUST have completed before calling this
// function.
template <typename T>
void Gatherer<T>::reset() {
  assert(initialized.load() && "Cannot reset uninitialized gather object");
  assert((wait_complete.exchange(false) == false) && "Cannot reset a gather in-progress");
  for(int i=0; i<num_nodes; ++i) 
    recvd_flags[i] = false;
  buf = new T[num_nodes];
  wait_complete.store(false);
  all_recvd.store(false);
  num_recvd.store(0);
}

/* 
   Coordinates Broadcast requests. The Broadcast root will send data to each 
   other node in the fabric; all other nodes will wait until broadcast data is 
   received. All calls will return data of type T.
   
   The Fabric will contain a single Broadcaster object, which will be reused
   for each broadcast operation.

   Currently, only one broadcast may be in progress at a time on a given node, 
   otherwise behavior is undefined. The Broadcaster will attempt to enforce 
   this by crashing if a Broadcast is received from the wrong node.

   The wait() function will spin until a Broadcast messages are recieved 
   from the expected node, and then return the received data.
   
   The Broadcaster does not need to be initialized, and does not return any
   dynamically allocated data. Internally, the Broadcaster stores requests in a FIFO
   queue.

 */
template <typename T>
class Broadcaster {
public:
  Broadcaster()
    : queue(100) { };
  ~Broadcaster() { };
  // Wait for a broadcast to arrive from node Sender
  void wait(T& t, NodeId sender);
  void add_entry(T& entry, NodeId sender);
  
private:
  // Contains received data and the sender.
  moodycamel::BlockingReaderWriterQueue<std::pair<T, NodeId>> queue;  
};

template <typename T>
void Broadcaster<T>::add_entry(T& entry, NodeId sender) {
  bool success = queue.try_enqueue(std::pair<T, NodeId>(entry, sender));
  assert((success) && "Error -- Broadcast queue full!");  
}

// Wait for a broadcast from a given sender, return in t.
template <typename T>
void Broadcaster<T>::wait(T& t, NodeId sender) {
  std::pair<T, NodeId> p;
  queue.wait_dequeue(p);
  assert((p.second == sender) && "Error -- received broadcast from unexpected root");
  t = p.first;
} 

#endif // COLLECTIVE_H
