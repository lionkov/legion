// Henry Cooney <email: hacoo36@gmail.com> <Github: hacoo>
// 3 Aug. 2016
// 
// legion/runtime/fabric.cc

#include "fabric.h"

// Gatherer class -- helper object for coordinating gathers.
// See fabric.h for description.

template <typename T>
Gatherer<T>::Gatherer(size_t _num_nodes) {
  init(_num_nodes);
}

// Use if you want to initialize later
template <typename T>
Gatherer<T>::Gatherer() {
  : num_nodes(0), initialized(false)
}

template <typename T>
void Gatherer<T>::init(size_t _num_nodes) {
  num_nodes = _num_nodes
   // Will allow waiter to proceed once all entries complete
  assert(sem_init(&all_recvd_sem, 0, 0) == 0);
  
  atomic_init(&num_recvd);
  atomic_store(&num_recvd, 0);

  atomic_init(&wait_complete);
  atomic_stor(&wait_complete, false);
  
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
    if (atomic_load(wait_complete))
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
  size_t old = fetch_atomic_add(&num_recvd, 1);
  
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
  assert(initialized, "Cannot wait on uninitialized gather object");
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
template <typename t>
void Gatherer<T>::reset() {
  assert(initialized, "Cannot reset uninitialized gather object");
  assert((atomic_exchange(&wait_complete, false) == true) && "Cannot reset a gather in-progress");
  for(int i=0; i<num_nodes; ++i) 
    recvd_flags[i] = false;
  buf = new T[num_nodes];
}
