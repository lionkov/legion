// Henry Cooney <email: hacoo36@gmail.com> <Github: hacoo>
// 10 Aug. 2016
// 
// legion/runtime/barrier.cc

// Implements classes for coordinating barriers. Implementation is similar to collectives,
// however, multiple barriers may proceed at once as long as barriers are given different identifiers.

#include "barrier.h"


BarrierWaiterEntry::BarrierWaiterEntry(uint32_t _num_nodes)
  : num_nodes(_num_nodes),
    recvd_flags(NULL),
    wait_complete(false),
    all_recvd(false),
    num_recvd(0) {
  recvd_flags = new bool[num_nodes];
  std::fill_n(recvd_flags, num_nodes, false);
}

BarrierWaiterEntry::BarrierWaiterEntry(const BarrierWaiterEntry& other) {
  assert(false && "Cannot copy BarrierWaiterEntry");
}

BarrierWaiterEntry::BarrierWaiterEntry(BarrierWaiterEntry&& other)
  : num_nodes(other.num_nodes),
    recvd_flags(other.recvd_flags),
    wait_complete(other.wait_complete.load()),
    all_recvd(other.all_recvd.load()),
    num_recvd(other.num_recvd.load()) {
  other.recvd_flags = NULL;  
}

BarrierWaiterEntry::~BarrierWaiterEntry() {
  if (recvd_flags)
    delete[] recvd_flags;
}

BarrierWaiterEntry& BarrierWaiterEntry::operator= (const BarrierWaiterEntry& rhs) {
  if(this != &rhs)
    assert(false && "Cannot copy a BarrierWaiterEntry");
  return *this;
}

BarrierWaiterEntry& BarrierWaiterEntry::operator= (BarrierWaiterEntry&& rhs) {
  if (this != &rhs) {
    if(recvd_flags)
      delete[] recvd_flags;
    recvd_flags = rhs.recvd_flags;
    wait_complete.store(rhs.wait_complete.load());
    all_recvd.store(rhs.all_recvd.load());
    num_recvd.store(rhs.num_recvd.load());
  }
  return *this;
}

// Spinwait until all notifications are recvd
void BarrierWaiterEntry::wait() {
  assert((wait_complete.load() == false) && "Cannot re-use a BarrierWaiterEntry");
  while(all_recvd.load() == false) 
    ;
  assert((num_recvd.load() == num_nodes) && "Barrier collision -- received wrong number of notifications");
  wait_complete.store(true); 
}

void BarrierWaiterEntry::notify(NodeId sender) {
  assert((recvd_flags[sender] == false) && "Barrier collision -- received two notifications from same sender");
  recvd_flags[sender] = true;
  uint32_t old = num_recvd.fetch_add(1);
  if (old >= num_nodes-1)
   all_recvd.store(true);
}

BarrierWaiter::BarrierWaiter()
  : initialized(false) { }

BarrierWaiter::BarrierWaiter(uint32_t _num_nodes) {
  init(_num_nodes);
}

void BarrierWaiter::init(uint32_t _num_nodes) {
  num_nodes = _num_nodes;
  initialized = true;
}

// Notify an in-progress barrier that sender is checking in. If the barrier
// has not been created yet, create it.
void BarrierWaiter::notify(uint32_t barrier_id, NodeId sender) {
  assert((initialized == true) && "Barrier not initiazlied");
  map_mutex.lock();
  auto entry
    = barriers_in_progress.emplace(barrier_id, BarrierWaiterEntry(num_nodes));
  map_mutex.unlock();
  entry.first->second.notify(sender);
}

// Wait on an in-progress barrier. If it has not been created yet, create the BarrierWaiterEntry.
// Remove the entry when complete.
void BarrierWaiter::wait(uint32_t barrier_id) {
  assert((initialized == true) && "Barrier not initiazlied");
  map_mutex.lock();
  auto entry
    = barriers_in_progress.emplace(barrier_id, BarrierWaiterEntry(num_nodes));
  map_mutex.unlock();
  entry.first->second.wait();
  map_mutex.lock();
  barriers_in_progress.erase(entry.first);
  map_mutex.unlock();
}



