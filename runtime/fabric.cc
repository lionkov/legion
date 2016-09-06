// Henry Cooney <email: hacoo36@gmail.com> <Github: hacoo>
// 3 Aug. 2016
// 
// legion/runtime/fabric.cc

#include "fabric.h"

Fabric::Fabric() : log(NULL), num_msgs_added(0) {
  for (int i = 0; i < MAX_MESSAGE_TYPES; ++i)
    mts[i] = NULL;
}

Fabric::~Fabric() {
  for (int i = 0; i < MAX_MESSAGE_TYPES; ++i) {
    if (mts[i])
      delete mts[i];
  }
}

bool Fabric::add_message_type(MessageType *mt, const std::string tag)
{
  log_fabric().debug("registered message type: %s", tag.c_str());

  if (mt->id == 0 || mts[mt->id] != NULL || mt->id > MAX_MESSAGE_TYPES) {
    assert(false && "Attempted to add invalid message type");
    return false;
  }

  mts[mt->id] = mt;
  mdescs[mt->id] = tag;
  return true;
}

// Messages for collective communication
void EventGatherMessageType::send_request(NodeId dest, Realm::Event& event) {
  fabric->send(new EventGatherMessage(dest, event, fabric->get_id()));
}

void EventGatherMessageType::request(Message* m) {
  RequestArgs* args = (RequestArgs*) m->get_arg_ptr();
  fabric->recv_gather_event(args->event, args->sender);
}


void EventBroadcastMessageType::send_request(NodeId dest, Realm::Event& event) {
  fabric->send(new EventBroadcastMessage(dest, event, fabric->get_id()));
}

void EventBroadcastMessageType::request(Message* m) {
  RequestArgs* args = (RequestArgs*) m->get_arg_ptr();
  fabric->recv_broadcast_event(args->event, args->sender);
}

void BarrierNotifyMessageType::send_request(NodeId dest, uint32_t barrier_id) {
  fabric->send(new BarrierNotifyMessage(dest, barrier_id, fabric->get_id()));
}

void BarrierNotifyMessageType::request(Message* m) {
  RequestArgs* args = (RequestArgs*) m->get_arg_ptr();
  fabric->recv_barrier_notify(args->barrier_id, args->sender);
}
