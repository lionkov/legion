// Henry Cooney <email: hacoo36@gmail.com> <Github: hacoo>
// 3 Aug. 2016
// 
// legion/runtime/fabric.cc

#include "fabric.h"

// Messages for collective communication
void EventGatherMessageType::send_request(NodeId dest, Realm::Event& event) {
  fabric->send(new EventGatherMessage(dest, event, fabric->get_id()));
}

void EventGatherMessageType::request(Message* m) {
  RequestArgs* args = (RequestArgs*) m->get_arg_ptr();
  fabric->recv_gather_event(args->event, args->sender);
};
