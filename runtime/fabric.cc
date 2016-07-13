#include "fabric.h"

int MessageType::send(NodeId target)
{
  return fabric->send(target, id, NULL, NULL);
}

int MessageType::send(NodeId target, void *args)
{
  return fabric->send(target, id, args, NULL);
}

int MessageType::send(NodeId target, void *args, FabPayload *payload)
{
  return fabric->send(target, id, args, payload);
}

