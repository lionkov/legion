#include "fabric.h"

int MessageType::send(NodeId target)
{
  return fabric->send(target, id, NULL, NULL, inorder);
}

int MessageType::send(NodeId target, void *args)
{
  return fabric->send(target, id, args, NULL, inorder);
}

int MessageType::send(NodeId target, void *args, Payload *payload)
{
  return fabric->send(target, id, args, payload, inorder);
}
