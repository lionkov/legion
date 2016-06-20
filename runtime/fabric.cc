#include "fabric.h"

int MessageType::send(NodeId target)
{
	fabric->send(target, id, NULL, NULL, inorder);
}

int MessageType::send(NodeId target, void *args)
{
	fabric->send(target, id, args, NULL, inorder);
}

int MessageType::send(NodeId target, void *args, Payload *payload)
{
	fabric->send(target, id, args, payload, inorder);
}
