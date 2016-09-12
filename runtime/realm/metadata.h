/* Copyright 2016 Stanford University, NVIDIA Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Realm object metadata base implementation

#ifndef REALM_METADATA_H
#define REALM_METADATA_H

#include "event.h"
#include "id.h"
#include "nodeset.h"

#include "fabric.h"

namespace Realm {

  class GenEventImpl;

  class MetadataBase {
  public:
    MetadataBase(void);
    ~MetadataBase(void);

    enum State { STATE_INVALID,
		 STATE_VALID,
		 STATE_REQUESTED,
		 STATE_INVALIDATE,  // if invalidate passes normal request response
		 STATE_CLEANUP };

    bool is_valid(void) const { return state == STATE_VALID; }

    void mark_valid(void); // used by owner
    void handle_request(int requestor);

    // returns an Event for when data will be valid
    Event request_data(int owner, ID::IDType id);
    void await_data(bool block = true);  // request must have already been made
    void handle_response(void);
    void handle_invalidate(void);

    // these return true once all remote copies have been invalidated
    bool initiate_cleanup(ID::IDType id);
    bool handle_inval_ack(int sender);

  protected:
    MUTEX_T mutex;
    State state;  // current state
    Event valid_event;
    NodeSet remote_copies;
  };

  // active messages
  class MetadataRequestMessageType : public MessageType {
  public:
  MetadataRequestMessageType()
    : MessageType(METADATA_REQUEST_MSGID, sizeof(RequestArgs), false, true) { }
      
    struct RequestArgs {
      RequestArgs() { }
      RequestArgs(int _node, ID::IDType _id)
	: node(_node), id(_id) { }
      int node;
      ID::IDType id;
    };

    void request(Message* m);
    static void send_request(NodeId target, ID::IDType id);
  };

  class MetadataRequestMessage : public Message {
  public:
  MetadataRequestMessage(NodeId target, int node, ID::IDType id)
    : Message(target, METADATA_REQUEST_MSGID, &args, NULL),
      args(node, id) { }

    MetadataRequestMessageType::RequestArgs args;
  };
    
  class MetadataResponseMessageType : public MessageType {
  public:
  MetadataResponseMessageType()
    : MessageType(METADATA_RESPONSE_MSGID, sizeof(RequestArgs), true, true) { }
      
    struct RequestArgs  {
      RequestArgs() { }
      RequestArgs(ID::IDType _id)
	: id(_id) { }
      ID::IDType id;
    };

    void request(Message* m);
    static void send_request(NodeId target, ID::IDType id, 
			     void *data, size_t datalen, int payload_mode);
  };

  class MetadataResponseMessage : public Message {
  public: 
  MetadataResponseMessage(NodeId target, ID::IDType id, FabPayload* payload)
    : Message(target, METADATA_RESPONSE_MSGID, &args, payload),
      args(id) { }

    MetadataResponseMessageType::RequestArgs args;
  };
    
  class MetadataInvalidateMessageType : public MessageType {
  public:
  MetadataInvalidateMessageType()
    : MessageType(METADATA_INVALIDATE_MSGID, sizeof(RequestArgs), false, true) { }
      
    struct RequestArgs {
      RequestArgs() { }
      RequestArgs(int _owner, ID::IDType _id)
	: owner(_owner), id(_id) { }
      int owner;
      ID::IDType id;
    };

    struct BroadcastHelper : public RequestArgs {
      BroadcastHelper(int _owner, ID::IDType _id)
	: RequestArgs(_owner, _id) { }
      inline void apply(NodeId target);
      void broadcast(const NodeSet& targets);
    };
    

    void request(Message* m);
    static void send_request(NodeId target, ID::IDType id);
    static void broadcast_request(const NodeSet& targets, ID::IDType id);
  };

  class MetadataInvalidateMessage : public Message {
  public:
  MetadataInvalidateMessage(NodeId dest, int owner, ID::IDType id)
    : Message(dest, METADATA_INVALIDATE_MSGID, &args, NULL),
      args(owner, id) { }

  MetadataInvalidateMessage(NodeId dest, MetadataInvalidateMessageType::RequestArgs* _args)
    : Message(dest, METADATA_INVALIDATE_MSGID, &args, NULL),
      args(_args->owner, _args->id) { }

    MetadataInvalidateMessageType::RequestArgs args;
  };
    
  class MetadataInvalidateAckMessageType : public MessageType {
  public:
  MetadataInvalidateAckMessageType()
    : MessageType(METADATA_INVALIDATE_ACK_MSGID, sizeof(RequestArgs), false, true) { }
      
    struct RequestArgs {
      RequestArgs() { }
      RequestArgs(NodeId _node, ID::IDType _id)
	: node(_node), id(_id) { }
      NodeId node;
      ID::IDType id;
    };

    void request(Message* m);
    static void send_request(NodeId target, ID::IDType id);
  };

  class MetadataInvalidateAckMessage : public Message {
  public:
    MetadataInvalidateAckMessage(NodeId dest, NodeId node, ID::IDType id)
      : Message(dest, METADATA_INVALIDATE_ACK_MSGID, &args, NULL),
      args(node, id) { }

    MetadataInvalidateAckMessageType::RequestArgs args;
  };
    
}; // namespace Realm

#endif // ifndef REALM_METADATA_H
