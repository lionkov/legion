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

#include "activemsg.h"

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
      GASNetHSL mutex;
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
	int node;
	ID::IDType id;
      };

      void request(Message* m);
      static void send_request(NodeId target, ID::IDType id);
    };

    class MetadataRequestMessage : public FabMessage {
    public:
      MetadataRequestMessage(NodeId target, void* args)
	: FabMessage(target, METADATA_REQUEST_MSGID, args, NULL) { }
    };
    
    class MetadataResponseMessageType : public MessageType {
    public:
      MetadataResponseMessageType()
	: MessageType(METADATA_RESPONSE_MSGID, sizeof(RequestArgs), true, true) { }
      
      struct RequestArgs : public BaseMedium {
	ID::IDType id;
      };

      void request(Message* m);
      static void send_request(NodeId target, ID::IDType id, 
			       void *data, size_t datalen, int payload_mode);
    };

    class MetadataResponseMessage : public FabMessage {
    public: 
      MetadataResponseMessage(NodeId target, void* args, FabPayload* payload)
	: FabMessage(target, METADATA_RESPONSE_MSGID, args, payload) { }
    };
    
    class MetadataInvalidateMessageType : public MessageType {
    public:
      MetadataInvalidateMessageType()
	: MessageType(METADATA_INVALIDATE_MSGID, sizeof(RequestArgs), false, true) { }
      
      struct RequestArgs {
	int owner;
	ID::IDType id;
      };

      void request(Message* m);
      static void send_request(NodeId target, ID::IDType id);
      // TODO
      //static void broadcast_request(const NodeSet& targets, ID::IDType id);
    };

    class MetadataInvalidateMessage : public FabMessage {
    public:
      MetadataInvalidateMessage(NodeId dest, void* args)
	: FabMessage(dest, METADATA_INVALIDATE_MSGID, args, NULL) { }
    };
    
    class MetadataInvalidateAckMessageType : public MessageType {
    public:
      MetadataInvalidateAckMessageType()
	: MessageType(METADATA_INVALIDATE_ACK_MSGID, sizeof(RequestArgs), false, true) { }
      
      struct RequestArgs {
	gasnet_node_t node;
	ID::IDType id;
      };

      void request(Message* m);
      static void send_request(NodeId target, ID::IDType id);
    };

    class MetadataInvalidateAckMessage : public FabMessage {
    public:
      MetadataInvalidateAckMessage(NodeId dest, void* args)
	: FabMessage(dest, METADATA_INVALIDATE_ACK_MSGID, args, NULL) { }      
    };
    
}; // namespace Realm

#endif // ifndef REALM_METADATA_H
