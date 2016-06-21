#ifndef MSG_H
#define MSG_H

// For now, fabric will depend on ActiveMessagIDs and
// Payload definitions from activemsg.h. When GASNET has
// been fully removed, this will be moved back in to fabric.h / msg.h
#include "activemsg.h"



/* enum ActiveMessageIDs { */
/*       FIRST_AVAILABLE = 140, */
/*       NODE_ANNOUNCE_MSGID, */
/*       SPAWN_TASK_MSGID, */
/*       LOCK_REQUEST_MSGID, */
/*       LOCK_RELEASE_MSGID, */
/*       LOCK_GRANT_MSGID, */
/*       EVENT_SUBSCRIBE_MSGID, */
/*       EVENT_TRIGGER_MSGID, */
/*       EVENT_UPDATE_MSGID, */
/*       REMOTE_MALLOC_MSGID, */
/*       REMOTE_MALLOC_RPLID = 150, */
/*       CREATE_ALLOC_MSGID, */
/*       CREATE_ALLOC_RPLID, */
/*       CREATE_INST_MSGID, */
/*       CREATE_INST_RPLID, */
/*       VALID_MASK_REQ_MSGID, */
/*       VALID_MASK_DATA_MSGID, */
/*       ROLL_UP_TIMER_MSGID, */
/*       ROLL_UP_TIMER_RPLID, */
/*       ROLL_UP_DATA_MSGID, */
/*       CLEAR_TIMER_MSGID, */
/*       DESTROY_INST_MSGID = 160, */
/*       REMOTE_WRITE_MSGID, */
/*       REMOTE_REDUCE_MSGID, */
/*       REMOTE_SERDEZ_MSGID, */
/*       REMOTE_WRITE_FENCE_MSGID, */
/*       REMOTE_WRITE_FENCE_ACK_MSGID, */
/*       DESTROY_LOCK_MSGID, */
/*       REMOTE_REDLIST_MSGID, */
/*       MACHINE_SHUTDOWN_MSGID, */
/*       BARRIER_ADJUST_MSGID, */
/*       BARRIER_SUBSCRIBE_MSGID = 170, */
/*       BARRIER_TRIGGER_MSGID, */
/*       BARRIER_MIGRATE_MSGID, */
/*       METADATA_REQUEST_MSGID, */
/*       METADATA_RESPONSE_MSGID, // should really be a reply */
/*       METADATA_INVALIDATE_MSGID, */
/*       METADATA_INVALIDATE_ACK_MSGID, */
/*       REGISTER_TASK_MSGID, */
/*       REGISTER_TASK_COMPLETE_MSGID, */
/*     }; */
#endif
