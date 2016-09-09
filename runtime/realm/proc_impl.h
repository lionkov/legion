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

// Processor/ProcessorGroup implementations for Realm

#ifndef REALM_PROC_IMPL_H
#define REALM_PROC_IMPL_H

#include "processor.h"
#include "id.h"

#include "fabric.h"
#include "operation.h"
#include "profiling.h"
#include "sampling.h"

#include "event_impl.h"
#include "rsrv_impl.h"

#include "tasks.h"
#include "threads.h"
#include "codedesc.h"

namespace Realm {

    class ProcessorGroup;

    class ProcessorImpl {
    public:
      ProcessorImpl(Processor _me, Processor::Kind _kind, int _num_cores=1);

      virtual ~ProcessorImpl(void);

      virtual void enqueue_task(Task *task) = 0;

      virtual void spawn_task(Processor::TaskFuncID func_id,
			      const void *args, size_t arglen,
                              const ProfilingRequestSet &reqs,
			      Event start_event, Event finish_event,
                              int priority) = 0;

      // blocks until things are cleaned up
      virtual void shutdown(void);

      virtual void add_to_group(ProcessorGroup *group) = 0;

      virtual void register_task(Processor::TaskFuncID func_id,
				 CodeDescriptor& codedesc,
				 const ByteArrayRef& user_data);

    protected:
      friend class Task;

      virtual void execute_task(Processor::TaskFuncID func_id,
				const ByteArrayRef& task_args);

    public:
      Processor me;
      Processor::Kind kind;
      int num_cores;
    }; 

    // generic local task processor - subclasses must create and configure a task
    // scheduler and pass in with the set_scheduler() method
    class LocalTaskProcessor : public ProcessorImpl {
    public:
      LocalTaskProcessor(Processor _me, Processor::Kind _kind, int num_cores=1);
      virtual ~LocalTaskProcessor(void);

      virtual void enqueue_task(Task *task);

      virtual void spawn_task(Processor::TaskFuncID func_id,
			      const void *args, size_t arglen,
                              const ProfilingRequestSet &reqs,
			      Event start_event, Event finish_event,
                              int priority);

      virtual void register_task(Processor::TaskFuncID func_id,
				 CodeDescriptor& codedesc,
				 const ByteArrayRef& user_data);

      // blocks until things are cleaned up
      virtual void shutdown(void);

      virtual void add_to_group(ProcessorGroup *group);

    protected:
      void set_scheduler(ThreadedTaskScheduler *_sched);

    ThreadedTaskScheduler *sched;
    PriorityQueue<Task *, FabMutex> task_queue;
    ProfilingGauges::AbsoluteRangeGauge<int> ready_task_count;

      struct TaskTableEntry {
	Processor::TaskFuncPtr fnptr;
	ByteArray user_data;
      };

      std::map<Processor::TaskFuncID, TaskTableEntry> task_table;

      virtual void execute_task(Processor::TaskFuncID func_id,
				const ByteArrayRef& task_args);
    };

    // three simple subclasses for:
    // a) "CPU" processors, which request a dedicated core and use user threads
    //      when possible
    // b) "utility" processors, which also use user threads but share cores with
    //      other runtime threads
    // c) "IO" processors, which use kernel threads so that blocking IO calls
    //      are permitted
    //
    // each of these is implemented just by supplying the right kind of scheduler to
    //  LocalTaskProcessor in the constructor

    class LocalCPUProcessor : public LocalTaskProcessor {
    public:
      LocalCPUProcessor(Processor _me, CoreReservationSet& crs, size_t _stack_size);
      virtual ~LocalCPUProcessor(void);
    protected:
      CoreReservation *core_rsrv;
    };

    class LocalUtilityProcessor : public LocalTaskProcessor {
    public:
      LocalUtilityProcessor(Processor _me, CoreReservationSet& crs, size_t _stack_size);
      virtual ~LocalUtilityProcessor(void);
    protected:
      CoreReservation *core_rsrv;
    };

    class LocalIOProcessor : public LocalTaskProcessor {
    public:
      LocalIOProcessor(Processor _me, CoreReservationSet& crs, size_t _stack_size,
		       int _concurrent_io_threads);
      virtual ~LocalIOProcessor(void);
    protected:
      CoreReservation *core_rsrv;
    };

    class RemoteProcessor : public ProcessorImpl {
    public:
      RemoteProcessor(Processor _me, Processor::Kind _kind, int _num_cores=1);
      virtual ~RemoteProcessor(void);

      virtual void enqueue_task(Task *task);

      virtual void add_to_group(ProcessorGroup *group);

      virtual void spawn_task(Processor::TaskFuncID func_id,
			      const void *args, size_t arglen,
                              const ProfilingRequestSet &reqs,
			      Event start_event, Event finish_event,
                              int priority);
    };

    class ProcessorGroup : public ProcessorImpl {
    public:
      ProcessorGroup(void);

      virtual ~ProcessorGroup(void);

      static const ID::ID_Types ID_TYPE = ID::ID_PROCGROUP;

      void init(Processor _me, int _owner);

      void set_group_members(const std::vector<Processor>& member_list);

      void get_group_members(std::vector<Processor>& member_list);

      virtual void enqueue_task(Task *task);

      virtual void add_to_group(ProcessorGroup *group);

      virtual void spawn_task(Processor::TaskFuncID func_id,
			      const void *args, size_t arglen,
                              const ProfilingRequestSet &reqs,
			      Event start_event, Event finish_event,
                              int priority);

    public: //protected:
      bool members_valid;
      bool members_requested;
      std::vector<ProcessorImpl *> members;
      ReservationImpl lock;
      ProcessorGroup *next_free;

      void request_group_members(void);

    PriorityQueue<Task *, FabMutex> task_queue;
    ProfilingGauges::AbsoluteRangeGauge<int> *ready_task_count;
  };
    
    // this is generally useful to all processor implementations, so put it here
    class DeferredTaskSpawn : public EventWaiter {
    public:
      DeferredTaskSpawn(ProcessorImpl *_proc, Task *_task) 
        : proc(_proc), task(_task) {}

      virtual ~DeferredTaskSpawn(void)
      {
        // we do _NOT_ own the task - do not free it
      }

      virtual bool event_triggered(Event e, bool poisoned);
      virtual void print(std::ostream& os) const;
      virtual Event get_finish_event(void) const;

    protected:
      ProcessorImpl *proc;
      Task *task;
    };

    // a task registration can take a while if remote processors and/or JITs are
    //  involved
    class TaskRegistration : public Operation {
    public:
      TaskRegistration(const CodeDescriptor& _codedesc,
		       const ByteArrayRef& _userdata,
		       Event _finish_event, const ProfilingRequestSet &_requests);

    protected:
      // deletion performed when reference count goes to zero
      virtual ~TaskRegistration(void);

    public:
      virtual void print(std::ostream& os) const;

      CodeDescriptor codedesc;
      ByteArray userdata;
    };

    class RemoteTaskRegistration : public Operation::AsyncWorkItem {
    public:
      RemoteTaskRegistration(TaskRegistration *reg_op, int _target_node);

      virtual void request_cancellation(void);

      virtual void print(std::ostream& os) const;

  protected:
    int target_node;
  };

  // active messages
  class SpawnTaskMessageType : public MessageType {
  public:
  SpawnTaskMessageType()
    : MessageType(SPAWN_TASK_MSGID, sizeof(RequestArgs), true, true) { }

    struct RequestArgs {
      RequestArgs() { }
      RequestArgs(Processor _proc, Event _start_event, Event _finish_event,
		  size_t _user_arglen, int _priority, Processor::TaskFuncID _func_id)
	: proc(_proc), start_event(_start_event), finish_event(_finish_event),
	  user_arglen(_user_arglen), priority(_priority), func_id(_func_id) { }
      
      Processor proc;
      Event start_event;
      Event finish_event;
      size_t user_arglen;
      int priority;
      Processor::TaskFuncID func_id;
    };

    virtual void request(Message* m);
      
    static void send_request(NodeId target,
			     Processor proc,
			     Processor::TaskFuncID func_id,
			     const void *args,
			     size_t arglen,
			     const ProfilingRequestSet *prs,
			     Event start_event,
			     Event finish_event,
			     int priority);      
  };


  class SpawnTaskMessage : public Message {
  public: 
  SpawnTaskMessage(NodeId dest,
		   Processor proc,
		   Event start_event,
		   Event finish_event,
		   size_t user_arglen,
		   int priority,
		   Processor::TaskFuncID func_id,
		   FabPayload* payload)
    : Message(dest, SPAWN_TASK_MSGID, &args, payload),
      args(proc, start_event, finish_event, user_arglen, priority, func_id) { }

    SpawnTaskMessageType::RequestArgs args;    
  };

  class RegisterTaskMessageType : public MessageType {
  public:
  RegisterTaskMessageType()
    : MessageType(REGISTER_TASK_MSGID, sizeof(RequestArgs), true, true) { }

    struct RequestArgs {
      RequestArgs() { }
      RequestArgs(NodeId _sender,
		  Processor::TaskFuncID _func_id, Processor::Kind _kind, RemoteTaskRegistration* _reg_op)
	: sender(_sender), func_id(_func_id), kind(_kind), reg_op(_reg_op) { }
      NodeId sender;
      Processor::TaskFuncID func_id;
      Processor::Kind kind;
      RemoteTaskRegistration *reg_op;
    };

    void request(Message* m);
      
    static void send_request(NodeId target,
			     Processor::TaskFuncID func_id,
			     Processor::Kind kind,
			     const std::vector<Processor>& procs,
			     const CodeDescriptor& codedesc,
			     const void *userdata, size_t userlen,
			     RemoteTaskRegistration *reg_op);
  };

  class RegisterTaskMessage : public Message {
  public:
  RegisterTaskMessage(NodeId dest, NodeId sender,
		      Processor::TaskFuncID func_id, Processor::Kind kind,
		      RemoteTaskRegistration* reg_op, FabPayload* payload)
    : Message(dest, REGISTER_TASK_MSGID, &args, payload),
      args(sender, func_id, kind, reg_op) { }
    
    RegisterTaskMessageType::RequestArgs args;
  };

  class RegisterTaskCompleteMessageType : public MessageType {
  public:
  RegisterTaskCompleteMessageType()
    : MessageType(REGISTER_TASK_COMPLETE_MSGID, sizeof(RequestArgs), false, true) { }
      
    struct RequestArgs {
      RequestArgs() { }
      RequestArgs(NodeId _sender, RemoteTaskRegistration* _reg_op, bool _successful)
	: sender(_sender), reg_op(_reg_op), successful(_successful) { }
      NodeId sender;
      RemoteTaskRegistration *reg_op;
      bool successful;
    };

    void request(Message* m);
      
    static void send_request(NodeId target,
			     RemoteTaskRegistration *reg_op,
			     bool successful);
  };

  class RegisterTaskCompleteMessage : public Message {
  public:
  RegisterTaskCompleteMessage(NodeId dest, NodeId sender,
			      RemoteTaskRegistration* reg_op,
			      bool successful)
    : Message(dest, REGISTER_TASK_COMPLETE_MSGID, &args, NULL),
      args(sender, reg_op, successful) { }
    
    RegisterTaskCompleteMessageType::RequestArgs args;
  };

}; // namespace Realm

#endif // ifndef REALM_PROC_IMPL_H
