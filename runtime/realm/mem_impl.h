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

// Memory implementations for Realm

#ifndef REALM_MEMORY_IMPL_H
#define REALM_MEMORY_IMPL_H

#include "memory.h"
#include "id.h"

#include "fabric.h"
#include "operation.h"
#include "profiling.h"
#include "sampling.h"

#include "event_impl.h"
#include "rsrv_impl.h"
#include <algorithm>

namespace Realm {

  class RegionInstanceImpl;
  
  class MemoryImpl {
  public:
    enum MemoryKind {
      MKIND_SYSMEM,  // directly accessible from CPU
      MKIND_GLOBAL,  // accessible via GASnet (spread over all nodes)
      MKIND_RDMA,    // remote, but accessible via RDMA
      MKIND_REMOTE,  // not accessible

      // defined even if USE_CUDA==0
      // TODO: make kinds more extensible
      MKIND_GPUFB,   // GPU framebuffer memory (accessible via cudaMemcpy)

      MKIND_ZEROCOPY, // CPU memory, pinned for GPU access
      MKIND_DISK,    // disk memory accessible by owner node
      MKIND_FILE,    // file memory accessible by owner node
#ifdef USE_HDF
      MKIND_HDF      // HDF memory accessible by owner node
#endif
    };

    MemoryImpl(Memory _me, size_t _size, MemoryKind _kind, size_t _alignment, Memory::Kind _lowlevel_kind);

    virtual ~MemoryImpl(void);

    unsigned add_instance(RegionInstanceImpl *i);

    RegionInstanceImpl *get_instance(RegionInstance i);

    RegionInstance create_instance_local(IndexSpace is,
					 const int *linearization_bits,
					 size_t bytes_needed,
					 size_t block_size,
					 size_t element_size,
					 const std::vector<size_t>& field_sizes,
					 ReductionOpID redopid,
					 off_t list_size,
					 const ProfilingRequestSet &reqs,
					 RegionInstance parent_inst);

    RegionInstance create_instance_remote(IndexSpace is,
					  const int *linearization_bits,
					  size_t bytes_needed,
					  size_t block_size,
					  size_t element_size,
					  const std::vector<size_t>& field_sizes,
					  ReductionOpID redopid,
					  off_t list_size,
					  const ProfilingRequestSet &reqs,
					  RegionInstance parent_inst);

    virtual RegionInstance create_instance(IndexSpace is,
					   const int *linearization_bits,
					   size_t bytes_needed,
					   size_t block_size,
					   size_t element_size,
					   const std::vector<size_t>& field_sizes,
					   ReductionOpID redopid,
					   off_t list_size,
					   const ProfilingRequestSet &reqs,
					   RegionInstance parent_inst) = 0;

    void destroy_instance_local(RegionInstance i, bool local_destroy);
    void destroy_instance_remote(RegionInstance i, bool local_destroy);

    virtual void destroy_instance(RegionInstance i, 
				  bool local_destroy) = 0;

    off_t alloc_bytes_local(size_t size);
    void free_bytes_local(off_t offset, size_t size);

    off_t alloc_bytes_remote(size_t size);
    void free_bytes_remote(off_t offset, size_t size);

    virtual off_t alloc_bytes(size_t size) = 0;
    virtual void free_bytes(off_t offset, size_t size) = 0;

    virtual void get_bytes(off_t offset, void *dst, size_t size) = 0;
    virtual void put_bytes(off_t offset, const void *src, size_t size) = 0;

    virtual void apply_reduction_list(off_t offset, const ReductionOpUntyped *redop,
				      size_t count, const void *entry_buffer)
    {
      assert(0);
    }

    virtual void *get_direct_ptr(off_t offset, size_t size) = 0;
    virtual int get_home_node(off_t offset, size_t size) = 0;

    virtual void *local_reg_base(void) { return 0; };

    Memory::Kind get_kind(void) const;

  public:
    Memory me;
    size_t size;
    MemoryKind kind;
    size_t alignment;
    Memory::Kind lowlevel_kind;
    MUTEX_T mutex; // protection for resizing vectors
    std::vector<RegionInstanceImpl *> instances;
    std::map<off_t, off_t> free_blocks;
    ProfilingGauges::AbsoluteGauge<size_t> usage, peak_usage, peak_footprint;
  };

  class LocalCPUMemory : public MemoryImpl {
  public:
    static const size_t ALIGNMENT = 256;

    LocalCPUMemory(Memory _me, size_t _size,
		   void *prealloc_base = 0, bool _registered = false);

    virtual ~LocalCPUMemory(void);

    virtual RegionInstance create_instance(IndexSpace r,
					   const int *linearization_bits,
					   size_t bytes_needed,
					   size_t block_size,
					   size_t element_size,
					   const std::vector<size_t>& field_sizes,
					   ReductionOpID redopid,
					   off_t list_size,
					   const ProfilingRequestSet &reqs,
					   RegionInstance parent_inst);
    virtual void destroy_instance(RegionInstance i, 
				  bool local_destroy);
    virtual off_t alloc_bytes(size_t size);
    virtual void free_bytes(off_t offset, size_t size);
    virtual void get_bytes(off_t offset, void *dst, size_t size);
    virtual void put_bytes(off_t offset, const void *src, size_t size);
    virtual void *get_direct_ptr(off_t offset, size_t size);
    virtual int get_home_node(off_t offset, size_t size);
    virtual void *local_reg_base(void);

  public: //protected:
    char *base, *base_orig;
    bool prealloced, registered;
  };

  /*
  class GASNetMemory : public MemoryImpl {
  public:
    static const size_t MEMORY_STRIDE = 1024;

    GASNetMemory(Memory _me, size_t size_per_node);

    virtual ~GASNetMemory(void);

    virtual RegionInstance create_instance(IndexSpace is,
					   const int *linearization_bits,
					   size_t bytes_needed,
					   size_t block_size,
					   size_t element_size,
					   const std::vector<size_t>& field_sizes,
					   ReductionOpID redopid,
					   off_t list_size,
					   const ProfilingRequestSet &reqs,
					   RegionInstance parent_inst);

    virtual void destroy_instance(RegionInstance i, 
				  bool local_destroy);

    virtual off_t alloc_bytes(size_t size);

    virtual void free_bytes(off_t offset, size_t size);

    virtual void get_bytes(off_t offset, void *dst, size_t size);

    virtual void put_bytes(off_t offset, const void *src, size_t size);

    virtual void apply_reduction_list(off_t offset, const ReductionOpUntyped *redop,
				      size_t count, const void *entry_buffer);

    virtual void *get_direct_ptr(off_t offset, size_t size);
    virtual int get_home_node(off_t offset, size_t size);

    void get_batch(size_t batch_size,
		   const off_t *offsets, void * const *dsts, 
		   const size_t *sizes);

    void put_batch(size_t batch_size,
		   const off_t *offsets, const void * const *srcs, 
		   const size_t *sizes);

  protected:
    int num_nodes;
    off_t memory_stride;
    gasnet_seginfo_t *seginfos;
    //std::map<off_t, off_t> free_blocks;
  };
  */
  
  class DiskMemory : public MemoryImpl {
  public:
    static const size_t ALIGNMENT = 256;

    DiskMemory(Memory _me, size_t _size, std::string _file);

    virtual ~DiskMemory(void);

    virtual RegionInstance create_instance(IndexSpace is,
					   const int *linearization_bits,
					   size_t bytes_needed,
					   size_t block_size,
					   size_t element_size,
					   const std::vector<size_t>& field_sizes,
					   ReductionOpID redopid,
					   off_t list_size,
					   const ProfilingRequestSet &reqs,
					   RegionInstance parent_inst);

    virtual void destroy_instance(RegionInstance i,
				  bool local_destroy);

    virtual off_t alloc_bytes(size_t size);

    virtual void free_bytes(off_t offset, size_t size);

    virtual void get_bytes(off_t offset, void *dst, size_t size);

    virtual void put_bytes(off_t offset, const void *src, size_t size);

    virtual void apply_reduction_list(off_t offset, const ReductionOpUntyped *redop,
				      size_t count, const void *entry_buffer);

    virtual void *get_direct_ptr(off_t offset, size_t size);
    virtual int get_home_node(off_t offset, size_t size);

  public:
    int fd; // file descriptor
    std::string file;  // file name
  };

  class FileMemory : public MemoryImpl {
  public:
    static const size_t ALIGNMENT = 256;

    FileMemory(Memory _me);

    virtual ~FileMemory(void);

    virtual RegionInstance create_instance(IndexSpace is,
					   const int *linearization_bits,
					   size_t bytes_needed,
					   size_t block_size,
					   size_t element_size,
					   const std::vector<size_t>& field_sizes,
					   ReductionOpID redopid,
					   off_t list_size,
					   const ProfilingRequestSet &reqs,
					   RegionInstance parent_inst);

    RegionInstance create_instance(IndexSpace is,
				   const int *linearization_bits,
				   size_t bytes_needed,
				   size_t block_size,
				   size_t element_size,
				   const std::vector<size_t>& field_sizes,
				   ReductionOpID redopid,
				   off_t list_size,
				   const ProfilingRequestSet &reqs,
				   RegionInstance parent_inst,
				   const char *file_name,
				   Domain domain,
				   legion_lowlevel_file_mode_t file_mode);
    virtual void destroy_instance(RegionInstance i,
				  bool local_destroy);


    virtual off_t alloc_bytes(size_t size);

    virtual void free_bytes(off_t offset, size_t size);

    virtual void get_bytes(off_t offset, void *dst, size_t size);
    void get_bytes(ID::IDType inst_id, off_t offset, void *dst, size_t size);

    virtual void put_bytes(off_t offset, const void *src, size_t size);
    void put_bytes(ID::IDType inst_id, off_t offset, const void *src, size_t size);

    virtual void apply_reduction_list(off_t offset, const ReductionOpUntyped *redop,
				      size_t count, const void *entry_buffer);

    virtual void *get_direct_ptr(off_t offset, size_t size);
    virtual int get_home_node(off_t offset, size_t size);

    int get_file_des(ID::IDType inst_id);
  public:
    std::vector<int> file_vec;
    pthread_mutex_t vector_lock;
  };

#ifdef USE_HDF
  class HDFMemory : public MemoryImpl {
  public:
    static const size_t ALIGNMENT = 256;

    HDFMemory(Memory _me);

    virtual ~HDFMemory(void);

    virtual RegionInstance create_instance(IndexSpace is,
					   const int *linearization_bits,
					   size_t bytes_needed,
					   size_t block_size,
					   size_t element_size,
					   const std::vector<size_t>& field_sizes,
					   ReductionOpID redopid,
					   off_t list_size,
					   const ProfilingRequestSet &reqs,
					   RegionInstance parent_inst);

    RegionInstance create_instance(IndexSpace is,
				   const int *linearization_bits,
				   size_t bytes_needed,
				   size_t block_size,
				   size_t element_size,
				   const std::vector<size_t>& field_sizes,
				   ReductionOpID redopid,
				   off_t list_size,
				   const ProfilingRequestSet &reqs,
				   RegionInstance parent_inst,
				   const char* file,
				   const std::vector<const char*>& path_names,
				   Domain domain,
				   bool read_only);

    virtual void destroy_instance(RegionInstance i,
				  bool local_destroy);

    virtual off_t alloc_bytes(size_t size);

    virtual void free_bytes(off_t offset, size_t size);

    virtual void get_bytes(off_t offset, void *dst, size_t size);
    void get_bytes(ID::IDType inst_id, const DomainPoint& dp, int fid, void *dst, size_t size);

    virtual void put_bytes(off_t offset, const void *src, size_t size);
    void put_bytes(ID::IDType inst_id, const DomainPoint& dp, int fid, const void *src, size_t size);

    virtual void apply_reduction_list(off_t offset, const ReductionOpUntyped *redop,
				      size_t count, const void *entry_buffer);

    virtual void *get_direct_ptr(off_t offset, size_t size);
    virtual int get_home_node(off_t offset, size_t size);

  public:
    struct HDFMetadata {
      int lo[3];
      hsize_t dims[3];
      int ndims;
      hid_t type_id;
      hid_t file_id;
      std::vector<hid_t> dataset_ids;
      std::vector<hid_t> datatype_ids;
    };
    std::vector<HDFMetadata*> hdf_metadata;
  };
#endif

  class RemoteMemory : public MemoryImpl {
  public:
    RemoteMemory(Memory _me, size_t _size, Memory::Kind k, void *_regbase);
    virtual ~RemoteMemory(void);

    virtual RegionInstance create_instance(IndexSpace r,
					   const int *linearization_bits,
					   size_t bytes_needed,
					   size_t block_size,
					   size_t element_size,
					   const std::vector<size_t>& field_sizes,
					   ReductionOpID redopid,
					   off_t list_size,
					   const ProfilingRequestSet &reqs,
					   RegionInstance parent_inst);
    virtual void destroy_instance(RegionInstance i, 
				  bool local_destroy);
    virtual off_t alloc_bytes(size_t size);
    virtual void free_bytes(off_t offset, size_t size);
    virtual void get_bytes(off_t offset, void *dst, size_t size);
    virtual void put_bytes(off_t offset, const void *src, size_t size);
    virtual void *get_direct_ptr(off_t offset, size_t size);
    virtual int get_home_node(off_t offset, size_t size);

  public:
    void *regbase;
  };


  // active messages
    
  class RemoteMemAllocRequestType : public MessageType {
  public:
  RemoteMemAllocRequestType()
    : MessageType(REMOTE_MALLOC_MSGID, sizeof(RequestArgs), false, true) { }
      
    struct RequestArgs {
      RequestArgs() { }
      RequestArgs(int _sender, void* _resp_ptr, Memory _memory, size_t _size)
	: sender(_sender), resp_ptr(_resp_ptr), memory(_memory), size(_size) { }
      int sender;
      void *resp_ptr;
      Memory memory;
      size_t size;
    };

    void request(Message* m);
    static off_t send_request(NodeId target, Memory memory, size_t size);
  };

  class RemoteMemAllocRequestMessage : public Message {
  public: 
  RemoteMemAllocRequestMessage(NodeId target, int sender,
			       void* resp_ptr, Memory memory, size_t size)
    : Message(target, REMOTE_MALLOC_MSGID, &args, NULL),
      args(sender, resp_ptr, memory, size) { }
    
    RemoteMemAllocRequestType::RequestArgs args;
  };

  class RemoteMemAllocResponseType : public MessageType {
  public:
  RemoteMemAllocResponseType()
    : MessageType(REMOTE_MALLOC_RPLID, sizeof(RequestArgs), false, true) { }
      
    struct RequestArgs {
      RequestArgs() { }
      RequestArgs(void* _resp_ptr, off_t _offset)
	: resp_ptr(_resp_ptr), offset(_offset) { }
      void* resp_ptr;
      off_t offset;
    };

    void request(Message* m);      
  };

  class RemoteMemAllocResponse : public Message {
  public:
  RemoteMemAllocResponse(NodeId dest, void* resp_ptr, off_t offset)
    : Message(dest, REMOTE_MALLOC_RPLID, &args, NULL),
      args(resp_ptr, offset) { }

    RemoteMemAllocResponseType::RequestArgs args;
  };

  class CreateInstanceRequestType : public MessageType {
  public:
  CreateInstanceRequestType()
    : MessageType(CREATE_INST_MSGID, sizeof(RequestArgs), true, true) { }

    struct RequestArgs {
      RequestArgs() { }
      RequestArgs(Memory _m, IndexSpace _r, RegionInstance _parent_inst, int _sender, void* _resp_ptr)
	: m(_m), r(_r), parent_inst(_parent_inst), sender(_sender), resp_ptr(_resp_ptr) { }
      Memory m;
      IndexSpace r;
      RegionInstance parent_inst;
      int sender;
      void *resp_ptr;
    };

    // TODO: replace with new serialization stuff
    struct PayloadData {
      size_t bytes_needed;
      size_t block_size;
      size_t element_size;
      //off_t adjust;
      off_t list_size;
      ReductionOpID redopid;
      int linearization_bits[16]; //RegionInstanceImpl::MAX_LINEARIZATION_LEN];
      size_t num_fields; // as long as it needs to be
      const size_t &field_size(int idx) const { return *((&num_fields)+idx+1); }
      size_t &field_size(int idx) { return *((&num_fields)+idx+1); }
    };

    struct Result {
      RegionInstance i;
      off_t inst_offset;
      off_t count_offset;
    };


    void request(Message* m);
      
    static void send_request(Result *result,
			     NodeId target, Memory memory, IndexSpace ispace,
			     RegionInstance parent_inst, size_t bytes_needed,
			     size_t block_size, size_t element_size,
			     off_t list_size, ReductionOpID redopid,
			     const int *linearization_bits,
			     const std::vector<size_t>& field_sizes,
			     const ProfilingRequestSet *prs);
  };

  class CreateInstanceRequest : public Message {
  public:
  CreateInstanceRequest(NodeId dest, Memory m, IndexSpace r, RegionInstance parent_inst,
			int sender, void* resp_ptr, FabPayload* payload) 
    : Message(dest, CREATE_INST_MSGID, &args, payload),
      args(m, r, parent_inst, sender, resp_ptr) { }
    
    CreateInstanceRequestType::RequestArgs args;
  };

  class CreateInstanceResponseType : public MessageType {
  public:
  CreateInstanceResponseType()
    : MessageType(CREATE_INST_RPLID, sizeof(RequestArgs), false, true) { }
      
    struct RequestArgs {
      RequestArgs() { }
      RequestArgs(void* _resp_ptr, RegionInstance _i, off_t _inst_offset, off_t _count_offset)
	: resp_ptr(_resp_ptr), i(_i), inst_offset(_inst_offset), count_offset(_count_offset) { }
      void *resp_ptr;
      RegionInstance i;
      off_t inst_offset;
      off_t count_offset;
    };

    void request(Message* m);
  };

  class CreateInstanceResponse : public Message {
  public:
  CreateInstanceResponse(NodeId dest, void* resp_ptr, RegionInstance i,
			 off_t inst_offset, off_t count_offset)
    : Message(dest, CREATE_INST_RPLID, &args, NULL),
      args(resp_ptr, i, inst_offset, count_offset) { }

    CreateInstanceResponseType::RequestArgs args;
  };

  class DestroyInstanceMessageType : public MessageType {
  public:
  DestroyInstanceMessageType()
    : MessageType(DESTROY_INST_MSGID, sizeof(RequestArgs), false, true) { }

    struct RequestArgs {
      RequestArgs() { }
      RequestArgs(Memory _m, RegionInstance _i)
	: m(_m), i(_i) { }
      Memory m;
      RegionInstance i;
    };

    void request(Message* m);
      
    static void send_request(NodeId target,
			     Memory memory,
			     RegionInstance inst);
  };

  class DestroyInstanceMessage : public Message {
  public: 
  DestroyInstanceMessage(NodeId dest, Memory m, RegionInstance i)
    : Message(dest, DESTROY_INST_MSGID, &args, NULL),
      args(m, i) { }
    DestroyInstanceMessageType::RequestArgs args;
  }; 

  class RemoteWriteMessageType : public MessageType {
  public:
  RemoteWriteMessageType()
    : MessageType(REMOTE_WRITE_MSGID, sizeof(RequestArgs), true, true) { }

    struct RequestArgs  {
      RequestArgs() { }
      RequestArgs(Memory _mem, off_t _offset, unsigned _sender, unsigned _sequence_id)
	: mem(_mem), offset(_offset), sender(_sender), sequence_id(_sequence_id) { }
      Memory mem;
      off_t offset;
      unsigned sender;
      unsigned sequence_id;
    };

    void request(Message* m);
    // no simple send_request method
  };

  class RemoteWriteMessage : public Message {
  public:
  RemoteWriteMessage(NodeId dest, Memory mem, off_t offset, unsigned sender,
		     unsigned sequence_id, FabPayload* payload)
    : Message(dest, REMOTE_WRITE_MSGID, &args, payload),
      args(mem, offset, sender, sequence_id) { }
    
    RemoteWriteMessageType::RequestArgs args;
  };
    
  class RemoteSerdezMessageType : public MessageType {
  public: 
  RemoteSerdezMessageType()
    : MessageType(REMOTE_SERDEZ_MSGID, sizeof(RequestArgs), true, true) { }

    struct RequestArgs  {
      RequestArgs() { }
    RequestArgs(Memory _mem, off_t _offset, size_t _count, CustomSerdezID _serdez_id,
		unsigned _sender, unsigned _sequence_id)
      : mem(_mem), offset(_offset), count(_count), serdez_id(_serdez_id),
	sender(_sender), sequence_id(_sequence_id) { }
      Memory mem;
      off_t offset;
      size_t count;
      CustomSerdezID serdez_id;
      unsigned sender;
      unsigned sequence_id;
    };

    void request(Message* m);
    // no simple send_request method here - see below      
  };

  class RemoteSerdezMessage : public Message {
  public:
  RemoteSerdezMessage(NodeId dest, Memory mem, off_t offset, size_t count,
		      CustomSerdezID serdez_id, unsigned sender, unsigned sequence_id,
		      FabPayload* payload)
    : Message(dest, REMOTE_SERDEZ_MSGID, &args, payload),
      args(mem, offset, count, serdez_id, sender, sequence_id) { }

    RemoteSerdezMessageType::RequestArgs args;
  };

  class RemoteReduceMessageType : public MessageType {
  public:
  RemoteReduceMessageType()
    : MessageType(REMOTE_REDUCE_MSGID, sizeof(RequestArgs), true, true) { }

    struct RequestArgs {
      RequestArgs() { }
      RequestArgs(Memory _mem, off_t _offset, int _stride, ReductionOpID _redop_id,
		  unsigned _sender, unsigned _sequence_id)
	: mem(_mem), offset(_offset), stride(_stride), redop_id(_redop_id), sender(_sender), sequence_id(_sequence_id) { }
      Memory mem;
      off_t offset;
      int stride;
      ReductionOpID redop_id;
      //bool red_fold;
      unsigned sender;
      unsigned sequence_id;
    };

    void request(Message* m);
    // no simple send_request method here - see below      
  };
    
  class RemoteReduceMessage : public Message {
  public:
  RemoteReduceMessage(NodeId dest, Memory mem, off_t offset, int stride,
		      ReductionOpID redop_id, unsigned sender, unsigned sequence_id,
		      FabPayload* payload)
    : Message(dest, REMOTE_REDUCE_MSGID, &args, payload),
      args(mem, offset, stride, redop_id, sender, sequence_id) { }

    RemoteReduceMessageType::RequestArgs args;
  };

  class RemoteReduceListMessageType : public MessageType {
  public:
  RemoteReduceListMessageType()
    : MessageType(REMOTE_REDLIST_MSGID, sizeof(RequestArgs), true, true) { }
      
    struct RequestArgs  {
      RequestArgs() { }
      RequestArgs(Memory _mem, off_t _offset, ReductionOpID _redopid)
	: mem(_mem), offset(_offset), redopid(_redopid) { }
      Memory mem;
      off_t offset;
      ReductionOpID redopid;
    };

    void request(Message* m);

    static void send_request(NodeId target, Memory mem, off_t offset,
			     ReductionOpID redopid,
			     const void *data, size_t datalen,
			     int payload_mode);
  };

  class RemoteReduceListMessage : public Message {
  public:
  RemoteReduceListMessage(NodeId dest, Memory mem, off_t offset,
			  ReductionOpID redopid, FabPayload* payload)
    : Message(dest, REMOTE_REDLIST_MSGID, &args, payload),
      args(mem, offset, redopid) { }
    
    RemoteReduceListMessageType::RequestArgs args;
  };
        
  class RemoteWriteFence : public Operation::AsyncWorkItem {
  public:
    RemoteWriteFence(Operation *op);

    virtual void request_cancellation(void);

    virtual void print(std::ostream& os) const;
  };

  class RemoteWriteFenceMessageType : public MessageType {
  public:
  RemoteWriteFenceMessageType()
    : MessageType(REMOTE_WRITE_FENCE_MSGID, sizeof(RequestArgs), false, true) { }

    struct RequestArgs {
      RequestArgs() { }
      RequestArgs(Memory _mem, unsigned _sender, unsigned _sequence_id, unsigned _num_writes,
		  RemoteWriteFence* _fence)
	: mem(_mem), sender(_sender), sequence_id(_sequence_id), num_writes(_num_writes), fence(_fence) { }
      Memory mem;
      unsigned sender;
      unsigned sequence_id;
      unsigned num_writes;
      RemoteWriteFence *fence;
    };

    void request(Message* m);

    static void send_request(NodeId target, Memory memory,
			     unsigned sequence_id, unsigned num_writes,
			     RemoteWriteFence *fence);
  };

  class RemoteWriteFenceMessage : public Message {
  public:
  RemoteWriteFenceMessage(NodeId dest, Memory mem, unsigned sender,
			  unsigned sequence_id, unsigned num_writes,
			  RemoteWriteFence* fence)
    : Message(dest, REMOTE_WRITE_FENCE_MSGID, &args, NULL),
      args(mem, sender, sequence_id, num_writes, fence) { }
    
    RemoteWriteFenceMessageType::RequestArgs args;
  };


  class RemoteWriteFenceAckMessageType : public MessageType {
  public:
  RemoteWriteFenceAckMessageType()
    : MessageType(REMOTE_WRITE_FENCE_ACK_MSGID, sizeof(RequestArgs), false, true) { }
      
    struct RequestArgs {
            RequestArgs() { }
      RequestArgs(RemoteWriteFence* _fence)
	: fence(_fence) { }
      RemoteWriteFence *fence;
      // TODO: success/failure
    };

    void request(Message* m);
    static void send_request(NodeId target,
			     RemoteWriteFence *fence);
  };

  class RemoteWriteFenceAckMessage : public Message {
  public:
  RemoteWriteFenceAckMessage(NodeId dest, RemoteWriteFence* fence)
    : Message(dest, REMOTE_WRITE_FENCE_ACK_MSGID, &args, NULL),
      args(fence) { }
    
    RemoteWriteFenceAckMessageType::RequestArgs args;
  };
    
  // remote memory writes

  extern unsigned do_remote_write(Memory mem, off_t offset,
				  const void *data, size_t datalen,
				  unsigned sequence_id,
				  bool make_copy = false);

  extern unsigned do_remote_write(Memory mem, off_t offset,
				  const void *data, size_t line_len,
				  off_t stride, size_t lines,
				  unsigned sequence_id,
				  bool make_copy = false);
    
  extern unsigned do_remote_write(Memory mem, off_t offset,
				  SpanList* spans, size_t datalen,
				  unsigned sequence_id,
				  bool make_copy = false);
    
  extern unsigned do_remote_serdez(Memory mem, off_t offset,
				   CustomSerdezID serdez_id,
				   const void *data, size_t datalen,
				   unsigned sequence_id);
    
  extern unsigned do_remote_reduce(Memory mem, off_t offset,
				   ReductionOpID redop_id, bool red_fold,
				   const void *data, size_t count,
				   off_t src_stride, off_t dst_stride,
				   unsigned sequence_id,
				   bool make_copy = false);				     

  extern unsigned do_remote_apply_red_list(int node, Memory mem, off_t offset,
					   ReductionOpID redopid,
					   const void *data, size_t datalen,
					   unsigned sequence_id);

  extern void do_remote_fence(Memory mem, unsigned sequence_id,
			      unsigned count, RemoteWriteFence *fence);


  template <class T> struct HandlerReplyFuture {
    MUTEX_T mutex;
    MUTEX_T condmutex;
    CONDVAR_T	 cond;
    bool valid;
    T value;

    HandlerReplyFuture(void)
      : cond(condmutex) {
      valid = false;
    }

    void set(T newval)
    {
      mutex.lock();
      valid = true;
      value = newval;
      cond.broadcast();
      mutex.lock();
    }

    bool is_set(void) const { return valid; }

    void wait(void)
    {
      if(valid) return; // early out
      mutex.lock();
      while(!valid) cond.wait();
      mutex.lock();
    }

    T get(void) const { return value; }
  };    
}; // namespace Realm

#endif // ifndef REALM_MEM_IMPL_H

