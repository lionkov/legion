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

#include "activemsg.h"
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
      GASNetHSL mutex; // protection for resizing vectors
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
	int sender;
	void *resp_ptr;
	Memory memory;
	size_t size;
      };

      void request(Message* m);
      static off_t send_request(NodeId target, Memory memory, size_t size);
    };

    class RemoteMemAllocRequestMessage : public FabMessage {
    public: 
      RemoteMemAllocRequestMessage(NodeId target, void* args)
	: FabMessage(target, REMOTE_MALLOC_MSGID, args, NULL) { }
    };

    class RemoteMemAllocResponseType : public MessageType {
    public:
      RemoteMemAllocResponseType()
	: MessageType(REMOTE_MALLOC_RPLID, sizeof(RequestArgs), false, true) { }
      
      struct RequestArgs {
	void *resp_ptr;
	off_t offset;
      };

      void request(Message* m);      
    };

    class RemoteMemAllocResponse : public FabMessage {
    public:
      RemoteMemAllocResponse(NodeId dest, void* args)
	: FabMessage(dest, REMOTE_MALLOC_RPLID, args, NULL) { }      
    };

    class CreateInstanceRequestType : public MessageType {
    public:
      CreateInstanceRequestType()
	: MessageType(CREATE_INST_MSGID, sizeof(RequestArgs), true, true) { }

      struct RequestArgs : public BaseMedium {
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

    class CreateInstanceRequest : public FabMessage {
    public:
      CreateInstanceRequest(NodeId dest, void* args, FabPayload* payload) 
	: FabMessage(dest, CREATE_INST_MSGID, args, payload) { }
    };

    class CreateInstanceResponseType : public MessageType {
    public:
      CreateInstanceResponseType()
	: MessageType(CREATE_INST_RPLID, sizeof(RequestArgs), false, true) { }
      
      struct RequestArgs {
	void *resp_ptr;
	RegionInstance i;
	off_t inst_offset;
	off_t count_offset;
      };

      void request(Message* m);
    };

    class CreateInstanceResponse : public FabMessage {
    public:
      CreateInstanceResponse(NodeId dest, void* args)
	: FabMessage(dest, CREATE_INST_RPLID, args, NULL) { }
    };

    class DestroyInstanceMessageType : public MessageType {
    public:
      DestroyInstanceMessageType()
	: MessageType(DESTROY_INST_MSGID, sizeof(RequestArgs), false, true) { }

      struct RequestArgs {
	Memory m;
	RegionInstance i;
      };

      void request(Message* m);
      
      static void send_request(NodeId target,
			       Memory memory,
			       RegionInstance inst);
    };

    class DestroyInstanceMessage : public FabMessage {
    public: 
      DestroyInstanceMessage(NodeId dest, void* args)
	: FabMessage(dest, DESTROY_INST_MSGID, args, NULL) { }
    }; 

    class RemoteWriteMessageType : public MessageType {
    public:
      RemoteWriteMessageType()
	: MessageType(REMOTE_WRITE_MSGID, sizeof(RequestArgs), true, true) { }

      struct RequestArgs : public BaseMedium {
	Memory mem;
	off_t offset;
	unsigned sender;
	unsigned sequence_id;
      };

      void request(Message* m);

      // no simple send_request methodg
    };

    class RemoteWriteMessage : public FabMessage {
    public:
    RemoteWriteMessage(NodeId dest, void* args, FabPayload* payload)
      : FabMessage(dest, REMOTE_WRITE_MSGID, args, payload) { }
    };
    
    class RemoteSerdezMessageType : public MessageType {
    public: 
      RemoteSerdezMessageType()
	: MessageType(REMOTE_SERDEZ_MSGID, sizeof(RequestArgs), true, true) { }

      struct RequestArgs : public BaseMedium {
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

    class RemoteSerdezMessage : public FabMessage {
    public:
      RemoteSerdezMessage(NodeId dest, void* args, FabPayload* payload)
	: FabMessage(dest, REMOTE_SERDEZ_MSGID, args, payload) { }
    };

    class RemoteReduceMessageType : public MessageType {
    public:
      RemoteReduceMessageType()
	: MessageType(REMOTE_REDUCE_MSGID, sizeof(RequestArgs), true, true) { }

      struct RequestArgs : public BaseMedium {
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
    
    class RemoteReduceMessage : public FabMessage {
    public:
      RemoteReduceMessage(NodeId dest, void* args, FabPayload* payload)
	: FabMessage(dest, REMOTE_REDUCE_MSGID, args, payload) { }
    };

    class RemoteReduceListMessageType : public MessageType {
    public:
      RemoteReduceListMessageType()
	: MessageType(REMOTE_REDLIST_MSGID, sizeof(RequestArgs), true, true) { }
      
      struct RequestArgs : public BaseMedium {
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

    class RemoteReduceListMessage : public FabMessage {
    public:
      RemoteReduceListMessage(NodeId dest, void* args, FabPayload* payload)
	: FabMessage(dest, REMOTE_REDLIST_MSGID, args, payload) {  }      
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

    class RemoteWriteFenceMessage : public FabMessage {
    public:
      RemoteWriteFenceMessage(NodeId dest, void* args)
	: FabMessage(dest, REMOTE_WRITE_FENCE_MSGID, args, NULL) { }
    };


    class RemoteWriteFenceAckMessageType : public MessageType {
    public:
      RemoteWriteFenceAckMessageType()
	: MessageType(REMOTE_WRITE_FENCE_ACK_MSGID, sizeof(RequestArgs), false, true) { }
      
      struct RequestArgs {
	RemoteWriteFence *fence;
        // TODO: success/failure
      };

      void request(Message* m);
      static void send_request(NodeId target,
			       RemoteWriteFence *fence);
    };

    class RemoteWriteFenceAckMessage : public FabMessage {
    public:
      RemoteWriteFenceAckMessage(NodeId dest, void* args)
	: FabMessage(dest, REMOTE_WRITE_FENCE_ACK_MSGID, args, NULL) { }
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
				    const SpanList& spans, size_t datalen,
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
    
}; // namespace Realm

#endif // ifndef REALM_MEM_IMPL_H

