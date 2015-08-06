/* Copyright 2015 Stanford University, NVIDIA Corporation
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

// IndexSpace implementation for Realm

#include "idx_impl.h"

#include "logging.h"
#include "inst_impl.h"
#include "mem_impl.h"
#include "runtime_impl.h"

namespace Realm {

  Logger log_meta("meta");
  Logger log_region("region");
  Logger log_copy("copy");

  ////////////////////////////////////////////////////////////////////////
  //
  // class IndexSpace
  //

    /*static*/ const IndexSpace IndexSpace::NO_SPACE = { 0 };
    /*static*/ const Domain Domain::NO_DOMAIN = Domain();

    /*static*/ IndexSpace IndexSpace::create_index_space(size_t num_elmts)
    {
      DetailedTimer::ScopedPush sp(TIME_LOW_LEVEL);

      IndexSpaceImpl *impl = get_runtime()->local_index_space_free_list->alloc_entry();
      
      impl->init(impl->me, NO_SPACE, num_elmts);
      
      log_meta.info("index space created: id=" IDFMT " num_elmts=%zd",
	       impl->me.id, num_elmts);
      return impl->me;
    }

    /*static*/ IndexSpace IndexSpace::create_index_space(const ElementMask &mask)
    {
      DetailedTimer::ScopedPush sp(TIME_LOW_LEVEL);

      IndexSpaceImpl *impl = get_runtime()->local_index_space_free_list->alloc_entry();
      
      // TODO: actually decide when to safely consider a subregion frozen
      impl->init(impl->me, NO_SPACE, mask.get_num_elmts(), &mask, true);
      
      log_meta.info("index space created: id=" IDFMT " num_elmts=%d",
	       impl->me.id, mask.get_num_elmts());
      return impl->me;
    }

    /*static*/ IndexSpace IndexSpace::create_index_space(IndexSpace parent, const ElementMask &mask, bool allocable)
    {
      DetailedTimer::ScopedPush sp(TIME_LOW_LEVEL);

      IndexSpaceImpl *impl = get_runtime()->local_index_space_free_list->alloc_entry();
      assert(impl);
      assert(ID(impl->me).type() == ID::ID_INDEXSPACE);

      StaticAccess<IndexSpaceImpl> p_data(get_runtime()->get_index_space_impl(parent));

      impl->init(impl->me, parent,
		 p_data->num_elmts, 
		 &mask,
		 !allocable);  // TODO: actually decide when to safely consider a subregion frozen
      
      log_meta.info("index space created: id=" IDFMT " parent=" IDFMT " (num_elmts=%zd)",
	       impl->me.id, parent.id, p_data->num_elmts);
      return impl->me;
    }

    IndexSpaceAllocator IndexSpace::create_allocator(void) const
    {
      DetailedTimer::ScopedPush sp(TIME_LOW_LEVEL);
      IndexSpaceAllocatorImpl *a_impl = new IndexSpaceAllocatorImpl(get_runtime()->get_index_space_impl(*this));
      return IndexSpaceAllocator(a_impl);
    }

    void IndexSpace::destroy(Event wait_on) const
    {
      assert(wait_on.has_triggered());
      //assert(0);
    }

    void IndexSpaceAllocator::destroy(void) 
    {
      DetailedTimer::ScopedPush sp(TIME_LOW_LEVEL);
      if (impl != NULL)
      {
        delete (IndexSpaceAllocatorImpl *)impl;
        // Avoid double frees
        impl = NULL;
      }
    }

    const ElementMask &IndexSpace::get_valid_mask(void) const
    {
      DetailedTimer::ScopedPush sp(TIME_LOW_LEVEL);
      IndexSpaceImpl *r_impl = get_runtime()->get_index_space_impl(*this);
#ifdef COHERENT_BUT_BROKEN_WAY
      // for now, just hand out the valid mask for the master allocator
      //  and hope it's accessible to the caller
      SharedAccess<IndexSpaceImpl> data(r_impl);
      assert((data->valid_mask_owners >> gasnet_mynode()) & 1);
#else
      if(!r_impl->valid_mask_complete) {
	Event wait_on = r_impl->request_valid_mask();
	
	log_copy.info("missing valid mask (" IDFMT "/%p) - waiting for " IDFMT "/%d",
		      id, r_impl->valid_mask,
		      wait_on.id, wait_on.gen);

	wait_on.wait();
      }
#endif
      return *(r_impl->valid_mask);
    }

    Event IndexSpace::create_equal_subspaces(size_t count, size_t granularity,
                                             std::vector<IndexSpace>& subspaces,
                                             bool mutable_results,
                                             Event wait_on /*= Event::NO_EVENT*/) const
    {
      // TODO: Implement this
      assert(false);
      return Event::NO_EVENT;
    }

    Event IndexSpace::create_equal_subspaces(size_t count, size_t granularity,
                                             std::vector<IndexSpace>& subspaces,
                                             const ProfilingRequestSet &reqs,
                                             bool mutable_results,
                                             Event wait_on /*= Event::NO_EVENT*/) const
    {
      // TODO: Implement this
      assert(false);
      return Event::NO_EVENT;
    }

    Event IndexSpace::create_weighted_subspaces(size_t count, size_t granularity,
                                                const std::vector<int>& weights,
                                                std::vector<IndexSpace>& subspaces,
                                                bool mutable_results,
                                                Event wait_on /*= Event::NO_EVENT*/) const
    {
      // TODO: Implement this
      assert(false);
      return Event::NO_EVENT;
    }

    Event IndexSpace::create_weighted_subspaces(size_t count, size_t granularity,
                                                const std::vector<int>& weights,
                                                std::vector<IndexSpace>& subspaces,
                                                const ProfilingRequestSet &reqs,
                                                bool mutable_results,
                                                Event wait_on /*= Event::NO_EVENT*/) const
    {
      // TODO: Implement this
      assert(false);
      return Event::NO_EVENT;
    }

    /*static*/
    Event IndexSpace::compute_index_spaces(std::vector<BinaryOpDescriptor>& pairs,
                                           bool mutable_results,
					   Event wait_on /*= Event::NO_EVENT*/)
    {
      // TODO: Implement this
      assert(false);
      return Event::NO_EVENT;
    }

    /*static*/
    Event IndexSpace::compute_index_spaces(std::vector<BinaryOpDescriptor>& pairs,
                                           const ProfilingRequestSet &reqs,
                                           bool mutable_results,
					   Event wait_on /*= Event::NO_EVENT*/)
    {
      // TODO: Implement this
      assert(false);
      return Event::NO_EVENT;
    }

    /*static*/
    Event IndexSpace::reduce_index_spaces(IndexSpaceOperation op,
                                          const std::vector<IndexSpace>& spaces,
                                          IndexSpace& result,
                                          bool mutable_results,
                                          IndexSpace parent /*= IndexSpace::NO_SPACE*/,
				          Event wait_on /*= Event::NO_EVENT*/)
    {
      // TODO: Implement this
      assert(false);
      return Event::NO_EVENT;
    }

    /*static*/
    Event IndexSpace::reduce_index_spaces(IndexSpaceOperation op,
                                          const std::vector<IndexSpace>& spaces,
                                          const ProfilingRequestSet &reqs,
                                          IndexSpace& result,
                                          bool mutable_results,
                                          IndexSpace parent /*= IndexSpace::NO_SPACE*/,
				          Event wait_on /*= Event::NO_EVENT*/)
    {
      // TODO: Implement this
      assert(false);
      return Event::NO_EVENT;
    }

    Event IndexSpace::create_subspaces_by_field(
                                const std::vector<FieldDataDescriptor>& field_data,
                                std::map<DomainPoint, IndexSpace>& subspaces,
                                bool mutable_results,
                                Event wait_on /*= Event::NO_EVENT*/) const
    {
      // TODO: Implement this
      assert(false);
      return Event::NO_EVENT;
    }

    Event IndexSpace::create_subspaces_by_field(
                                const std::vector<FieldDataDescriptor>& field_data,
                                std::map<DomainPoint, IndexSpace>& subspaces,
                                const ProfilingRequestSet &reqs,
                                bool mutable_results,
                                Event wait_on /*= Event::NO_EVENT*/) const
    {
      // TODO: Implement this
      assert(false);
      return Event::NO_EVENT;
    }

    Event IndexSpace::create_subspaces_by_image(
                                const std::vector<FieldDataDescriptor>& field_data,
                                std::map<IndexSpace, IndexSpace>& subspaces,
                                bool mutable_results,
                                Event wait_on /*= Event::NO_EVENT*/) const
    {
      // TODO: Implement this
      assert(false);
      return Event::NO_EVENT;
    }

    Event IndexSpace::create_subspaces_by_image(
                                const std::vector<FieldDataDescriptor>& field_data,
                                std::map<IndexSpace, IndexSpace>& subspaces,
                                const ProfilingRequestSet &reqs,
                                bool mutable_results,
                                Event wait_on /*= Event::NO_EVENT*/) const
    {
      // TODO: Implement this
      assert(false);
      return Event::NO_EVENT;
    }

    Event IndexSpace::create_subspaces_by_preimage(
                                 const std::vector<FieldDataDescriptor>& field_data,
                                 std::map<IndexSpace, IndexSpace>& subspaces,
                                 bool mutable_results,
                                 Event wait_on /*= Event::NO_EVENT*/) const
    {
      // TODO: Implement this
      assert(false);
      return Event::NO_EVENT;
    }

    Event IndexSpace::create_subspaces_by_preimage(
                                 const std::vector<FieldDataDescriptor>& field_data,
                                 std::map<IndexSpace, IndexSpace>& subspaces,
                                 const ProfilingRequestSet &reqs,
                                 bool mutable_results,
                                 Event wait_on /*= Event::NO_EVENT*/) const
    {
      // TODO: Implement this
      assert(false);
      return Event::NO_EVENT;
    }

  
  ////////////////////////////////////////////////////////////////////////
  //
  // class Domain
  //

    RegionInstance Domain::create_instance(Memory memory,
					   size_t elem_size,
					   ReductionOpID redop_id) const
    {
      DetailedTimer::ScopedPush sp(TIME_LOW_LEVEL);      
      std::vector<size_t> field_sizes(1);
      field_sizes[0] = elem_size;

      return create_instance(memory, field_sizes, 1, redop_id);
    }

    RegionInstance Domain::create_instance(Memory memory,
					   size_t elem_size,
                                           const ProfilingRequestSet &reqs,
					   ReductionOpID redop_id) const
    {
      DetailedTimer::ScopedPush sp(TIME_LOW_LEVEL);      
      std::vector<size_t> field_sizes(1);
      field_sizes[0] = elem_size;

      return create_instance(memory, field_sizes, 1, reqs, redop_id);
    }

    RegionInstance Domain::create_instance(Memory memory,
					   const std::vector<size_t> &field_sizes,
					   size_t block_size,
					   ReductionOpID redop_id) const
    {
      ProfilingRequestSet requests;
      return create_instance(memory, field_sizes, block_size, requests, redop_id);
    }

    RegionInstance Domain::create_instance(Memory memory,
					   const std::vector<size_t> &field_sizes,
					   size_t block_size,
                                           const ProfilingRequestSet &reqs,
					   ReductionOpID redop_id) const
    {
      DetailedTimer::ScopedPush sp(TIME_LOW_LEVEL);      
      ID id(memory);

      MemoryImpl *m_impl = get_runtime()->get_memory_impl(memory);

      size_t elem_size = 0;
      for(std::vector<size_t>::const_iterator it = field_sizes.begin();
	  it != field_sizes.end();
	  it++)
	elem_size += *it;

      size_t num_elements;
      int linearization_bits[RegionInstanceImpl::MAX_LINEARIZATION_LEN];
      if(get_dim() > 0) {
	// we have a rectangle - figure out its volume and create based on that
	LegionRuntime::Arrays::Rect<1> inst_extent;
	switch(get_dim()) {
	case 1:
	  {
	    std::vector<LegionRuntime::Layouts::DimKind> kind_vec;
	    std::vector<size_t> size_vec;
	    kind_vec.push_back(LegionRuntime::Layouts::DIM_X);
	    size_vec.push_back(get_rect<1>().dim_size(0));
	    LegionRuntime::Layouts::SplitDimLinearization<1> cl(kind_vec, size_vec);
	    //Arrays::FortranArrayLinearization<1> cl(get_rect<1>(), 0);
	    DomainLinearization dl = DomainLinearization::from_mapping<1>(LegionRuntime::Arrays::Mapping<1, 1>::new_dynamic_mapping(cl));
	    inst_extent = cl.image_convex(get_rect<1>());
	    dl.serialize(linearization_bits);
	    break;
	  }

	case 2:
	  {
	    std::vector<LegionRuntime::Layouts::DimKind> kind_vec;
	    std::vector<size_t> size_vec;
	    kind_vec.push_back(LegionRuntime::Layouts::DIM_X);
	    kind_vec.push_back(LegionRuntime::Layouts::DIM_Y);
	    size_vec.push_back(get_rect<2>().dim_size(0));
	    size_vec.push_back(get_rect<2>().dim_size(1));
	    LegionRuntime::Layouts::SplitDimLinearization<2> cl(kind_vec, size_vec);
	    //Arrays::FortranArrayLinearization<2> cl(get_rect<2>(), 0);
	    DomainLinearization dl = DomainLinearization::from_mapping<2>(LegionRuntime::Arrays::Mapping<2, 1>::new_dynamic_mapping(cl));
	    inst_extent = cl.image_convex(get_rect<2>());
	    dl.serialize(linearization_bits);
	    break;
	  }

	case 3:
	  {
	    std::vector<LegionRuntime::Layouts::DimKind> kind_vec;
	    std::vector<size_t> size_vec;
	    kind_vec.push_back(LegionRuntime::Layouts::DIM_X);
	    kind_vec.push_back(LegionRuntime::Layouts::DIM_Y);
	    kind_vec.push_back(LegionRuntime::Layouts::DIM_Z);
	    size_vec.push_back(get_rect<3>().dim_size(0));
	    size_vec.push_back(get_rect<3>().dim_size(1));
	    size_vec.push_back(get_rect<3>().dim_size(2));
	    LegionRuntime:: Layouts::SplitDimLinearization<3> cl(kind_vec, size_vec);
	    //Arrays::FortranArrayLinearization<3> cl(get_rect<3>(), 0);
	    DomainLinearization dl = DomainLinearization::from_mapping<3>(LegionRuntime::Arrays::Mapping<3, 1>::new_dynamic_mapping(cl));
	    inst_extent = cl.image_convex(get_rect<3>());
	    dl.serialize(linearization_bits);
	    break;
	  }

	default: assert(0);
	}
	num_elements = inst_extent.volume();
	//printf("num_elements = %zd\n", num_elements);
      } else {
	IndexSpaceImpl *r = get_runtime()->get_index_space_impl(get_index_space());

	StaticAccess<IndexSpaceImpl> data(r);
	assert(data->num_elmts > 0);

#ifdef FULL_SIZE_INSTANCES
	num_elements = data->last_elmt + 1;
	// linearization is an identity translation
	Translation<1> inst_offset(0);
	DomainLinearization dl = DomainLinearization::from_mapping<1>(Mapping<1,1>::new_dynamic_mapping(inst_offset));
	dl.serialize(linearization_bits);
#else
	num_elements = data->last_elmt - data->first_elmt + 1;
        // round num_elements up to a multiple of 4 to line things up better with vectors, cache lines, etc.
        if(num_elements & 3) {
          if (block_size == num_elements)
            block_size = (block_size + 3) & ~(size_t)3;
          num_elements = (num_elements + 3) & ~(size_t)3;
        }
	if(block_size > num_elements)
	  block_size = num_elements;

	//printf("CI: %zd %zd %zd\n", data->num_elmts, data->first_elmt, data->last_elmt);

	Translation<1> inst_offset(-(int)(data->first_elmt));
	DomainLinearization dl = DomainLinearization::from_mapping<1>(Mapping<1,1>::new_dynamic_mapping(inst_offset));
	dl.serialize(linearization_bits);
#endif
      }

      // for instances with a single element, there's no real difference between AOS and
      //  SOA - force the block size to indicate "full SOA" as it makes the DMA code
      //  use a faster path
      if(field_sizes.size() == 1)
	block_size = num_elements;

#ifdef FORCE_SOA_INSTANCE_LAYOUT
      // the big hammer
      if(block_size != num_elements) {
        log_inst.info("block size changed from %zd to %zd (SOA)",
                      block_size, num_elements);
        block_size = num_elements;
      }
#endif

      if(block_size > 1) {
	size_t leftover = num_elements % block_size;
	if(leftover > 0)
	  num_elements += (block_size - leftover);
      }

      size_t inst_bytes = elem_size * num_elements;

      RegionInstance i = m_impl->create_instance(get_index_space(), linearization_bits, inst_bytes,
						 block_size, elem_size, field_sizes,
						 redop_id,
						 -1 /*list size*/, reqs,
						 RegionInstance::NO_INST);
      log_meta.info("instance created: region=" IDFMT " memory=" IDFMT " id=" IDFMT " bytes=%zd",
	       this->is_id, memory.id, i.id, inst_bytes);
      return i;
    }

    RegionInstance Domain::create_hdf5_instance(const char *file_name,
                                                const std::vector<size_t> &field_sizes,
                                                const std::vector<const char*> &field_files,
                                                bool read_only) const
    {
#ifndef USE_HDF
      // TODO: Implement this
      assert(false);
      return RegionInstance::NO_INST;
#else
      ProfilingRequestSet requests;

      assert(field_sizes.size() == field_files.size());
      Memory memory = Memory::NO_MEMORY;
      Machine machine = Machine::get_machine();
      std::set<Memory> mem;
      machine.get_all_memories(mem);
      for(std::set<Memory>::iterator it = mem.begin(); it != mem.end(); it++) {
        if (it->kind() == Memory::HDF_MEM) {
          memory = *it;
        }
      }
      assert(memory.kind() == Memory::HDF_MEM);
      DetailedTimer::ScopedPush sp(TIME_LOW_LEVEL);
      HDFMemory* hdf_mem = (HDFMemory*) get_runtime()->get_memory_impl(memory);
      size_t elem_size = 0;
      for(std::vector<size_t>::const_iterator it = field_sizes.begin();
	  it != field_sizes.end();
	  it++)
	elem_size += *it;
      
      size_t num_elements;
      int linearization_bits[RegionInstanceImpl::MAX_LINEARIZATION_LEN];
      assert(get_dim() > 0);
      {
        LegionRuntime::Arrays::Rect<1> inst_extent;
        switch(get_dim()) {
	case 1:
	  {
	    LegionRuntime::Arrays::FortranArrayLinearization<1> cl(get_rect<1>(), 0);
	    DomainLinearization dl = DomainLinearization::from_mapping<1>(LegionRuntime::Arrays::Mapping<1, 1>::new_dynamic_mapping(cl));
	    inst_extent = cl.image_convex(get_rect<1>());
	    dl.serialize(linearization_bits);
	    break;
	  }

	case 2:
	  {
	    LegionRuntime::Arrays::FortranArrayLinearization<2> cl(get_rect<2>(), 0);
	    DomainLinearization dl = DomainLinearization::from_mapping<2>(LegionRuntime::Arrays::Mapping<2, 1>::new_dynamic_mapping(cl));
	    inst_extent = cl.image_convex(get_rect<2>());
	    dl.serialize(linearization_bits);
	    break;
	  }

	case 3:
	  {
	    LegionRuntime::Arrays::FortranArrayLinearization<3> cl(get_rect<3>(), 0);
	    DomainLinearization dl = DomainLinearization::from_mapping<3>(LegionRuntime::Arrays::Mapping<3, 1>::new_dynamic_mapping(cl));
	    inst_extent = cl.image_convex(get_rect<3>());
	    dl.serialize(linearization_bits);
	    break;
	  }

	default: assert(0);
	}

	num_elements = inst_extent.volume();
      }

      size_t inst_bytes = elem_size * num_elements;
      RegionInstance i = hdf_mem->create_instance(get_index_space(), linearization_bits, inst_bytes, 
                                                  1/*block_size*/, elem_size, field_sizes,
                                                  0 /*redop_id*/, -1/*list_size*/, requests, RegionInstance::NO_INST,
                                                  file_name, field_files, *this, read_only);
      log_meta.info("instance created: region=" IDFMT " memory=" IDFMT " id=" IDFMT " bytes=%zd",
	       this->is_id, memory.id, i.id, inst_bytes);
      return i;
#endif
    }

  
  ////////////////////////////////////////////////////////////////////////
  //
  // class IndexSpaceAllocator
  //

    unsigned IndexSpaceAllocator::alloc(unsigned count /*= 1*/) const
    {
      DetailedTimer::ScopedPush sp(TIME_LOW_LEVEL);
      return ((IndexSpaceAllocatorImpl *)impl)->alloc_elements(count);
    }

    void IndexSpaceAllocator::reserve(unsigned ptr, unsigned count /*= 1  */) const
    {
      DetailedTimer::ScopedPush sp(TIME_LOW_LEVEL);
      return ((IndexSpaceAllocatorImpl *)impl)->reserve_elements(ptr, count);
    }

    void IndexSpaceAllocator::free(unsigned ptr, unsigned count /*= 1  */) const
    {
      DetailedTimer::ScopedPush sp(TIME_LOW_LEVEL);
      return ((IndexSpaceAllocatorImpl *)impl)->free_elements(ptr, count);
    }


  ////////////////////////////////////////////////////////////////////////
  //
  // class ElementMask
  //

    ElementMask::ElementMask(void)
      : first_element(-1), num_elements(-1), memory(Memory::NO_MEMORY), offset(-1),
	raw_data(0), first_enabled_elmt(-1), last_enabled_elmt(-1)
    {
    }

    ElementMask::ElementMask(int _num_elements, int _first_element /*= 0*/)
      : first_element(_first_element), num_elements(_num_elements), memory(Memory::NO_MEMORY), offset(-1), first_enabled_elmt(-1), last_enabled_elmt(-1)
    {
      size_t bytes_needed = ElementMaskImpl::bytes_needed(first_element, num_elements);
      raw_data = calloc(1, bytes_needed);
      //((ElementMaskImpl *)raw_data)->count = num_elements;
      //((ElementMaskImpl *)raw_data)->offset = first_element;
    }

    ElementMask::ElementMask(const ElementMask &copy_from, 
			     int _num_elements /*= -1*/, int _first_element /*= 0*/)
    {
      first_element = copy_from.first_element;
      num_elements = copy_from.num_elements;
      first_enabled_elmt = copy_from.first_enabled_elmt;
      last_enabled_elmt = copy_from.last_enabled_elmt;
      size_t bytes_needed = ElementMaskImpl::bytes_needed(first_element, num_elements);
      raw_data = calloc(1, bytes_needed);

      if(copy_from.raw_data) {
	memcpy(raw_data, copy_from.raw_data, bytes_needed);
      } else {
	get_runtime()->get_memory_impl(copy_from.memory)->get_bytes(copy_from.offset, raw_data, bytes_needed);
      }
    }

    ElementMask::~ElementMask(void)
    {
      if (raw_data) {
        free(raw_data);
        raw_data = 0;
      }
    }

    ElementMask& ElementMask::operator=(const ElementMask &rhs)
    {
      first_element = rhs.first_element;
      num_elements = rhs.num_elements;
      first_enabled_elmt = rhs.first_enabled_elmt;
      last_enabled_elmt = rhs.last_enabled_elmt;
      size_t bytes_needed = rhs.raw_size();
      if (raw_data)
        free(raw_data);
      raw_data = calloc(1, bytes_needed);
      if (rhs.raw_data)
        memcpy(raw_data, rhs.raw_data, bytes_needed);
      else
        get_runtime()->get_memory_impl(rhs.memory)->get_bytes(rhs.offset, raw_data, bytes_needed);
      return *this;
    }

    void ElementMask::init(int _first_element, int _num_elements, Memory _memory, off_t _offset)
    {
      first_element = _first_element;
      num_elements = _num_elements;
      memory = _memory;
      offset = _offset;
      size_t bytes_needed = ElementMaskImpl::bytes_needed(first_element, num_elements);
      raw_data = get_runtime()->get_memory_impl(memory)->get_direct_ptr(offset, bytes_needed);
    }

    void ElementMask::enable(int start, int count /*= 1*/)
    {
      if(raw_data != 0) {
	ElementMaskImpl *impl = (ElementMaskImpl *)raw_data;
	//printf("ENABLE %p %d %d %d " IDFMT "\n", raw_data, offset, start, count, impl->bits[0]);
	int pos = start - first_element;
        assert(pos < num_elements);
	for(int i = 0; i < count; i++) {
	  uint64_t *ptr = &(impl->bits[pos >> 6]);
	  *ptr |= (1ULL << (pos & 0x3f));
	  pos++;
	}
	//printf("ENABLED %p %d %d %d " IDFMT "\n", raw_data, offset, start, count, impl->bits[0]);
      } else {
	//printf("ENABLE(2) " IDFMT " %d %d %d\n", memory.id, offset, start, count);
	MemoryImpl *m_impl = get_runtime()->get_memory_impl(memory);

	int pos = start - first_element;
	for(int i = 0; i < count; i++) {
	  off_t ofs = offset + ((pos >> 6) << 3);
	  uint64_t val;
	  m_impl->get_bytes(ofs, &val, sizeof(val));
	  //printf("ENABLED(2) %d,  " IDFMT "\n", ofs, val);
	  val |= (1ULL << (pos & 0x3f));
	  m_impl->put_bytes(ofs, &val, sizeof(val));
	  pos++;
	}
      }

      if((first_enabled_elmt < 0) || (start < first_enabled_elmt))
	first_enabled_elmt = start;

      if((last_enabled_elmt < 0) || ((start+count-1) > last_enabled_elmt))
	last_enabled_elmt = start + count - 1;
    }

    void ElementMask::disable(int start, int count /*= 1*/)
    {
      if(raw_data != 0) {
	ElementMaskImpl *impl = (ElementMaskImpl *)raw_data;
	int pos = start - first_element;
	for(int i = 0; i < count; i++) {
	  uint64_t *ptr = &(impl->bits[pos >> 6]);
	  *ptr &= ~(1ULL << (pos & 0x3f));
	  pos++;
	}
      } else {
	//printf("DISABLE(2) " IDFMT " %d %d %d\n", memory.id, offset, start, count);
	MemoryImpl *m_impl = get_runtime()->get_memory_impl(memory);

	int pos = start - first_element;
	for(int i = 0; i < count; i++) {
	  off_t ofs = offset + ((pos >> 6) << 3);
	  uint64_t val;
	  m_impl->get_bytes(ofs, &val, sizeof(val));
	  //printf("DISABLED(2) %d,  " IDFMT "\n", ofs, val);
	  val &= ~(1ULL << (pos & 0x3f));
	  m_impl->put_bytes(ofs, &val, sizeof(val));
	  pos++;
	}
      }

      // not really right
      if(start == first_enabled_elmt) {
	//printf("pushing first: %d -> %d\n", first_enabled_elmt, first_enabled_elmt+1);
	first_enabled_elmt++;
      }
    }

    int ElementMask::find_enabled(int count /*= 1 */, int start /*= 0*/) const
    {
      if(start == 0)
	start = first_enabled_elmt;
      if(raw_data != 0) {
	ElementMaskImpl *impl = (ElementMaskImpl *)raw_data;
	//printf("FIND_ENABLED %p %d %d " IDFMT "\n", raw_data, first_element, count, impl->bits[0]);
	for(int pos = start; pos <= num_elements - count; pos++) {
	  int run = 0;
	  while(1) {
	    uint64_t bit = ((impl->bits[pos >> 6] >> (pos & 0x3f))) & 1;
	    if(bit != 1) break;
	    pos++; run++;
	    if(run >= count) return pos - run;
	  }
	}
      } else {
	MemoryImpl *m_impl = get_runtime()->get_memory_impl(memory);
	//printf("FIND_ENABLED(2) " IDFMT " %d %d %d\n", memory.id, offset, first_element, count);
	for(int pos = start; pos <= num_elements - count; pos++) {
	  int run = 0;
	  while(1) {
	    off_t ofs = offset + ((pos >> 6) << 3);
	    uint64_t val;
	    m_impl->get_bytes(ofs, &val, sizeof(val));
	    uint64_t bit = (val >> (pos & 0x3f)) & 1;
	    if(bit != 1) break;
	    pos++; run++;
	    if(run >= count) return pos - run;
	  }
	}
      }
      return -1;
    }

    int ElementMask::find_disabled(int count /*= 1 */, int start /*= 0*/) const
    {
      if((start == 0) && (first_enabled_elmt > 0))
	start = first_enabled_elmt;
      if(raw_data != 0) {
	ElementMaskImpl *impl = (ElementMaskImpl *)raw_data;
	for(int pos = start; pos <= num_elements - count; pos++) {
	  int run = 0;
	  while(1) {
	    uint64_t bit = ((impl->bits[pos >> 6] >> (pos & 0x3f))) & 1;
	    if(bit != 0) break;
	    pos++; run++;
	    if(run >= count) return pos - run;
	  }
	}
      } else {
	assert(0);
      }
      return -1;
    }

    size_t ElementMask::raw_size(void) const
    {
      return ElementMaskImpl::bytes_needed(offset, num_elements);
    }

    const void *ElementMask::get_raw(void) const
    {
      return raw_data;
    }

    void ElementMask::set_raw(const void *data)
    {
      assert(0);
    }

    bool ElementMask::is_set(int ptr) const
    {
      if(raw_data != 0) {
	ElementMaskImpl *impl = (ElementMaskImpl *)raw_data;
	
	int pos = ptr;// - first_element;
	uint64_t val = (impl->bits[pos >> 6]);
        uint64_t bit = ((val) >> (pos & 0x3f));
        return ((bit & 1) != 0);
      } else {
        assert(0);
	MemoryImpl *m_impl = get_runtime()->get_memory_impl(memory);

	int pos = ptr - first_element;
	off_t ofs = offset + ((pos >> 6) << 3);
	uint64_t val;
	m_impl->get_bytes(ofs, &val, sizeof(val));
        uint64_t bit = ((val) >> (pos & 0x3f));
        return ((bit & 1) != 0);
      }
    }

    size_t ElementMask::pop_count(bool enabled) const
    {
      size_t count = 0;
      if (raw_data != 0) {
        ElementMaskImpl *impl = (ElementMaskImpl *)raw_data;
        int max_full = (num_elements >> 6);
        bool remainder = (num_elements % 64) != 0;
        for (int index = 0; index < max_full; index++)
          count += __builtin_popcountll(impl->bits[index]);
        if (remainder)
          count += __builtin_popcountll(impl->bits[max_full]);
        if (!enabled)
          count = num_elements - count;
      } else {
        // TODO: implement this
        assert(0);
      }
      return count;
    }

    bool ElementMask::operator!(void) const
    {
      if (raw_data != 0) {
        ElementMaskImpl *impl = (ElementMaskImpl *)raw_data;
        const int max_full = ((num_elements+63) >> 6);
        for (int index = 0; index < max_full; index++) {
          if (impl->bits[index])
            return false;
        }
      } else {
        // TODO: implement this
        assert(0);
      }
      return true;
    }

    bool ElementMask::operator==(const ElementMask &other) const
    {
      if (num_elements != other.num_elements)
        return false;
      if (raw_data != 0) {
        ElementMaskImpl *impl = (ElementMaskImpl *)raw_data;
        if (other.raw_data != 0) {
          ElementMaskImpl *other_impl = (ElementMaskImpl *)other.raw_data;
          const int max_full = ((num_elements+63) >> 6);
          for (int index = 0; index < max_full; index++)
          {
            if (impl->bits[index] != other_impl->bits[index])
              return false;
          }
        } else {
          // TODO: Implement this
          assert(false);
        }
      } else {
        // TODO: Implement this
        assert(false);
      }
      return true;
    }

    bool ElementMask::operator!=(const ElementMask &other) const
    {
      return !((*this) == other);
    }

    ElementMask ElementMask::operator|(const ElementMask &other) const
    {
      ElementMask result(num_elements);
      ElementMaskImpl *target = (ElementMaskImpl *)result.raw_data;
      if (raw_data != 0) {
        ElementMaskImpl *impl = (ElementMaskImpl *)raw_data;
        if (other.raw_data != 0) {
          ElementMaskImpl *other_impl = (ElementMaskImpl *)other.raw_data;
          assert(num_elements == other.num_elements);
          const int max_full = ((num_elements+63) >> 6);
          for (int index = 0; index < max_full; index++) {
            target->bits[index] = impl->bits[index] | other_impl->bits[index]; 
          }
        } else {
          // TODO implement this
          assert(0);
        }
      } else {
        // TODO: implement this
        assert(0);
      }
      return result;
    }

    ElementMask ElementMask::operator&(const ElementMask &other) const
    {
      ElementMask result(num_elements);
      ElementMaskImpl *target = (ElementMaskImpl *)result.raw_data;
      if (raw_data != 0) {
        ElementMaskImpl *impl = (ElementMaskImpl *)raw_data;
        if (other.raw_data != 0) {
          ElementMaskImpl *other_impl = (ElementMaskImpl *)other.raw_data;
          assert(num_elements == other.num_elements);
          const int max_full = ((num_elements+63) >> 6);
          for (int index = 0; index < max_full; index++) {
            target->bits[index] = impl->bits[index] & other_impl->bits[index];
          }
        } else {
          // TODO: implement this
          assert(0);
        }
      } else {
        // TODO: implement this
        assert(0);
      }
      return result;
    }

    ElementMask ElementMask::operator-(const ElementMask &other) const
    {
      ElementMask result(num_elements);
      ElementMaskImpl *target = (ElementMaskImpl *)result.raw_data;
      if (raw_data != 0) {
        ElementMaskImpl *impl = (ElementMaskImpl *)raw_data;
        if (other.raw_data != 0) {
          ElementMaskImpl *other_impl = (ElementMaskImpl *)other.raw_data;
          assert(num_elements == other.num_elements);
          const int max_full = ((num_elements+63) >> 6);
          for (int index = 0; index < max_full; index++) {
            target->bits[index] = impl->bits[index] & ~(other_impl->bits[index]);
          }
        } else {
          // TODO: implement this
          assert(0);
        }
      } else {
        // TODO: implement this
        assert(0);
      }
      return result;
    }

    ElementMask& ElementMask::operator|=(const ElementMask &other)
    {
      if (raw_data != 0) {
        ElementMaskImpl *impl = (ElementMaskImpl *)raw_data;
        if (other.raw_data != 0) {
          ElementMaskImpl *other_impl = (ElementMaskImpl *)other.raw_data;
          assert(num_elements == other.num_elements);
          const int max_full = ((num_elements+63) >> 6);
          for (int index = 0; index < max_full; index++) {
            impl->bits[index] |= other_impl->bits[index];
          }
        } else {
          // TODO: implement this
          assert(0);
        }
      } else {
        // TODO: implement this
        assert(0);
      }
      return *this;
    }

    ElementMask& ElementMask::operator&=(const ElementMask &other)
    {
      if (raw_data != 0) {
        ElementMaskImpl *impl = (ElementMaskImpl *)raw_data;
        if (other.raw_data != 0) {
          ElementMaskImpl *other_impl = (ElementMaskImpl *)other.raw_data;
          assert(num_elements == other.num_elements);
          const int max_full = ((num_elements+63) >> 6);
          for (int index = 0; index < max_full; index++) {
            impl->bits[index] &= other_impl->bits[index];
          }
        } else {
          // TODO: implement this
          assert(0);
        }
      } else {
        // TODO: implement this
        assert(0);
      }
      return *this;
    }

    ElementMask& ElementMask::operator-=(const ElementMask &other)
    {
      if (raw_data != 0) {
        ElementMaskImpl *impl = (ElementMaskImpl *)raw_data;
        if (other.raw_data != 0) {
          ElementMaskImpl *other_impl = (ElementMaskImpl *)other.raw_data;
          assert(num_elements == other.num_elements);
          const int max_full = ((num_elements+63) >> 6);
          for (int index = 0; index < max_full; index++) {
            impl->bits[index] &= ~(other_impl->bits[index]);
          }
        } else {
          // TODO: implement this
          assert(0);
        }
      } else {
        // TODO: implement this
        assert(0);
      }
      return *this;
    }

    ElementMask::OverlapResult ElementMask::overlaps_with(const ElementMask& other,
							  off_t max_effort /*= -1*/) const
    {
      if (raw_data != 0) {
        ElementMaskImpl *i1 = (ElementMaskImpl *)raw_data;
        if (other.raw_data != 0) {
          ElementMaskImpl *i2 = (ElementMaskImpl *)(other.raw_data);
          assert(num_elements == other.num_elements);
          for(int i = 0; i < (num_elements + 63) >> 6; i++)
            if((i1->bits[i] & i2->bits[i]) != 0)
              return ElementMask::OVERLAP_YES;
          return ElementMask::OVERLAP_NO;
        } else {
          return ElementMask::OVERLAP_MAYBE;
        }
      } else {
        return ElementMask::OVERLAP_MAYBE;
      }
    }

    ElementMask::Enumerator *ElementMask::enumerate_enabled(int start /*= 0*/) const
    {
      return new ElementMask::Enumerator(*this, start, 1);
    }

    ElementMask::Enumerator *ElementMask::enumerate_disabled(int start /*= 0*/) const
    {
      return new ElementMask::Enumerator(*this, start, 0);
    }

    ElementMask::Enumerator::Enumerator(const ElementMask& _mask, int _start, int _polarity)
      : mask(_mask), pos(_start), polarity(_polarity) {}

    ElementMask::Enumerator::~Enumerator(void) {}

    bool ElementMask::Enumerator::get_next(int &position, int &length)
    {
      if(mask.raw_data != 0) {
	ElementMaskImpl *impl = (ElementMaskImpl *)(mask.raw_data);

	// are we already off the end?
	if(pos >= mask.num_elements)
	  return false;

        // if our current pos is below the first known-set element, skip to there
        if((mask.first_enabled_elmt > 0) && (pos < mask.first_enabled_elmt))
          pos = mask.first_enabled_elmt;

	// fetch first value and see if we have any bits set
	int idx = pos >> 6;
	uint64_t bits = impl->bits[idx];
	if(!polarity) bits = ~bits;

	// for the first one, we may have bits to ignore at the start
	if(pos & 0x3f)
	  bits &= ~((1ULL << (pos & 0x3f)) - 1);

	// skip over words that are all zeros, and try to ignore trailing zeros completely
        int stop_at = mask.num_elements;
        if(mask.last_enabled_elmt >= 0)
          stop_at = mask.last_enabled_elmt+1;
	while(!bits) {
	  idx++;
	  if((idx << 6) >= stop_at) {
	    pos = mask.num_elements; // so we don't scan again
	    return false;
	  }
	  bits = impl->bits[idx];
	  if(!polarity) bits = ~bits;
	}

	// if we get here, we've got at least one good bit
	int extra = __builtin_ctzll(bits);
	assert(extra < 64);
	position = (idx << 6) + extra;
	
	// now we're going to turn it around and scan ones
	if(extra)
	  bits |= ((1ULL << extra) - 1);
	bits = ~bits;

	while(!bits) {
	  idx++;
	  // did our 1's take us right to the end?
	  if((idx << 6) >= mask.num_elements) {
	    pos = mask.num_elements; // so we don't scan again
	    length = mask.num_elements - position;
	    return true;
	  }
	  bits = ~impl->bits[idx]; // note the inversion
	  if(!polarity) bits = ~bits;
	}

	// if we get here, we got to the end of the 1's
	int extra2 = __builtin_ctzll(bits);
	pos = (idx << 6) + extra2;
	if(pos >= mask.num_elements)
	  pos = mask.num_elements;
	length = pos - position;
	return true;
      } else {
	assert(0);
	MemoryImpl *m_impl = get_runtime()->get_memory_impl(mask.memory);

	// scan until we find a bit set with the right polarity
	while(pos < mask.num_elements) {
	  off_t ofs = mask.offset + ((pos >> 5) << 2);
	  unsigned val;
	  m_impl->get_bytes(ofs, &val, sizeof(val));
	  int bit = ((val >> (pos & 0x1f))) & 1;
	  if(bit != polarity) {
	    pos++;
	    continue;
	  }

	  // ok, found one bit with the right polarity - now see how many
	  //  we have in a row
	  position = pos++;
	  while(pos < mask.num_elements) {
	    off_t ofs = mask.offset + ((pos >> 5) << 2);
	    unsigned val;
	    m_impl->get_bytes(ofs, &val, sizeof(val));
	    int bit = ((val >> (pos & 0x1f))) & 1;
	    if(bit != polarity) break;
	    pos++;
	  }
	  // we get here either because we found the end of the run or we 
	  //  hit the end of the mask
	  length = pos - position;
	  return true;
	}

	// if we fall off the end, there's no more ranges to enumerate
	return false;
      }
    }

    bool ElementMask::Enumerator::peek_next(int &position, int &length)
    {
      int old_pos = pos;
      bool ret = get_next(position, length);
      pos = old_pos;
      return ret;
    }


  ////////////////////////////////////////////////////////////////////////
  //
  // class IndexSpaceImpl
  //

    IndexSpaceImpl::IndexSpaceImpl(void)
    {
      init(IndexSpace::NO_SPACE, -1);
    }

    void IndexSpaceImpl::init(IndexSpace _me, unsigned _init_owner)
    {
      assert(!_me.exists() || (_init_owner == ID(_me).node()));

      me = _me;
      locked_data.valid = false;
      lock.init(ID(me).convert<Reservation>(), ID(me).node());
      lock.in_use = true;
      lock.set_local_data(&locked_data);
      valid_mask = 0;
      valid_mask_complete = false;
      valid_mask_event = Event::NO_EVENT;
      valid_mask_event_impl = 0;
    }

    void IndexSpaceImpl::init(IndexSpace _me, IndexSpace _parent,
				size_t _num_elmts,
				const ElementMask *_initial_valid_mask /*= 0*/, bool _frozen /*= false*/)
    {
      me = _me;
      locked_data.valid = true;
      locked_data.parent = _parent;
      locked_data.frozen = _frozen;
      locked_data.num_elmts = _num_elmts;
      locked_data.valid_mask_owners = (1ULL << gasnet_mynode());
      locked_data.avail_mask_owner = gasnet_mynode();
      valid_mask = (_initial_valid_mask?
		    new ElementMask(*_initial_valid_mask) :
		    new ElementMask(_num_elmts));
      valid_mask_complete = true;
      valid_mask_event = Event::NO_EVENT;
      valid_mask_event_impl = 0;
      if(_frozen) {
	avail_mask = 0;
	locked_data.first_elmt = valid_mask->first_enabled();
	locked_data.last_elmt = valid_mask->last_enabled();
	log_region.info("subregion " IDFMT " (of " IDFMT ") restricted to [%zd,%zd]",
			me.id, _parent.id, locked_data.first_elmt,
			locked_data.last_elmt);
      } else {
	avail_mask = new ElementMask(_num_elmts);
	if(_parent == IndexSpace::NO_SPACE) {
	  avail_mask->enable(0, _num_elmts);
	  locked_data.first_elmt = 0;
	  locked_data.last_elmt = _num_elmts - 1;
	} else {
	  StaticAccess<IndexSpaceImpl> pdata(get_runtime()->get_index_space_impl(_parent));
	  locked_data.first_elmt = pdata->first_elmt;
	  locked_data.last_elmt = pdata->last_elmt;
	}
      }
      lock.init(ID(me).convert<Reservation>(), ID(me).node());
      lock.in_use = true;
      lock.set_local_data(&locked_data);
    }

    IndexSpaceImpl::~IndexSpaceImpl(void)
    {
      delete valid_mask;
    }

    bool IndexSpaceImpl::is_parent_of(IndexSpace other)
    {
      while(other != IndexSpace::NO_SPACE) {
	if(other == me) return true;
	IndexSpaceImpl *other_impl = get_runtime()->get_index_space_impl(other);
	other = StaticAccess<IndexSpaceImpl>(other_impl)->parent;
      }
      return false;
    }

    Event IndexSpaceImpl::request_valid_mask(void)
    {
      size_t num_elmts = StaticAccess<IndexSpaceImpl>(this)->num_elmts;
      int valid_mask_owner = -1;
      
      Event e;
      {
	AutoHSLLock a(valid_mask_mutex);
	
	if(valid_mask != 0) {
	  // if the mask exists, we've already requested it, so just provide
	  //  the event that we have
          return valid_mask_event;
	}
	
	valid_mask = new ElementMask(num_elmts);
	valid_mask_owner = ID(me).node(); // a good guess?
	valid_mask_count = (valid_mask->raw_size() + 2047) >> 11;
	valid_mask_complete = false;
	valid_mask_event_impl = GenEventImpl::create_genevent();
        valid_mask_event = valid_mask_event_impl->current_event();
        e = valid_mask_event;
      }

      ValidMaskRequestMessage::send_request(valid_mask_owner, me);

      return e;
    }
  
  ////////////////////////////////////////////////////////////////////////
  //
  // class IndexSpaceAllocatorImpl
  //

    IndexSpaceAllocatorImpl::IndexSpaceAllocatorImpl(IndexSpaceImpl *_is_impl)
      : is_impl(_is_impl)
    {
    }

    IndexSpaceAllocatorImpl::~IndexSpaceAllocatorImpl(void)
    {
    }

    unsigned IndexSpaceAllocatorImpl::alloc_elements(unsigned count /*= 1 */)
    {
      SharedAccess<IndexSpaceImpl> is_data(is_impl);
      assert((is_data->valid_mask_owners >> gasnet_mynode()) & 1);
      int start = is_impl->valid_mask->find_disabled(count);
      assert(start >= 0);

      reserve_elements(start, count);

      return start;
    }

    void IndexSpaceAllocatorImpl::reserve_elements(unsigned ptr, unsigned count /*= 1 */)
    {
      // for now, do updates of valid masks immediately
      IndexSpaceImpl *impl = is_impl;
      while(1) {
	SharedAccess<IndexSpaceImpl> is_data(impl);
	assert((is_data->valid_mask_owners >> gasnet_mynode()) & 1);
	is_impl->valid_mask->enable(ptr, count);
	IndexSpace is = is_data->parent;
	if(is == IndexSpace::NO_SPACE) break;
	impl = get_runtime()->get_index_space_impl(is);
      }
    }

    void IndexSpaceAllocatorImpl::free_elements(unsigned ptr, unsigned count /*= 1*/)
    {
      // for now, do updates of valid masks immediately
      IndexSpaceImpl *impl = is_impl;
      while(1) {
	SharedAccess<IndexSpaceImpl> is_data(impl);
	assert((is_data->valid_mask_owners >> gasnet_mynode()) & 1);
	is_impl->valid_mask->disable(ptr, count);
	IndexSpace is = is_data->parent;
	if(is == IndexSpace::NO_SPACE) break;
	impl = get_runtime()->get_index_space_impl(is);
      }
    }

  
  ////////////////////////////////////////////////////////////////////////
  //
  // class ValidMaskRequestMessage
  //

  /*static*/ void ValidMaskRequestMessage::handle_request(RequestArgs args)
  {
    DetailedTimer::ScopedPush sp(TIME_LOW_LEVEL);
    IndexSpaceImpl *r_impl = get_runtime()->get_index_space_impl(args.is);

    assert(r_impl->valid_mask);
    const char *mask_data = (const char *)(r_impl->valid_mask->get_raw());
    assert(mask_data);

    size_t mask_len = r_impl->valid_mask->raw_size();

    // send data in 2KB blocks
    unsigned block_id = 0;
    while(mask_len >= (1 << 11)) {
      ValidMaskDataMessage::send_request(args.sender, args.is, block_id,
					 mask_data,
					 1 << 11,
					 PAYLOAD_KEEP);
      mask_data += 1 << 11;
      mask_len -= 1 << 11;
      block_id++;
    }
    if(mask_len) {
      ValidMaskDataMessage::send_request(args.sender, args.is, block_id,
					 mask_data,
					 mask_len,
					 PAYLOAD_KEEP);
    }
  }

  /*static*/ void ValidMaskRequestMessage::send_request(gasnet_node_t target,
							IndexSpace is)
  {
    RequestArgs args;

    args.sender = gasnet_mynode();
    args.is = is;
    Message::request(target, args);
  }
  

  ////////////////////////////////////////////////////////////////////////
  //
  // class IndexSpaceAllocatorImpl
  //

  /*static*/ void ValidMaskDataMessage::handle_request(RequestArgs args,
						       const void *data,
						       size_t datalen)
  {
    DetailedTimer::ScopedPush sp(TIME_LOW_LEVEL);
    IndexSpaceImpl *r_impl = get_runtime()->get_index_space_impl(args.is);

    assert(r_impl->valid_mask);
    // removing const on purpose here...
    char *mask_data = (char *)(r_impl->valid_mask->get_raw());
    assert(mask_data);
    assert((args.block_id << 11) < r_impl->valid_mask->raw_size());

    memcpy(mask_data + (args.block_id << 11), data, datalen);

    GenEventImpl *to_trigger = 0;
    {
      AutoHSLLock a(r_impl->valid_mask_mutex);
      //printf("got piece of valid mask data for region " IDFMT " (%d expected)\n",
      //       args.region.id, r_impl->valid_mask_count);
      r_impl->valid_mask_count--;
      if(r_impl->valid_mask_count == 0) {
	r_impl->valid_mask_complete = true;
	to_trigger = r_impl->valid_mask_event_impl;
	r_impl->valid_mask_event_impl = 0;
      }
    }

    if(to_trigger) {
      //printf("triggering " IDFMT "/%d\n",
      //       r_impl->valid_mask_event.id, r_impl->valid_mask_event.gen);
      to_trigger->trigger_current();
    }
  }

  /*static*/ void ValidMaskDataMessage::send_request(gasnet_node_t target,
						     IndexSpace is, unsigned block_id,
						     const void *data,
						     size_t datalen,
						     int payload_mode)
  {
    RequestArgs args;

    args.is = is;
    args.block_id = block_id;
    Message::request(target, args, data, datalen, payload_mode);
  }
  
}; // namespace Realm