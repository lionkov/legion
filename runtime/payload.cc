#include "payload.h"

FabContiguousPayload::FabContiguousPayload(int m, void *d, size_t s)
  : FabPayload(m), sz(s), data((void*) d), internal_buffer(NULL) {
  checkmode();
}

FabContiguousPayload::~FabContiguousPayload(void) {
  
  std::cout << "DESTRUCTING PAYLOAD -- data: " << (char*) data
	    << " mode: " << payload_descs[mode].desc << std::endl;
  
  if (mode == FAB_PAYLOAD_FREE && data) 
    free(data);
  
  if (mode == FAB_PAYLOAD_COPY && internal_buffer)
    free(internal_buffer);
  
}

// If the payload mode is FAB_PAYLOAD_COPY, copies the data pointer
// into the internal buffer and updates data pointer to point to it.
// Otherwise, does nothing.

// Returns 0 on success, -1 on failure (in which case this payload object
// is invalid.)./f
int FabContiguousPayload::checkmode() {
  switch (mode) {
  default:
    // just refuse to do anything, return an error later
    mode = FAB_PAYLOAD_ERROR;
    return -1;
    
  case FAB_PAYLOAD_ERROR:
    return -1;
    
  case FAB_PAYLOAD_NONE:
    return 0;
    
  case FAB_PAYLOAD_KEEP:
    return 0;
    
  case FAB_PAYLOAD_FREE:
    return 0;

  case FAB_PAYLOAD_COPY:
    internal_buffer = malloc(sz);
    
    if (!internal_buffer) {
      mode = FAB_PAYLOAD_ERROR;
      return -1;
    }

    if (copy(internal_buffer, sz) != sz) {
      mode = FAB_PAYLOAD_ERROR;
      return -1;
    }
    
    data = internal_buffer; // Update data pointer to point to internal data
    return 0;
  }
}
// Copy data to dptr. Returns number of bytes copied, or -1 if the
// copy fails.
ssize_t FabContiguousPayload::copy(void *dptr, size_t dsz) {
  int ret;

  if (mode == FAB_PAYLOAD_ERROR)
    return -1;
  ret = dsz;
  if (ret > sz)
    ret = sz;

  memcpy(dptr, data, ret);
  return ret;
}

// Assign iovecs at iov to point to this payload's data.
// Since this paylad is contiguous, all data will be assigned
// to iov[0]. Returns -1 on failure, otherwise returns the number of
// iovs assigned. If not enough iovs were available, will assign as many
// as possible.
ssize_t FabContiguousPayload::iovec(struct iovec *iov, size_t iovnum) {
  if (mode == FAB_PAYLOAD_ERROR)
    return -1;
  if (mode == FAB_PAYLOAD_NONE)
    return 0;
  if (iovnum >= 1) {
    iov[0].iov_base = data;
    iov[0].iov_len = sz;
    return 1;
  }

  // Error, there were not enough iovs to assign to.
  return 0;
}

// Returns the number of iov entries required to transfer
// this payload's data. Returns -1 on error.
ssize_t FabContiguousPayload::get_iovs_required() {
  if (mode == FAB_PAYLOAD_ERROR)
    return -1;
  if (mode == FAB_PAYLOAD_NONE)
    return 0;
  return 1;  
}


FabTwoDPayload::FabTwoDPayload(int m, void *d, size_t line_size, size_t line_count, ptrdiff_t line_stride)
  : FabPayload(m), linesz(line_size), linecnt(line_count), stride(line_stride), data((void*) d) {
  checkmode();
}

FabTwoDPayload::~FabTwoDPayload(void) {
  std::cout << "DESTRUCTING TWOD PAYLOAD -- data: " << (char*) data << std::endl;
  if (mode == FAB_PAYLOAD_FREE && data) 
    free(data);
  
  if (mode == FAB_PAYLOAD_COPY && internal_buffer)
    free(internal_buffer);
}


// Check mode of payload and copy data into this object if necessary.
// Only strided data will be copied, but extra space will be allocated
// to accomodate empty space in between strides.
int FabTwoDPayload::checkmode() {
  switch (mode) {
  default:
    // just refuse to do anything, return an error later
    mode = FAB_PAYLOAD_ERROR;
    return -1;
    
  case FAB_PAYLOAD_ERROR:
    return -1;
    
  case FAB_PAYLOAD_NONE:
    return 0;
    
  case FAB_PAYLOAD_KEEP:
    return 0;
    
  case FAB_PAYLOAD_FREE:
    return 0;

  case FAB_PAYLOAD_COPY:
    int sz = linesz*linecnt;
    internal_buffer = malloc(sz);
    
    if (!internal_buffer) {
      mode = FAB_PAYLOAD_ERROR;
      return -1;
    }

    if (copy(internal_buffer, sz) != linecnt*linesz) {
      mode = FAB_PAYLOAD_ERROR;
      return -1;
    }
    
    data = internal_buffer; // Update data pointer to point to internal data
    return 0;
  }
}

// Copy data to buffer at dest. The stride of the payload will
// be maintained, i.e., the buffer must contain stride lines
// which will be skipped over.
// Returns the number of bytes copied (NOT including strides)
// or -1 on error.
ssize_t FabTwoDPayload::copy_strided(void *dest, size_t destsz) {
  int ret = 0;
  int remaining = destsz;
  char* p = (char*) data;
  char* dest_p = (char*) dest;
  
  
  if (mode == FAB_PAYLOAD_ERROR)
    return -1;

  for (int i=0; i<linecnt; ++i) {
    
    if(remaining < linesz) {  // out of space to copy into
      memcpy(dest_p, p, remaining);
      ret += remaining;
      return ret;
    }
    
    memcpy(dest_p, p, linesz);
    ret += linesz;
    remaining -= linesz;
    p += linesz*stride;
    dest_p += linesz*stride;
  }
  return ret;
}

// Copies payload data to the buffer at dest, ignoring all
// stride lines. Data in the resulting buffer will be
// fully contiguous.

// Returns the number of bytes copied, or -1 on error.
ssize_t FabTwoDPayload::copy(void *dest, size_t destsz) {
  int ret = 0;
  size_t remaining = destsz;
  char* p = (char*) data;
  char* dest_p = (char*) dest;
  
  if (mode == FAB_PAYLOAD_ERROR)
    return -1;

  for (int i=0; i<linecnt; ++i) {
    
    if(remaining < linesz) {  // out of space to copy into
      memcpy(dest_p, p, remaining);
      ret += remaining;
      return ret;
    }
    
    memcpy(dest_p, p, linesz);
    ret += linesz;
    remaining -= linesz;
    p += linesz*stride;
    dest_p += linesz;
  }
  return ret;
}




ssize_t FabTwoDPayload::get_iovs_required() {
  return linecnt; 
}


// Loads data into iovecs at iov. Will put one iov in each line.z
// Will NOT load skipped lines. Returns the number of iov entries assigned,
// or -1 on error.
ssize_t FabTwoDPayload::iovec(struct iovec *iov, size_t iovnum) {
  ssize_t ret;

  if (mode == FAB_PAYLOAD_ERROR)
    return -1;
  
  ret = (iovnum < linecnt) ? iovnum : linecnt;
  
  for(ssize_t i = 0; i < ret; ++i) {
    iov[i].iov_base = (char *)data + i*stride*linesz;
    iov[i].iov_len = linesz;
  }

  return ret;
}

FabSpanPayload::FabSpanPayload(int m, SpanList &sl)
  : FabPayload(m), sz(0), data(&sl) {
  checkmode(); 
}

FabSpanPayload::~FabSpanPayload(void) {
  std::cout << "DESTRUCTING PAYLOAD -- spanlist " << std::endl;
  
  if (mode == FAB_PAYLOAD_FREE || mode == FAB_PAYLOAD_COPY) {
    for(SpanList::const_iterator it = data->begin(); it != data->end(); it++) {
      if (it->first)
	free((void*) it->first);
    }
    delete data;
  }
  
}


// Determine the mode of this payload object, and load
// data accordingly.
int FabSpanPayload::checkmode() {
  switch (mode) {
  default:
    // just refuse to do anything, return an error later
    mode = FAB_PAYLOAD_ERROR;
    return -1;
    
  case FAB_PAYLOAD_ERROR:
    return -1;
    
  case FAB_PAYLOAD_NONE:
    return 0;
    
  case FAB_PAYLOAD_KEEP:
    return 0;
    
  case FAB_PAYLOAD_FREE:
    return 0;

  case FAB_PAYLOAD_COPY:
    copy_spans(data);
    data = internal_spans;
    return 0;
  }
}

// Assign each span to point to the contents of the span list.
// Returns the number of spans assigned on success, or -1
// on error. Error will set the mode of this payload to FAB_PAYLOAD_ERROR.
/*
ssize_t FabSpanPayload::assign_spans(SpanList* sl) {
  ssize_t ret = 0;
  sz = 0;
  if (mode == FAB_PAYLOAD_ERROR)
    return -1;
  
  for(SpanList::const_iterator i = sl->begin(); i != sl->end(); ++i) {
    FabSpanListEntry entry((void*) i->first, i->second);
    spans.push_back(entry);
    ++ret;
    sz += i->second; 
  }
  
  return ret;
}
*/

// Copies the data pointed to into the span payload.
// Returns the number of spans assigned on succes, or -1
// on error. Error will set the mode of this payload to FAB_PAYLOAD_ERROR.
ssize_t FabSpanPayload::copy_spans(SpanList* sl) {
  ssize_t ret = 0;
  sz = 0;
  internal_spans = new SpanList();
  
  if (mode == FAB_PAYLOAD_ERROR)
    return -1;
  
  for(SpanList::const_iterator i = sl->begin(); i != sl->end(); ++i) {
    void* buf = malloc(i->second);
    if (!buf) {
      mode = FAB_PAYLOAD_ERROR;      
      return -1;
    }
    memmove(buf, i->first, i->second);
 
    FabSpanListEntry entry(buf, i->second);
    internal_spans->push_back(entry);
    ++ret;
  }
  
  return ret;
}




size_t FabSpanPayload::size(void) {
  return sz;
}

void* FabSpanPayload::ptr(void) {
  return data;
}


// Copies data to a contiguous external buffer.
// Returns the number of bytes copied, or -1 on error.
// If not enough space is available, will copy as much as possible.
ssize_t FabSpanPayload::copy(void *dest, size_t destsz) {
  ssize_t ret = 0;
  size_t n;
  char* dest_p = (char*) dest;

  if (mode == FAB_PAYLOAD_ERROR)
    return -1;
 
  for(SpanList::const_iterator it = data->begin(); (destsz > 0) && (it != data->end()); it++) {
    n = (destsz > it->second) ? it->second : destsz;
    memmove(dest_p, it->first, n);
    ret += n;
    dest_p += n;
    destsz -= n;
  }

  return ret;
}

// Fills an array of iovecs at iov to poin to this payload object's
// data. Returns the number of iovecs assigned, or -1 if on error.
ssize_t FabSpanPayload::iovec(struct iovec *iov, size_t iovnum) {
  ssize_t i = 0;

  if (mode == FAB_PAYLOAD_ERROR)
    return -1;

  SpanList::const_iterator it;
  for(it = data->begin(); it != data->end(); ++it) {
    if (i >= iovnum)
      return i;
    
    iov[i].iov_base = (void*) it->first;
    iov[i].iov_len = it->second;
    ++i;
  }

  return i;
}

// Gets the number of iovs required to completely assign this Payload's
// data.
ssize_t FabSpanPayload::get_iovs_required() {
  return data->size();
}
