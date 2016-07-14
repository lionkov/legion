#include "payload.h"

FabContiguousPayload::FabContiguousPayload(int m, void *d, size_t s)
  : FabPayload(m), sz(s), data((void*) d), internal_buffer(NULL) {
  checkmode();
}

FabContiguousPayload::~FabContiguousPayload(void) {
  std::cout << "DESTRUCTING PAYLOAD -- data: " << (char*) data << std::endl;
  if (mode == FAB_PAYLOAD_FREE && data) 
    free(data);
  
  if (mode == FAB_PAYLOAD_COPY && internal_buffer)
    free(internal_buffer);
}

// If the payload mode is FAB_PAYLOAD_COPY, copies the data pointer
// into the internal buffer and updates data pointer to point to it.
// Otherwise, does nothing.

// Returns 0 on success, -1 on failure (in which case this payload object
// is invalid.)
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
// this payload's data.
//
// Returns -1 on error.
ssize_t FabContiguousPayload::get_iovs_required() {
  if (mode == FAB_PAYLOAD_ERROR)
    return -1;
  if (mode == FAB_PAYLOAD_NONE)
    return 0;
  return 1;  
}

// FabTwoDPayload::FabTwoDPayload(int m, void *d, size_t line_size, size_t line_count, ptrdiff_t line_stride)
//   : FabPayload(m), linesz(line_size), linecnt(line_count), stride(line_stride), data((void*) d)
// {
//   checkmode();
// }

// FabTwoDPayload::~FabTwoDPayload(void)
// {
//   if (mode == FAB_PAYLOAD_FREE)
//     free(data);
// }

// ssize_t FabTwoDPayload::size(void)
// {
//   return (linecnt/stride + (linecnt%stride?1:0)) * linesz;
// }

// void* FabTwoDPayload::ptr(void)
// {
//   return NULL;
// }

// ssize_t FabTwoDPayload::copy(void *dest, size_t destsz)
// {
//   ssize_t ret;
//   uintptr_t p, ep, d;

//   if (mode == FAB_PAYLOAD_ERROR)
//     return -1;

//   ret = checkcopy(dest, destsz);
//   if (ret >= 0)
//     return ret;

//   ret = 0;
//   p = (uintptr_t) data;
//   d = (uintptr_t) data;
	
//   ep = p + linecnt * linesz;
//   for(; (ret < destsz) && (p < d + (linecnt * linesz)); p += stride * linesz) {
//     size_t sz = linesz;
//     if (ret + sz > destsz)
//       sz = destsz - ret;

//     memmove((void*) (d + ret), (void*) p, sz);
//     ret += sz;
//   }

//   return ret;
// }

// ssize_t FabTwoDPayload::iovec(struct iovec *iov, size_t iovnum)
// {
//   ssize_t ret;

//   if (mode == FAB_PAYLOAD_ERROR)
//     return -1;

//   ret = checkiovec(iov, iovnum);
//   if (ret >= 0)
//     return ret;

//   ret = linecnt/stride + (linecnt%stride?1:0);
//   if (iovnum < ret)
//     return ret;

//   for(ssize_t i = 0; i < ret; i++) {
//     iov[i].iov_base = (char *)data + i*stride*linesz;
//     iov[i].iov_len = linesz;
//   }

//   return ret;
// }

// FabSpanPayload::FabSpanPayload(int m, SpanList &sl, size_t s) : FabPayload(m), spans(sl), sz(s)
// {
//   checkmode();
//   sz = 0;
	
//   for(SpanList::const_iterator it = spans.begin(); it != spans.end(); it++) {
//     sz += it->second;
//   }

// }

// FabSpanPayload::~FabSpanPayload(void)
// {  
//   if (mode == FAB_PAYLOAD_FREE) {
//     for(SpanList::const_iterator it = spans.begin(); it != spans.end(); it++) {
//       free((void*) it->first);
//     }
//   } 
// }

// ssize_t FabSpanPayload::size(void)
// {
//   return sz;
// }

// void* FabSpanPayload::ptr(void)
// {
//   return NULL;
// }

// ssize_t FabSpanPayload::copy(void *dest, size_t destsz)
// {
//   ssize_t ret, n;
//   char *p;

//   if (mode == FAB_PAYLOAD_ERROR)
//     return -1;

//   ret = checkcopy(dest, destsz);
//   if (ret >= 0)
//     return ret;

//   ret = 0;
//   p = (char *) dest;
//   for(SpanList::const_iterator it = spans.begin(); (destsz > 0) && (it != spans.end()); it++) {
//     n = destsz > it->second ? it->second : destsz;
//     memmove(p, it->first, n);
//     ret += n;
//     p += n;
//     destsz -= n;
//   }

//   return ret;
// }

// ssize_t FabSpanPayload::iovec(struct iovec *iov, size_t iovnum)
// {
//   ssize_t n;
//   int ret;

//   if (mode == FAB_PAYLOAD_ERROR)
//     return -1;

//   ret = checkiovec(iov, iovnum);
//   if (ret >= 0)
//     return ret;

//   n = spans.size();
//   if (iovnum < n)
//     return n;

//   SpanList::const_iterator it;
//   ssize_t i;
//   for(it = spans.begin(), i = 0; it != spans.end(); it++, i++) {
//     iov[i].iov_base = (void*) it->first;
//     iov[i].iov_len = it->second;
//   }

//   return n;
// }

