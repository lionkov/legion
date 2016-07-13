#include "payload.h"

int FabPayload::checkmode()
{
  int ret;

  ret = 0;
  switch (mode) {
  default:
    // just refuse to do anything, return an error later
    mode = FAB_PAYLOAD_ERROR;
    ret = -1;
    break;

  case FAB_PAYLOAD_NONE:
  case FAB_PAYLOAD_KEEP:
  case FAB_PAYLOAD_FREE:
    break;

  case FAB_PAYLOAD_COPY:
    bufsz = size();
    buf = malloc(bufsz);
    
    if (!buf) {
      ret = -1;
      break;
    }

    if (copy(buf, bufsz) != bufsz) {
      mode = FAB_PAYLOAD_ERROR;
      ret = -1;
    }
    
    break;
  }
  return ret;
}

ssize_t FabPayload::checkiovec(struct iovec *iov, size_t iovnum)
{
  if (mode != FAB_PAYLOAD_COPY)
    return -1;

  if (iovnum >= 1) {
    iov[0].iov_base = buf;
    iov[0].iov_len = bufsz;
  }

  return 1;
}

// if mode == FAB_PAYLOAD_COPY, copy the data, otherwise return -1
ssize_t FabPayload::checkcopy(void *dest, size_t destsz)
{
  size_t n;
  if (mode != FAB_PAYLOAD_COPY)
    return -1;

  n = destsz < bufsz ? destsz : bufsz;
  memmove(dest, buf, n);

  return n;
}

FabPayload::~FabPayload(void)
{
  free(buf);
  buf = NULL;
}
	
FabContiguousPayload::FabContiguousPayload(int m, void *d, size_t s)
  : FabPayload(m), sz(s), data((void*) d)
{
  checkmode();
}

FabContiguousPayload::~FabContiguousPayload(void)
{
  if (mode == FAB_PAYLOAD_FREE) {
    free(data);
    data = NULL;
  }
}

ssize_t FabContiguousPayload::size(void)
{
  if (mode == FAB_PAYLOAD_ERROR)
    return -1;

  return sz;
}

void *FabContiguousPayload::ptr(void)
{
  if (mode == FAB_PAYLOAD_ERROR)
    return NULL;

  return data;
}

ssize_t FabContiguousPayload::copy(void *dptr, size_t dsz)
{
  int ret;

  if (mode == FAB_PAYLOAD_ERROR)
    return -1;

  ret = checkcopy(dptr, dsz);
  if (ret >= 0)
    return ret;

  ret = dsz;
  if (ret > sz)
    ret = sz;

  memcpy(dptr, data, ret);
  return ret;
}

ssize_t FabContiguousPayload::iovec(struct iovec *iov, size_t iovnum)
{
  ssize_t ret;

  if (mode == FAB_PAYLOAD_ERROR)
    return -1;

  // check if FAB_PAYLOAD_COPY
  // If FAB_PAYLOAD_COPY, this call will assign the iovs to point
  // to buf and return 1.
  ret = checkiovec(iov, iovnum);
  if (ret >= 0)
    return ret;

  // Otherwise, we don't have our own copy of the data --
  // so assign iovs to point to data.
  if (iovnum >= 1) {
    iov[0].iov_base = data;
    iov[0].iov_len = sz;
  }

  return 1;
}


FabTwoDPayload::FabTwoDPayload(int m, void *d, size_t line_size, size_t line_count, ptrdiff_t line_stride)
  : FabPayload(m), linesz(line_size), linecnt(line_count), stride(line_stride), data((void*) d)
{
  checkmode();
}

FabTwoDPayload::~FabTwoDPayload(void)
{
  if (mode == FAB_PAYLOAD_FREE)
    free(data);
}

ssize_t FabTwoDPayload::size(void)
{
  return (linecnt/stride + (linecnt%stride?1:0)) * linesz;
}

void* FabTwoDPayload::ptr(void)
{
  return NULL;
}

ssize_t FabTwoDPayload::copy(void *dest, size_t destsz)
{
  ssize_t ret;
  uintptr_t p, ep, d;

  if (mode == FAB_PAYLOAD_ERROR)
    return -1;

  ret = checkcopy(dest, destsz);
  if (ret >= 0)
    return ret;

  ret = 0;
  p = (uintptr_t) data;
  d = (uintptr_t) data;
	
  ep = p + linecnt * linesz;
  for(; (ret < destsz) && (p < d + (linecnt * linesz)); p += stride * linesz) {
    size_t sz = linesz;
    if (ret + sz > destsz)
      sz = destsz - ret;

    memmove((void*) (d + ret), (void*) p, sz);
    ret += sz;
  }

  return ret;
}

ssize_t FabTwoDPayload::iovec(struct iovec *iov, size_t iovnum)
{
  ssize_t ret;

  if (mode == FAB_PAYLOAD_ERROR)
    return -1;

  ret = checkiovec(iov, iovnum);
  if (ret >= 0)
    return ret;

  ret = linecnt/stride + (linecnt%stride?1:0);
  if (iovnum < ret)
    return ret;

  for(ssize_t i = 0; i < ret; i++) {
    iov[i].iov_base = (char *)data + i*stride*linesz;
    iov[i].iov_len = linesz;
  }

  return ret;
}

FabSpanPayload::FabSpanPayload(int m, SpanList &sl, size_t s) : FabPayload(m), spans(sl), sz(s)
{
  checkmode();
  sz = 0;
	
  for(SpanList::const_iterator it = spans.begin(); it != spans.end(); it++) {
    sz += it->second;
  }

}

FabSpanPayload::~FabSpanPayload(void)
{  
  if (mode == FAB_PAYLOAD_FREE) {
    for(SpanList::const_iterator it = spans.begin(); it != spans.end(); it++) {
      free((void*) it->first);
    }
  } 
}

ssize_t FabSpanPayload::size(void)
{
  return sz;
}

void* FabSpanPayload::ptr(void)
{
  return NULL;
}

ssize_t FabSpanPayload::copy(void *dest, size_t destsz)
{
  ssize_t ret, n;
  char *p;

  if (mode == FAB_PAYLOAD_ERROR)
    return -1;

  ret = checkcopy(dest, destsz);
  if (ret >= 0)
    return ret;

  ret = 0;
  p = (char *) dest;
  for(SpanList::const_iterator it = spans.begin(); (destsz > 0) && (it != spans.end()); it++) {
    n = destsz > it->second ? it->second : destsz;
    memmove(p, it->first, n);
    ret += n;
    p += n;
    destsz -= n;
  }

  return ret;
}

ssize_t FabSpanPayload::iovec(struct iovec *iov, size_t iovnum)
{
  ssize_t n;
  int ret;

  if (mode == FAB_PAYLOAD_ERROR)
    return -1;

  ret = checkiovec(iov, iovnum);
  if (ret >= 0)
    return ret;

  n = spans.size();
  if (iovnum < n)
    return n;

  SpanList::const_iterator it;
  ssize_t i;
  for(it = spans.begin(), i = 0; it != spans.end(); it++, i++) {
    iov[i].iov_base = (void*) it->first;
    iov[i].iov_len = it->second;
  }

  return n;
}

