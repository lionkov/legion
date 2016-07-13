#ifndef PAYLOAD_H
#define PAYLOAD_H

#include <iostream>
#include <cstdio>
#include <cstring>
#include <sys/uio.h>
#include <cstddef>
#include <stdint.h>
#include <vector>
#include "activemsg.h"

enum {
  FAB_PAYLOAD_ERROR,
  FAB_PAYLOAD_NONE, // no payload in packet
  FAB_PAYLOAD_KEEP, // use payload pointer, guaranteed to be stable
  FAB_PAYLOAD_FREE, // take ownership of payload, free when done
  FAB_PAYLOAD_COPY, // make a copy of the payload
};

class FabPayload {
 protected:
  int	mode;

  // Internal buffer used if mode if FAB_PAYLOAD_COPY
  void*	        buf;
  size_t	bufsz;
	
  // Determine mode, copy data into buf if necessary
  int checkmode();
  // Transfer data into the target iovect
  ssize_t checkiovec(struct iovec* iov, size_t iovnum);
  // Copy data from buf into dest only if mode is FAB_PAYLOAD_COPY
  // Should this be here?
  ssize_t checkcopy(void *dest, size_t destsz);
	
 public:
  FabPayload(int m) : mode(m), buf(NULL), bufsz(0) {}
  virtual ~FabPayload(void);

  virtual ssize_t size(void) = 0;
  virtual void* ptr(void) = 0;

  // Copy 
  virtual ssize_t copy(void *dest, size_t destsz) = 0;
  virtual ssize_t iovec(struct iovec *iov, size_t iovnum) = 0;
};

struct FabContiguousPayload : public FabPayload {
 protected:
  size_t sz;
  void* data;

 public:
  FabContiguousPayload(int mode, void *data, size_t s);
  virtual ~FabContiguousPayload(void);
  
  virtual ssize_t size(void);
  virtual void* ptr(void);
  virtual ssize_t copy(void *dest, size_t destsz);
  virtual ssize_t iovec(struct iovec *iov, size_t iovnum);
};

struct FabTwoDPayload : public FabPayload {
 protected:
  size_t		linesz;
  size_t		linecnt;
  ptrdiff_t	stride;
  void*		data;

 public:
  FabTwoDPayload(int m, void *data, size_t line_size, size_t line_count, ptrdiff_t line_stride);
  virtual ~FabTwoDPayload(void);

  virtual ssize_t size(void);
  virtual void* ptr(void);
  virtual ssize_t copy(void *dest, size_t destsz);
  virtual ssize_t iovec(struct iovec *iov, size_t iovnum);
};

typedef std::pair<void *, size_t> FabSpanListEntry;
typedef std::vector<FabSpanListEntry> FabSpanList;

struct FabSpanPayload : public FabPayload {
 protected:
  SpanList	spans;
  size_t		sz;
 public:
  FabSpanPayload(int m, SpanList &sl, size_t s);
  virtual ~FabSpanPayload(void);

  virtual ssize_t size(void);
  virtual void* ptr(void);
  virtual ssize_t copy(void *dest, size_t destsz);
  virtual ssize_t iovec(struct iovec *iov, size_t iovnum);
};

#endif // PAYLOAD_H
