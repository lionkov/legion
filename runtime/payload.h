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
  int  mode;
  
  // Determine mode, copy data into internal buffer if necessary
  virtual int checkmode() = 0;
	
 public:
  FabPayload(int m) : mode(m) { }
  virtual ~FabPayload(void) { }

  // Return payload size 
  virtual ssize_t size(void) = 0;
  
  // Return direct pointer to payload
  virtual void* ptr(void) = 0;

  // Copy data into dest
  virtual ssize_t copy(void *dest, size_t destsz) = 0;

  // Assigns iovec iov to point to this payload's data
  virtual ssize_t iovec(struct iovec *iov, size_t iovnum) = 0;
};

class FabContiguousPayload : public FabPayload {
 protected:
  size_t sz; // size of data
  void* data; // pointer to data -- may point to external or internal buffer
  void* internal_buffer; // internal buffer used if mode is PAYLOAD_COPY
  virtual int checkmode();
  

 public:
  FabContiguousPayload(int mode, void *data, size_t s);
  virtual ~FabContiguousPayload(void);
  
  virtual ssize_t size(void) { return sz; };
  virtual void* ptr(void) { return (mode == FAB_PAYLOAD_ERROR) ? NULL : data; } 
  virtual ssize_t copy(void *dest, size_t destsz);
  virtual ssize_t iovec(struct iovec *iov, size_t iovnum);
};

/* class FabTwoDPayload : public FabPayload { */
/*  protected: */
/*   size_t		linesz; */
/*   size_t		linecnt; */
/*   ptrdiff_t	stride; */
/*   void*		data; */

/*  public: */
/*   FabTwoDPayload(int m, void *data, size_t line_size, size_t line_count, ptrdiff_t line_stride); */
/*   virtual ~FabTwoDPayload(void); */

/*   virtual ssize_t size(void); */
/*   virtual void* ptr(void); */
/*   virtual ssize_t copy(void *dest, size_t destsz); */
/*   virtual ssize_t iovec(struct iovec *iov, size_t iovnum); */
/* }; */

/* typedef std::pair<void *, size_t> FabSpanListEntry; */
/* typedef std::vector<FabSpanListEntry> FabSpanList; */

/* class FabSpanPayload : public FabPayload { */
/*  protected: */
/*   SpanList	spans; */
/*   size_t		sz; */
/*  public: */
/*   FabSpanPayload(int m, SpanList &sl, size_t s); */
/*   virtual ~FabSpanPayload(void); */

/*   virtual ssize_t size(void); */
/*   virtual void* ptr(void); */
/*   virtual ssize_t copy(void *dest, size_t destsz); */
/*   virtual ssize_t iovec(struct iovec *iov, size_t iovnum); */
/* }; */

#endif // PAYLOAD_H
