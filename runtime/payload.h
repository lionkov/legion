#ifndef PAYLOAD_H
#define PAYLOAD_H

#include <iostream>
#include <cstdio>
#include <cstring>
#include <sys/uio.h>
#include <cstddef>
#include <stdint.h>
#include <vector>

enum {
  FAB_PAYLOAD_NONE, // no payload in packet
  FAB_PAYLOAD_KEEP, // use payload pointer, guaranteed to be stable
  FAB_PAYLOAD_FREE, // take ownership of payload, free when done
  FAB_PAYLOAD_COPY, // make a copy of the payload
  FAB_PAYLOAD_SRCPTR, // payload has been copied to the src data pool
  FAB_PAYLOAD_PENDING, // payload needs to be copied, but hasn't yet
  FAB_PAYLOAD_KEEPREG, // use payload pointer, AND it's registered!
  FAB_PAYLOAD_EMPTY, // message can have payload, but this one is 0 bytes
  FAB_PAYLOAD_ERROR // something went wrong, discard this payload
};

typedef std::pair<const void *, size_t> SpanListEntry;
typedef std::vector<SpanListEntry> SpanList;

// Payloads hold data which may be in one of several formats.

// Payload objects hold data either internally, or by holding a pointer to they
// original data source. If FAB_PAYLOAD_COPY is used, data will be copied
// into internal buffers and freed when the payload object is destroyed. Otherwise,
// the payload object will hold a pointer to the original data. If FAB_PAYLOAD_FREE
// is used, the original data object will be deleted when the payload object is destroyed.

// Data in a payload may be accessed directly via the ptr() method, however, this is not
// preferred unless the format of the data is known. Instead, it is usually preferable to
// use the iovec() method, which will assign an array of iovecs to point to this payload's
// data, or the copy() method, which will copy the data out as a contiguous buffer.


class FabPayload {
 protected:
  int  mode;
  
  // Determine mode, copy data into internal buffer if necessary
  virtual int checkmode() = 0;
	
 public:
  FabPayload(int m) : mode(m) { }
  virtual ~FabPayload(void) { }

  // Return payload size 
  virtual size_t size(void) = 0;
  
  // Return direct pointer to payload
  virtual void* ptr(void) = 0;

  // Copy data into dest
  virtual ssize_t copy(void *dest, size_t destsz) = 0;

  // Return the number of iovs required to transfer data out
  virtual ssize_t get_iovs_required() = 0;

  // Assigns iovecs at iov to point to this payload's data.
  // Returns the numer of iovecs assigned, or -1 on failure.
  virtual ssize_t iovec(struct iovec *iov, size_t iovnum) = 0;

  int get_mode() { return mode; }
};

// A contiguous payload is just a congituous buffer with some length, sz.
class FabContiguousPayload : public FabPayload {
 protected:
  size_t sz; // size of data
  void* data; // pointer to data -- may point to external or internal buffer
  void* internal_buffer; // internal buffer used if mode is PAYLOAD_COPY
  virtual int checkmode();
  

 public:
  FabContiguousPayload(int mode, void *data, size_t s);
  virtual ~FabContiguousPayload(void);
  
  virtual size_t size(void) { return sz; };
  virtual void* ptr(void) { return (mode == FAB_PAYLOAD_ERROR) ? NULL : data; } 
  virtual ssize_t copy(void *dest, size_t destsz);
  virtual ssize_t get_iovs_required();
  virtual ssize_t iovec(struct iovec *iov, size_t iovnum);
};


// A TwoDPayload is intended to hold a two-dimensional, strided array.
// A FabTwoDPayload is defined as follows: a set of linecnt lines, in contiguous
// memory, each of length linesz. Each line is separated by stride lines, each of
// length linesz, that are not guaranteed to hold valid data.

// The 'size' of a FabTwoDPayload is the ammount of memory held in the valid lines,
// not including the stride lines. However, if FAB_PAYLOAD_COPY is used, more memory
// will be allocated than necessary to accommodate the stride lines.

// The ptr() method will return a pointer to the first line in the 2d array. The lines will
// be separated by some stride, so use the get_linesz and get_stride methods to traverse the
// array.
class FabTwoDPayload : public FabPayload {
 protected:
  size_t	linesz;
  size_t	linecnt;
  ptrdiff_t	stride;
  void*		data;
  void*         internal_buffer;

 public:
  FabTwoDPayload(int m, void *data, size_t line_size, size_t line_count, ptrdiff_t line_stride);
  virtual ~FabTwoDPayload(void);

  virtual int checkmode();
  virtual size_t size(void) { return linecnt*linesz; };
  virtual void* ptr(void) { return data; };
  virtual ssize_t copy(void *dest, size_t destsz);
  virtual ssize_t get_iovs_required();
  virtual ssize_t iovec(struct iovec *iov, size_t iovnum);
  virtual size_t get_linesz() { return linesz; }
  virtual size_t get_linecnt() { return linecnt; }
  virtual size_t get_stride() { return stride; }
  virtual ssize_t copy_strided(void* dest, size_t destsz);
};

typedef std::pair<void *, size_t> FabSpanListEntry;
typedef std::vector<FabSpanListEntry> FabSpanList;


// A SpanPayload is a list of of pointers to buffers of inconsistent length.
// If FAB_PAYLOAD_COPY is used, a new buffer will be allocated for each span
// and the data copied; otherwise, the spans will point directly  to the original
// data.

// The size of a SpanPayload is the total number of bytes contains in all spans.
class FabSpanPayload : public FabPayload {
 protected:
  SpanList*	internal_spans;
  size_t	sz;
  SpanList*     data; // Original spanlist
  virtual int checkmode();
  //virtual ssize_t assign_spans(SpanList* sl);
  virtual ssize_t copy_spans(SpanList* sl);
  virtual ssize_t get_iovs_required(); 
  
 public:
  FabSpanPayload(int m, SpanList &sl);
  virtual ~FabSpanPayload(void);

  virtual size_t size(void);
  virtual void* ptr(void);
  virtual ssize_t copy(void *dest, size_t destsz);
  virtual ssize_t iovec(struct iovec *iov, size_t iovnum);
};

#endif // PAYLOAD_H
