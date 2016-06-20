#include "fabric.h"

int Payload::checkmode()
{
	int ret;

	ret = 0;
	switch (mode) {
	default:
		// just refuse to do anything, return an error later
		mode = PAYLOAD_ERROR;
		data = NULL;
		sz = 0;
		ret = -1;
		break;

	case PAYLOAD_NONE:
	case PAYLOAD_KEEP:
	case PAYLOAD_FREE:
		break;

	case PAYLOAD_COPY:
		bufsz = size();
		buf = malloc(bufsz);

		if (copy(buf, bufsz) != bufsz) {
			mode = PAYLOAD_ERROR;
			ret = -1;
		}
		break;
	}
}

ssize_t Payload::checkiovec(struct iovec *iov, size_t iovnum)
{
	if (mode != PAYLOAD_COPY)
		return -1

	if (iovnum >= 1) {
		iov->iov_base = buf;
		iov->iov_len = bufsz;
	}

	return 1;
}

// if mode == PAYLOAD_COPY, copy the data, otherwise return -1
ssize_t Payload::checkcopy(void *dest, size_t destsz)
{
	size_t n;
	if (mode != PAYLOAD_COPY)
		return -1;

	n = destsz < bufsz ? destsz : bufsz;
	memmove(dest, buf, n);

	return n;
}

Payload::~Payload(void)
{
	free(buf);
	buf = NULL;
}
	
ContiguousPayload::ContiguousPayload(int m, void *d, size_t s):Payload(m),data(d),sz(s)
{
	checkmode();
}

ContiguousPayload::~ContiguousPayload(void)
{
	if (mode == PAYLOAD_FREE) {
		free(data);
		data = NULL;
	}
}

ssize_t ContiguousPayload::size(void)
{
	if (mode == PAYLOAD_ERROR)
		return -1;

	return sz;
}

void *ContiguousPayload::ptr(void)
{
	if (mode == PAYLOAD_ERROR)
		return NULL;

	return data;
}

ssize_t ContiguousPayload::copy(void *dptr, size_t dsz)
{
	size_t ret;

	if (mode == PAYLOAD_ERROR)
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

ssize_t ContiguousPayload::iovec(struct iovec *iov, size_t iovnum)
{
	ssize_t ret;

	if (mode == PAYLOAD_ERROR)
		return -1;

	// check if PAYLOAD_COPY
	ret = checkiovec(iov, iovnum);
	if (ret >= 0)
		return ret;
		
	if (iovnum >= 1) {
		iov[0].iov_base = data;
		iov[0].iov_len = sz;
	}

	return 1;
}


TwoDPayload::TwoDPayload(int m, void *d, size_t line_size, size_t line_count, ptrdiff_t line_stride):data(d), linesz(line_size), linecnt(line_count), stride(line_stride)
{
	checkmode();
}

TwoDPayload::~TwoDPayload(void)
{
	if (mode == PAYLOAD_FREE)
		free(data);
}

ssize_t TwoDPayload::size(void)
{
	return (linecnt/stride + (linecnt%stride?1:0)) * linesz;
}

void* TwoDPayload::ptr(void)
{
	return NULL;
}

ssize_t TwoDPayload::copy(void *dest, size_t destsz)
{
	ssize_t ret;
	char *p, *ep;

	if (mode == PAYLOAD_ERROR)
		return -1;

	ret = checkcopy(dest, destsz);
	if (ret >= 0)
		return ret;

	ret = 0;
	p = (char *) data;
	ep = p + linecnt * linesz;
	for(; (ret < destsz) && (p < data + (linecnt * linesz)); p += stride * linesz) {
		size_t sz = linesz;
		if (ret + sz > destsz)
			sz = destsz - ret;

		memmove(dest + ret, p, sz);
		ret += sz;
	}

	return ret;
}

ssize_t TwoDPayload::iovec(struct iovec *iov, size_t iovnum)
{
	ssize_t ret;

	if (mode == PAYLOAD_ERROR)
		return -1;

	ret = checkiovec(iov, iovnum);
	if (ret >= 0)
		return ret;

	ret = linecnt/stride + (linecnt%stride?1:0);
	if (iovnum < ret)
		return ret;

	for(i = 0; i < ret; i++) {
		iov[i].iov_base = (char *)data + i*stride*linesz;
		iov[i].iov_len = linesz;
	}

	return ret;
}

SpanPayload::SpanPayload(int m, SpanList &sl, size_t s):spans(sl), sz(s)
{
	checkmode();
	sz = 0;
	for(SpanList::const_iterator it = spans.begin(); it != spans.end(); it++) {
		sz += it->second;
	}
}

SpanPayload::~SpanPayload(void)
{
	if (mode == PAYLOAD_FREE) {
		for(SpanList::const_iterator it = spans.begin(); it != spans.end(); it++) {
			free(it->first);
		}
	}
}

ssize_t SpanPayload::size(void)
{
	return sz;
}

void* SpanPayload::ptr(void)
{
	return NULL;
}

ssize_t SpanPayload::copy(void *dest, size_t destsz)
{
	ssize_t ret;
	char *p, *ep;

	if (mode == PAYLOAD_ERROR)
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

ssize_t SpanPayload::iovec(struct iovec *iov, size_t iovnum)
{
	ssize_t n;

	if (mode == PAYLOAD_ERROR)
		return -1;

	ret = checkiovec(iov, iovnum);
	if (ret >= 0)
		return ret;

	n = spans.size();
	if (iovnum < n)
		return n;

	for(SpanList::const_iterator it = spans.begin(), ssize_t i = 0; it != spans.end(); it++, i++) {
		iov[i].iov_base = it->first;
		iov[i].iov_len = it->second;
	}

	return n
}
