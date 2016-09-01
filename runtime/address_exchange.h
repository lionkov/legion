// Utility class for exchanging node address information when the Fabric is started.

// A Fabric may pass in a buffer and its own address information, the exchanger
// will then gather the contents of this buffer to all nodes without needing
// to be aware of address format information.

// By default, it will try to use PMI 1.0

#ifndef ADDRESS_EXCHANGE_H
#define ADDRESS_EXCHANGE_H

// Currently uses PMI 1.0
#define PMI_VERSION 1
#define PMI_SUBVERSION 1


#include <stdint.h>
#include <cstring>
#include <iostream>
#include <cassert>
#include "pmi.h"
#include "fabric_types.h"

typedef enum {
  PMI_UNINITIALIZED = 0,
  SINGLETON_INIT_BUT_NO_PM = 1,
  NORMAL_INIT_WITH_PM,
  SINGLETON_INIT_WITH_PM
} PMIState;

typedef enum {
  EXCHANGE_SUCCESS = 0,
  EXCHANGE_INIT_FAILED = 1,
  EXCHANGE_PUT_FAILED,
  EXCHANGE_GET_FAILED,
} ExchangeError;

class PMIAddressExchange {
public:
  PMIAddressExchange()
    : rank(0),
      size(1),
      kvs_name(nullptr),
      kvs_key(nullptr),
      kvs_value(nullptr),
      max_name_len(0),
      max_key_len(0),
      max_val_len(0) { }

  // Exchange addresses. Will write node's ID into id, number
  // of nodes in to num_nodes, and addresses into addrs.
  size_t exchange(NodeId& id, uint32_t& num_nodes, const char* addr, char* addrs, size_t addrlen);
  
 
private:
  size_t init_pmi();
  size_t pmi_put_address(const char* addr, size_t addrlen) const;
  size_t pmi_get_addresses(char* addrs, size_t addrlen) const; 
  int rank; // rank of this process
  int size; // process group size
  char* kvs_name;
  char* kvs_key;
  char* kvs_value;
  int max_name_len;
  int max_key_len;
  int max_val_len;
};

#endif
