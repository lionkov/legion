#include "address_exchange.h"


// Initialize PMI. Return 0 on success, an error code.
size_t PMIAddressExchange::init_pmi() {
  int spawned, initialized;

  if (PMI_SUCCESS != PMI_Initialized(&initialized)) {
    return 1;
  }

  if (!initialized) {
    if (PMI_SUCCESS != PMI_Init(&initialized)) {
      return 2;
    }
  }

  if (PMI_SUCCESS != PMI_KVS_Get_name_length_max(&max_name_len)) {
    return 3;
  }
  
  kvs_name = (char*) malloc(max_name_len);
  if (NULL == kvs_name) return 4;

  if (PMI_SUCCESS != PMI_KVS_Get_key_length_max(&max_key_len)) {
    return 5;
  }
  
  kvs_key = (char*) malloc(max_key_len);
  if (NULL == kvs_key) return 6;

  if (PMI_SUCCESS != PMI_KVS_Get_value_length_max(&max_val_len)) {
    return 7;
  }
  
  kvs_value = (char*) malloc(max_val_len);
  if (NULL == kvs_value) return 8;

  if (PMI_SUCCESS != PMI_KVS_Get_my_name(kvs_name, max_name_len)) {
    return 7;
  }

  if (PMI_SUCCESS != PMI_Get_rank(&rank)) {
    return 9;
  }

  if (PMI_SUCCESS != PMI_Get_size(&size)) {
    return 10;
  }

  return 0;
}

// Add this node's address to the KVS, commit the change, and wait
// for all other nodes to commit as well.
size_t PMIAddressExchange::pmi_put_address(const char* addr, size_t addrlen) const {

  assert((addrlen <= max_name_len) && "Addresses are too small to be exchanged by PMI");
  snprintf(kvs_key, max_key_len, "fabric-%lu-addr", (unsigned long) rank);
  if(PMI_KVS_Put(kvs_name, kvs_key, addr) != PMI_SUCCESS) {
    return 1;
  }

  // Commit and sync
  if (PMI_KVS_Commit(kvs_name) != PMI_SUCCESS)
    return 2;

  if (PMI_Barrier() != PMI_SUCCESS)
    return 3;
  
  return 0;
}

size_t PMIAddressExchange::pmi_get_addresses(char* addrs, size_t addrlen) const {

  // Just iterate through all other nodes -- not scalable? This could be done lazily,
  // but I'd prefer not to
  char* p = addrs;
  char lookup[256];
  for (unsigned long i=0; i<size; ++i) {
    snprintf(lookup, 256, "fabric-%lu-addr", i);  
    if (PMI_KVS_Get(kvs_name, lookup, p, addrlen) != PMI_SUCCESS) {
      return 1;
    }
    p += addrlen;
  }
  
  return 0;
}


// Exchange addresses. Return 0 on success, else an error code.
// Error codes are defined in address_exchange.h
size_t PMIAddressExchange::exchange(NodeId& id,
				    uint32_t& num_nodes,
				    const char* addr,
				    char* addrs,
				    size_t addrlen) {
  int ret;
  ret = init_pmi();
  if (ret != 0) {
    std::cerr << "pmi_init failed with code: " << ret << std::endl;
    return EXCHANGE_INIT_FAILED;
  }

  // Publish this node's address
  ret = pmi_put_address(addr, addrlen);
  if (ret != 0) {
    std::cerr << "pmi_put_address failed with code: " << ret << std::endl;
    return EXCHANGE_PUT_FAILED;
  }

  // Look up everyone else's addresses
 
  ret = pmi_get_addresses(addrs, addrlen);
  if (ret != 0) {
    std::cerr << "pmi_get_addresses failed with code: " << ret << std::endl;
    return EXCHANGE_GET_FAILED;
  }
  
  // Set rank information for the root
  id = rank;
  num_nodes = size;
  
  std::cout << "ADDR: " << addr << std::endl;
  char* p;
  for (int i = 0; i < size; ++i) {
    std::cout << "ADDRS " << i << ": " << p << std::endl;
    p += addrlen;
  }
  
  std::cout << "PMI Information: \n"
	    << " rank: " << rank << "\n"
	    << " size: " << size << "\n"
	    << " kvs_name: " << kvs_name << "\n"
	    << " kvs_value: " << kvs_value << "\n"
	    << " max_name_len: " << max_name_len << "\n"
	    << " max_val_len: " << max_val_len << "\n"
	    << " max_key_len: " << max_key_len << std::endl;
  
  return EXCHANGE_SUCCESS;
}

