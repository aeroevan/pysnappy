from libc.stdint cimport uint32_t
cpdef uint32_t masked_crc32c(bytes)
cdef uint32_t masked_crc32c_buf(const char* data, size_t n) nogil
