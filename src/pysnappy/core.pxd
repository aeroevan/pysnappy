cpdef bytes uncompress(bytes)
cpdef bytes compress(bytes)

cdef bytes _compress_buf(const char* src, size_t n)
cdef bytes _uncompress_buf(const char* src, size_t n)
cdef Py_ssize_t _compress_append(const char* src, size_t n, bytearray dst) except -1
cdef Py_ssize_t _uncompress_append(const char* src, size_t n, bytearray dst) except -1
cdef size_t _raw_compress(const char* src, size_t n, char* dst, size_t cap) except? 0
cdef void _raw_uncompress(const char* src, size_t n, char* dst, size_t cap) except *
cdef size_t _max_compressed_len(size_t n) nogil
