from libc.stdlib cimport malloc, free
cimport pysnappy_c

def uncompress(bytes compressed):
    cdef size_t n = len(compressed)
    cdef size_t m
    cdef pysnappy_c.snappy_status status
    cdef char* uncompressed = NULL
    status = pysnappy_c.snappy_uncompressed_length(
        compressed, n, &m)
    if status != 0:
        raise Exception("Could not determine uncompressed length")
    uncompressed = <char*>malloc(m * sizeof(char*))
    if uncompressed == NULL:
        raise MemoryError("Could not allocate uncompressed buffer")
    status = pysnappy_c.snappy_uncompress(compressed, n, uncompressed, &m)
    if status != 0:
        free(uncompressed)
        raise Exception("Could not uncompress")
    return uncompressed

def compress(bytes uncompressed):
    cdef size_t n = len(uncompressed)
    cdef size_t m = pysnappy_c.snappy_max_compressed_length(n)
    cdef pysnappy_c.snappy_status status
    cdef char* compressed = NULL
    compressed = <char*>malloc(m * sizeof(char*))
    if compressed == NULL:
        raise MemoryError("Could not allocate compressed buffer")
    status = pysnappy_c.snappy_compress(uncompressed, n, compressed, &m)
    if status != 0:
        free(compressed)
        raise Exception("Could not compress")
    return compressed[:m]
