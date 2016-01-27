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
    print(m)
    uncompressed = <char*>malloc(m * sizeof(char*))
    if uncompressed == NULL:
        raise MemoryError("Could not allocate uncompressed buffer")
    status = pysnappy_c.snappy_uncompress(compressed, n, uncompressed, &m)
    if status != 0:
        free(uncompressed)
        raise Exception("Could not uncompress")
    return uncompressed
