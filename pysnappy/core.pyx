from cpython.mem cimport PyMem_Malloc, PyMem_Realloc, PyMem_Free
cimport pysnappy_c

def uncompress(bytes compressed):
    cdef size_t n = len(compressed)
    cdef size_t m
    cdef pysnappy_c.snappy_status status
    cdef char* uncompressed
    status = pysnappy_c.snappy_uncompressed_length(
        compressed, n, &m)
    if status != 0:
        raise Exception("Could not determine uncompressed length")
    uncompressed = <char*>PyMem_Malloc(m * sizeof(char*))
    if not uncompressed:
        raise MemoryError("Could not allocate uncompressed buffer")
    status = pysnappy_c.snappy_uncompress(compressed, n, uncompressed, &m)
    if status != 0:
        PyMem_Free(uncompressed)
        raise Exception("Could not uncompress")
    return uncompressed[:m]

def compress(bytes uncompressed):
    cdef size_t n = len(uncompressed)
    cdef size_t m = pysnappy_c.snappy_max_compressed_length(n)
    cdef pysnappy_c.snappy_status status
    cdef char* compressed
    compressed = <char*>PyMem_Malloc(m * sizeof(char*))
    if not compressed:
        raise MemoryError("Could not allocate compressed buffer")
    status = pysnappy_c.snappy_compress(uncompressed, n, compressed, &m)
    if status != 0:
        PyMem_Free(compressed)
        raise Exception("Could not compress")
    return compressed[:m]
