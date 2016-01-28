from libc.stdlib cimport malloc, free
from cpython.mem cimport PyMem_Malloc, PyMem_Realloc, PyMem_Free
cimport pysnappy_c
import struct as pystruct

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
        free(compressed)
        raise Exception("Could not compress")
    return compressed[:m]

cdef class HadoopStreamDecompressor:
    cdef bytes _buf
    cdef int _block_size
    cdef int _block_read
    cdef int _subblock_size

    def __init__(self):
        self._buf = b""
        self._block_size = -1
        self._block_read = 0
        self._subblock_size = -1

    cpdef bytes decompress(self, bytes data):
        self._buf += data
        cdef bytes output = b""
        while True:
            pass

    cpdef bytes _decompress_block(self):
        cdef bytes buf
        if self._block_size < 0:
            if len(self._buf) <= 4:
                return b""
            self._block_size = pystruct.unpack(
                ">i", self._buf[:4])[0]
            self._buf = self._buf[4:]
        cdef bytes output = b""
        while self._block_read < self._block_size:
            buf = self._decompress_subblock()
            if len(buf) > 0:
                output += buf
            else:
                # Buffer doesn't contain full subblock.
                break
        if self._block_read == self._block_size:
            # Finished reading this block, so reinitialize
            self._block_read = 0
            self._block_size = -1
        return output

    cpdef bytes _decompress_subblock(self):
        cdef bytes compressed
        cdef bytes uncompressed
        if self._subblock_size < 0:
            if len(self._buf) <= 4:
                return b""
            self._subblock_size = pystruct.unpack(
                ">i", self._buf[:4])
            self._buf = self._buf[4:]
        # Only attempt to decompress complete subblocks.
        if len(self._buf) < self._subblock_size:
            return b""
        compressed = self._buf[:self._subblock_size]
        self._buf = self._buf[self._subblock_size:]
        uncompressed = uncompress(compressed)
        self._block_read += len(uncompress)
        self._subblock_size = -1
        return uncompressed
