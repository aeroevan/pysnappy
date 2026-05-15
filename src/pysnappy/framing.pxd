from cpython cimport bool
cdef class RawDecompressor:
    cpdef bytes decompress(self, bytes)
    cpdef flush(self)

cdef class RawCompressor:
    cpdef bytes compress(self, bytes)
    cpdef flush(self)

cdef class HadoopDecompressor:
    cdef bytearray _buf
    cdef Py_ssize_t _buf_pos
    cdef int _block_size
    cdef int _block_read
    cdef int _subblock_size

    cpdef bytes decompress(self, bytes)

    cdef bint _decompress_block(self, bytearray output) except -1

    cdef bint _decompress_subblock(self, bytearray output) except -1

    cpdef flush(self)


cdef class HadoopCompressor:
    cdef int _buffer_size
    cdef bint _single_subblock
    cdef bytearray _buf

    cpdef bytes add_chunk(self, bytes)

    cpdef bytes compress(self, bytes)

    cpdef flush(self)

cdef int _CHUNK_MAX = 65536
cdef int _STREAM_TO_STREAM_BLOCK_SIZE = _CHUNK_MAX
cdef bytes _STREAM_IDENTIFIER = b"sNaPpY"
cdef int _COMPRESSED_CHUNK = 0x00
cdef int _UNCOMPRESSED_CHUNK = 0x01
cdef int _IDENTIFIER_CHUNK = 0xff
cdef int _RESERVED_UNSKIPPABLE_LEFT = 0x02  # chunk ranges are [inclusive, exclusive)
cdef int _RESERVED_UNSKIPPABLE_RIGHT = 0x80
cdef int _RESERVED_SKIPPABLE_LEFT = 0x80
cdef int _RESERVED_SKIPPABLE_RIGHT = 0xff

cdef double _COMPRESSION_THRESHOLD = 0.125

cdef class Decompressor:
    cdef bytearray _buf
    cdef bint _header_found

    cpdef bytes decompress(self, bytes)

    cpdef flush(self)

cdef class Compressor:
    cdef bint _header_chunk_written

    cpdef bytes add_chunk(self, bytes, compress=?)

    cpdef bytes compress(self, bytes)

    cpdef flush(self, mode=?)
