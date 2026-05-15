# cython: profile=False
from struct import pack, unpack_from
from cpython.bytearray cimport (
    PyByteArray_AS_STRING,
    PyByteArray_GET_SIZE,
    PyByteArray_Resize,
)
from cpython.bytes cimport PyBytes_AsString
from libc.stdint cimport uint32_t
from libc.string cimport memcpy
from pysnappy.crc32c cimport masked_crc32c_buf
from cpython.bytes cimport PyBytes_FromStringAndSize
from pysnappy.core cimport (
    compress as _compress,
    uncompress as _uncompress,
    _compress_append,
    _uncompress_append,
    _raw_compress,
    _max_compressed_len,
)


cdef inline void _write_be32(unsigned char* dst, unsigned int val) nogil:
    dst[0] = <unsigned char>((val >> 24) & 0xff)
    dst[1] = <unsigned char>((val >> 16) & 0xff)
    dst[2] = <unsigned char>((val >> 8) & 0xff)
    dst[3] = <unsigned char>(val & 0xff)


cdef int _CHUNK_MAX = 65536
cdef int _STREAM_TO_STREAM_BLOCK_SIZE = _CHUNK_MAX
cdef bytes _STREAM_IDENTIFIER = b"sNaPpY"
cdef int _COMPRESSED_CHUNK = 0x00
cdef int _UNCOMPRESSED_CHUNK = 0x01
cdef int _IDENTIFIER_CHUNK = 0xff
cdef int _RESERVED_UNSKIPPABLE_LEFT = 0x02
cdef int _RESERVED_UNSKIPPABLE_RIGHT = 0x80
cdef int _RESERVED_SKIPPABLE_LEFT = 0x80
cdef int _RESERVED_SKIPPABLE_RIGHT = 0xff
cdef double _COMPRESSION_THRESHOLD = 0.125


cdef class RawDecompressor:
    cpdef bytes decompress(self, bytes data):
        return _uncompress(data)
    cpdef flush(self):
        return b""


cdef class RawCompressor:
    cpdef bytes compress(self, bytes data):
        return _compress(data)
    cpdef flush(self):
        return b""


cdef class HadoopDecompressor:

    def __cinit__(self):
        self._buf = bytearray()
        self._buf_pos = 0
        self._block_size = -1
        self._block_read = 0
        self._subblock_size = -1

    cpdef bytes decompress(self, bytes data):
        cdef bytearray output = bytearray()
        self._buf.extend(data)
        try:
            while self._decompress_block(output):
                pass
        finally:
            if self._buf_pos > 0:
                del self._buf[:self._buf_pos]
                self._buf_pos = 0
        return bytes(output)

    cdef bint _decompress_block(self, bytearray output) except -1:
        """Decompress what we can of one block into `output`. Returns True if
        a full block was consumed (so the caller should loop again)."""
        if self._block_size < 0:
            if PyByteArray_GET_SIZE(self._buf) - self._buf_pos <= 4:
                return False
            self._block_size = unpack_from(">i", self._buf, self._buf_pos)[0]
            self._buf_pos += 4
        while self._block_read < self._block_size:
            if not self._decompress_subblock(output):
                return False
        # Finished this block.
        self._block_read = 0
        self._block_size = -1
        return True

    cdef bint _decompress_subblock(self, bytearray output) except -1:
        cdef Py_ssize_t available = PyByteArray_GET_SIZE(self._buf) - self._buf_pos
        cdef Py_ssize_t added
        if self._subblock_size < 0:
            if available <= 4:
                return False
            self._subblock_size = unpack_from(">i", self._buf, self._buf_pos)[0]
            self._buf_pos += 4
            available -= 4
        if available < self._subblock_size:
            return False
        added = _uncompress_append(
            PyByteArray_AS_STRING(self._buf) + self._buf_pos,
            <size_t>self._subblock_size,
            output,
        )
        self._buf_pos += self._subblock_size
        self._block_read += <int>added
        self._subblock_size = -1
        return True

    cpdef flush(self):
        if PyByteArray_GET_SIZE(self._buf) - self._buf_pos > 0:
            raise Exception("Chunk truncated")
        return b""


cdef class HadoopCompressor:
    def __cinit__(self, buffer_size=131072, single_subblock=False):
        self._buffer_size = buffer_size
        self._single_subblock = bool(single_subblock)
        self._buf = bytearray()

    cpdef bytes add_chunk(self, bytes data):
        cdef Py_ssize_t n = len(data)
        cdef bytes result
        cdef unsigned char* p
        cdef size_t cap
        cdef size_t compressed_size
        cdef bytearray output
        cdef Py_ssize_t pos
        cdef Py_ssize_t total
        cdef Py_ssize_t chunk_size
        cdef Py_ssize_t header_pos
        cdef Py_ssize_t sub_compressed
        cdef char* buf_ptr

        if self._single_subblock:
            # Fast path: one (block, subblock) pair per call, compressed in a
            # single snappy_compress call directly into a pre-sized bytes.
            if n == 0:
                return b""
            cap = _max_compressed_len(<size_t>n) + 8
            result = PyBytes_FromStringAndSize(NULL, <Py_ssize_t>cap)
            p = <unsigned char*>PyBytes_AsString(result)
            _write_be32(p, <unsigned int>n)
            compressed_size = _raw_compress(
                PyBytes_AsString(data), <size_t>n,
                <char*>(p + 8), cap - 8,
            )
            _write_be32(p + 4, <unsigned int>compressed_size)
            return result[:8 + <Py_ssize_t>compressed_size]

        # Default path: respect buffer_size, may emit multiple (block, subblock)
        # pairs per call. Useful when the consumer expects bounded subblocks.
        output = bytearray()
        self._buf.extend(data)
        total = PyByteArray_GET_SIZE(self._buf)
        while pos < total:
            chunk_size = self._buffer_size if (total - pos) > self._buffer_size else (total - pos)
            output.extend(pack(">i", <int>chunk_size))
            header_pos = PyByteArray_GET_SIZE(output)
            output.extend(b"\x00\x00\x00\x00")
            buf_ptr = PyByteArray_AS_STRING(self._buf) + pos
            sub_compressed = _compress_append(buf_ptr, <size_t>chunk_size, output)
            output[header_pos:header_pos + 4] = pack(">i", <int>sub_compressed)
            pos += chunk_size
        if pos > 0:
            del self._buf[:pos]
        return bytes(output)

    cpdef bytes compress(self, bytes data):
        return self.add_chunk(data)

    cpdef flush(self):
        if PyByteArray_GET_SIZE(self._buf) > 0:
            raise Exception("Chunk truncated")
        return b""


cdef class Decompressor:

    def __cinit__(self):
        self._buf = bytearray()
        self._header_found = False

    cpdef bytes decompress(self, bytes data):
        cdef bytearray output = bytearray()
        cdef bytearray buf = self._buf
        cdef Py_ssize_t pos = 0
        cdef Py_ssize_t buf_len
        cdef long chunk_type
        cdef long header
        cdef int size  # bytes of payload (CRC + data) after the 4-byte header
        cdef Py_ssize_t data_start
        cdef Py_ssize_t data_len
        cdef uint32_t stream_crc
        cdef Py_ssize_t out_size_before
        cdef Py_ssize_t added
        cdef char* buf_ptr
        cdef char* out_ptr
        buf.extend(data)
        buf_len = PyByteArray_GET_SIZE(buf)
        try:
            while True:
                if buf_len - pos < 4:
                    return bytes(output)
                header = unpack_from("<L", buf, pos)[0]
                size = <int>(header >> 8)
                chunk_type = header & 0xff
                if not self._header_found:
                    if chunk_type != _IDENTIFIER_CHUNK or size != len(_STREAM_IDENTIFIER):
                        raise Exception("Stream missing snappy identifier")
                    self._header_found = True
                if _RESERVED_UNSKIPPABLE_LEFT <= chunk_type < _RESERVED_UNSKIPPABLE_RIGHT:
                    raise Exception(
                        "Stream received unskippable but unknown chunk: " + str(chunk_type)
                    )
                if buf_len - pos < 4 + size:
                    return bytes(output)
                data_start = pos + 4
                if chunk_type == _IDENTIFIER_CHUNK:
                    if bytes(buf[data_start:data_start + size]) != _STREAM_IDENTIFIER:
                        raise Exception("Stream has invalid snappy identifier")
                    pos = data_start + size
                    continue
                if _RESERVED_SKIPPABLE_LEFT <= chunk_type < _RESERVED_SKIPPABLE_RIGHT:
                    pos = data_start + size
                    continue
                if chunk_type != _COMPRESSED_CHUNK and chunk_type != _UNCOMPRESSED_CHUNK:
                    raise AssertionError("invalid chunk_type")
                # First 4 bytes of payload are CRC; the rest is the (possibly
                # compressed) chunk data.
                stream_crc = <uint32_t>unpack_from("<L", buf, data_start)[0]
                data_start += 4
                data_len = size - 4
                buf_ptr = PyByteArray_AS_STRING(buf) + data_start
                if chunk_type == _COMPRESSED_CHUNK:
                    out_size_before = PyByteArray_GET_SIZE(output)
                    added = _uncompress_append(
                        buf_ptr, <size_t>data_len, output)
                    out_ptr = PyByteArray_AS_STRING(output) + out_size_before
                    if masked_crc32c_buf(out_ptr, <size_t>added) != stream_crc:
                        raise Exception("crc mismatch")
                else:
                    if masked_crc32c_buf(buf_ptr, <size_t>data_len) != stream_crc:
                        raise Exception("crc mismatch")
                    out_size_before = PyByteArray_GET_SIZE(output)
                    if PyByteArray_Resize(
                        output, out_size_before + data_len
                    ) != 0:
                        raise MemoryError()
                    out_ptr = PyByteArray_AS_STRING(output) + out_size_before
                    # buf_ptr may have been invalidated by the resize above
                    # if output and buf are the same object (they aren't), but
                    # in general re-fetch after a resize on a *different*
                    # bytearray is unnecessary. Use the pointer we already
                    # computed since `buf` was not resized.
                    memcpy(out_ptr, buf_ptr, <size_t>data_len)
                pos = data_start + data_len
        finally:
            if pos > 0:
                del buf[:pos]

    cpdef flush(self):
        if PyByteArray_GET_SIZE(self._buf) > 0:
            raise Exception("Chunk truncated")
        return b""


cdef class Compressor:

    def __cinit__(self):
        self._header_chunk_written = False

    cpdef bytes add_chunk(self, bytes data, compress=None):
        cdef bytearray out = bytearray()
        cdef Py_ssize_t i = 0
        cdef Py_ssize_t n = len(data)
        cdef Py_ssize_t chunk_len
        cdef uint32_t crc
        cdef Py_ssize_t header_pos
        cdef Py_ssize_t chunk_data_pos
        cdef Py_ssize_t actual_len
        cdef long chunk_type
        cdef const char* src = PyBytes_AsString(data)

        if not self._header_chunk_written:
            self._header_chunk_written = True
            out.extend(pack(
                "<L", _IDENTIFIER_CHUNK + (len(_STREAM_IDENTIFIER) << 8)))
            out.extend(_STREAM_IDENTIFIER)

        while i < n:
            chunk_len = _CHUNK_MAX if (n - i) > _CHUNK_MAX else (n - i)
            crc = masked_crc32c_buf(src + i, <size_t>chunk_len)
            header_pos = PyByteArray_GET_SIZE(out)
            # Reserve 8 bytes for the chunk header (chunk_type+size, CRC);
            # filled in after we know the final data length.
            out.extend(b"\x00\x00\x00\x00\x00\x00\x00\x00")
            chunk_data_pos = PyByteArray_GET_SIZE(out)
            if compress is False:
                if PyByteArray_Resize(
                    out, chunk_data_pos + chunk_len
                ) != 0:
                    raise MemoryError()
                memcpy(
                    PyByteArray_AS_STRING(out) + chunk_data_pos,
                    src + i,
                    <size_t>chunk_len,
                )
                actual_len = chunk_len
                chunk_type = _UNCOMPRESSED_CHUNK
            else:
                _compress_append(src + i, <size_t>chunk_len, out)
                actual_len = PyByteArray_GET_SIZE(out) - chunk_data_pos
                if compress is None and (
                    <double>actual_len > (1.0 - _COMPRESSION_THRESHOLD) * <double>chunk_len
                ):
                    # Roll back compressed data and emit raw instead.
                    if PyByteArray_Resize(
                        out, chunk_data_pos + chunk_len
                    ) != 0:
                        raise MemoryError()
                    memcpy(
                        PyByteArray_AS_STRING(out) + chunk_data_pos,
                        src + i,
                        <size_t>chunk_len,
                    )
                    actual_len = chunk_len
                    chunk_type = _UNCOMPRESSED_CHUNK
                else:
                    chunk_type = _COMPRESSED_CHUNK
            out[header_pos:header_pos + 8] = pack(
                "<LL",
                chunk_type + ((actual_len + 4) << 8),
                crc,
            )
            i += chunk_len
        return bytes(out)

    cpdef bytes compress(self, bytes data):
        return self.add_chunk(data)

    cpdef flush(self, mode=None):
        return b""
