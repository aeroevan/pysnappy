# cython: profile=False
from cpython.bytes cimport PyBytes_FromStringAndSize, PyBytes_AsString
from cpython.bytearray cimport (
    PyByteArray_AS_STRING,
    PyByteArray_GET_SIZE,
    PyByteArray_Resize,
)
from pysnappy.snappy cimport (
    snappy_compress,
    snappy_uncompress,
    snappy_uncompressed_length,
    snappy_max_compressed_length,
    snappy_status,
)
from pysnappy.framing cimport Compressor, Decompressor
from pysnappy.framing cimport HadoopCompressor, HadoopDecompressor


cdef size_t _raw_compress(
    const char* src, size_t n, char* dst, size_t cap
) except? 0:
    cdef size_t out_len = cap
    cdef snappy_status status
    with nogil:
        status = snappy_compress(src, n, dst, &out_len)
    if status != 0:
        raise Exception("Could not compress")
    return out_len


cdef void _raw_uncompress(
    const char* src, size_t n, char* dst, size_t cap
) except *:
    cdef size_t out_len = cap
    cdef snappy_status status
    with nogil:
        status = snappy_uncompress(src, n, dst, &out_len)
    if status != 0:
        raise Exception("Could not uncompress")


cdef size_t _max_compressed_len(size_t n) nogil:
    return snappy_max_compressed_length(n)


cdef bytes _compress_buf(const char* src, size_t n):
    cdef size_t cap = snappy_max_compressed_length(n)
    cdef bytes result = PyBytes_FromStringAndSize(NULL, <Py_ssize_t>cap)
    cdef size_t out_len = _raw_compress(src, n, PyBytes_AsString(result), cap)
    if out_len == cap:
        return result
    return result[:out_len]


cdef bytes _uncompress_buf(const char* src, size_t n):
    cdef size_t cap
    cdef snappy_status status = snappy_uncompressed_length(src, n, &cap)
    if status != 0:
        raise Exception("Could not determine uncompressed length")
    cdef bytes result = PyBytes_FromStringAndSize(NULL, <Py_ssize_t>cap)
    _raw_uncompress(src, n, PyBytes_AsString(result), cap)
    return result


cdef Py_ssize_t _compress_append(
    const char* src, size_t n, bytearray dst
) except -1:
    """Snappy-compress `src` and append to `dst`. Returns bytes appended."""
    cdef Py_ssize_t old_size = PyByteArray_GET_SIZE(dst)
    cdef size_t cap = snappy_max_compressed_length(n)
    if PyByteArray_Resize(dst, old_size + <Py_ssize_t>cap) != 0:
        raise MemoryError()
    cdef char* dst_ptr = PyByteArray_AS_STRING(dst) + old_size
    cdef size_t out_len = _raw_compress(src, n, dst_ptr, cap)
    if PyByteArray_Resize(dst, old_size + <Py_ssize_t>out_len) != 0:
        raise MemoryError()
    return <Py_ssize_t>out_len


cdef Py_ssize_t _uncompress_append(
    const char* src, size_t n, bytearray dst
) except -1:
    """Snappy-uncompress `src` and append to `dst`. Returns bytes appended."""
    cdef size_t cap
    cdef snappy_status status = snappy_uncompressed_length(src, n, &cap)
    if status != 0:
        raise Exception("Could not determine uncompressed length")
    cdef Py_ssize_t old_size = PyByteArray_GET_SIZE(dst)
    if PyByteArray_Resize(dst, old_size + <Py_ssize_t>cap) != 0:
        raise MemoryError()
    cdef char* dst_ptr = PyByteArray_AS_STRING(dst) + old_size
    _raw_uncompress(src, n, dst_ptr, cap)
    return <Py_ssize_t>cap


cpdef bytes compress(data, encoding="utf-8"):
    cdef bytes buf
    if isinstance(data, str):
        buf = (<str>data).encode(encoding)
    else:
        buf = data
    return _compress_buf(PyBytes_AsString(buf), <size_t>len(buf))


cpdef bytes uncompress(data):
    cdef bytes buf = data
    return _uncompress_buf(PyBytes_AsString(buf), <size_t>len(buf))


cpdef bytes decompress(data):
    return uncompress(data)


cpdef void stream_compress(fh_in, fh_out, framing, int bs=65536):
    if framing == "framing2":
        compressor = Compressor()
    else:
        compressor = HadoopCompressor()

    while True:
        buf = fh_in.read(bs)
        if buf:
            buf = compressor.add_chunk(buf)
        else:
            break
        if buf:
            fh_out.write(buf)


cpdef void stream_decompress(fh_in, fh_out, framing, int bs=65536):
    if framing == "framing2":
        decompressor = Decompressor()
    else:
        decompressor = HadoopDecompressor()

    while True:
        buf = fh_in.read(bs)
        if buf:
            buf = decompressor.decompress(buf)
        else:
            break
        if buf:
            fh_out.write(buf)
