# cython: profile=False
from libc.stdlib cimport malloc, free
from pysnappy.snappy cimport snappy_compress, snappy_uncompress, snappy_uncompressed_length, snappy_max_compressed_length, snappy_status
from pysnappy.framing cimport Compressor, Decompressor
from pysnappy.framing cimport HadoopCompressor, HadoopDecompressor

cpdef bytes uncompress(bytes compressed):
    cdef size_t n = len(compressed)
    cdef size_t m
    cdef snappy_status status
    cdef char* uncompressed
    status = snappy_uncompressed_length(
        compressed, n, &m)
    if status != 0:
        raise Exception("Could not determine uncompressed length")
    #uncompressed = <char*>PyMem_Malloc(m * sizeof(char*))
    uncompressed = <char*>malloc(m * sizeof(char*))
    if not uncompressed:
        raise MemoryError("Could not allocate uncompressed buffer")
    status = snappy_uncompress(compressed[:n], n, uncompressed, &m)
    if status != 0:
        #PyMem_Free(uncompressed)
        free(uncompressed)
        raise Exception("Could not uncompress")
    return uncompressed[:m]

cpdef bytes compress(bytes uncompressed):
    cdef size_t n = len(uncompressed)
    cdef size_t m = snappy_max_compressed_length(n)
    cdef snappy_status status
    cdef char* compressed
    compressed = <char*>malloc(m * sizeof(char*))
    if not compressed:
        raise MemoryError("Could not allocate compressed buffer")
    status = snappy_compress(uncompressed, n, compressed, &m)
    if status != 0:
        free(compressed)
        raise Exception("Could not compress")
    return compressed[:m]

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
