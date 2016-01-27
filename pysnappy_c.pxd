cdef extern from "snappy-c.h":
    ctypedef enum snappy_status:
        snappy_ok = 0
        snappy_invalid_input = 1
        snappy_buffer_too_small = 2

    snappy_status snappy_compress(
        const char* ipt, size_t input_length, char* compressed,
        size_t* compressed_length)
    snappy_status snappy_uncompress(
        const char* compressed, size_t compressed_length, char* uncompressed,
        size_t* uncompressed_length)
    snappy_status snappy_uncompressed_length(
        const char* compressed, size_t compressed_length, size_t* result)
