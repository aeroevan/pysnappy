import struct as pystruct
from pysnappy import compress, uncompress

cdef class HadoopDecompressor:
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
        cdef bytes output = b""
        cdef bytes buf
        self._buf += data
        while True:
            buf = self._decompress_block()
            if len(buf) > 0:
                output += buf
            else:
                break
        return output

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
                ">i", self._buf[:4])[0]
            self._buf = self._buf[4:]
        # Only attempt to decompress complete subblocks.
        if len(self._buf) < self._subblock_size:
            return b""
        compressed = self._buf[:self._subblock_size]
        self._buf = self._buf[self._subblock_size:]
        uncompressed = uncompress(compressed)
        self._block_read += len(uncompressed)
        self._subblock_size = -1
        return uncompressed


cdef class HadoopCompressor:
    cdef int _buffer_size
    cdef bytes _buf
    def __init__(self, buffer_size=131072):
        self._buffer_size = buffer_size
        self._buf = b""

    cpdef bytes compress(self, bytes data):
        cdef bytes output = b""
        cdef bytes buf
        cdef int uncompressed_length
        cdef int compressed_length
        self._buf += data
        while True:
            if len(self._buf) > self._buffer_size:
                buf = self._buf[:self._buffer_size]
                self._buf = self._buf[self.buffer_size:]
            else:
                buf = self._buf[:]
                self._buf = b""
            uncompressed_length = len(buf)
            if uncompressed_length == 0:
                break
            try:
                buf = compress(buf)
            except:
                break
            compressed_length = len(buf)
            output += pystruct.pack(">i", uncompressed_length)
            output += pystruct.pack(">i", compressed_length)
            output += buf
        return output

    def flush(self):
        if len(self._buf) > 0:
            raise Exception("Chunk truncated")
