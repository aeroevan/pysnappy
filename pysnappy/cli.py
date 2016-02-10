#!/usr/bin/env python
import argparse
from pysnappy.framing import Compressor, Decompressor
from pysnappy.framing import HadoopCompressor, HadoopDecompressor


def main():
    import sys
    parser = argparse.ArgumentParser(description="pysnappy driver")
    parser.add_argument("-f", "--file", help="Input file", default=sys.stdin)
    parser.add_argument("-b", "--bytesize",
                        help="Bitesize for streaming reads",
                        type=int, default=65536)
    parser.add_argument("-o", "--output", help="Output file",
                        default=sys.stdout)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-c", "--compress", action="store_true")
    group.add_argument("-d", "--decompress", action="store_false")
    parser.add_argument("-t", "--framing", help="Framing format",
                        choices=["framing2", "hadoop"], default="framing2")

    args = parser.parse_args()
    run(args)


def run(args):
    fh_in = open(args.file, "rb")
    fh_out = open(args.output, "wb")
    if args.compress:
        stream_compress(fh_in, fh_out, args.framing, args.bytesize)
    else:
        stream_decompress(fh_in, fh_out, args.framing, args.bytesize)

    fh_in.close()
    fh_out.close()


def stream_compress(fh_in, fh_out, framing, bs):
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


def stream_decompress(fh_in, fh_out, framing, bs):
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


if __name__ == "__main__":
    main()
