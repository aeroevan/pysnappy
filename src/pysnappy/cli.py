#!/usr/bin/env python
import argparse
import sys

from pysnappy.core import stream_compress, stream_decompress


def main(argv=None):
    parser = argparse.ArgumentParser(prog="pysnappy",
                                     description="pysnappy driver")
    parser.add_argument("-f", "--file",
                        help="Input file (default: stdin)")
    parser.add_argument("-b", "--bytesize",
                        help="Block size for streaming reads",
                        type=int, default=65536)
    parser.add_argument("-o", "--output",
                        help="Output file (default: stdout)")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-c", "--compress", action="store_true")
    group.add_argument("-d", "--decompress", action="store_true")
    parser.add_argument("-t", "--framing", help="Framing format",
                        choices=["framing2", "hadoop"], default="framing2")

    return run(parser.parse_args(argv))


def run(args):
    fh_in = open(args.file, "rb") if args.file else sys.stdin.buffer
    try:
        fh_out = open(args.output, "wb") if args.output else sys.stdout.buffer
        try:
            if args.compress:
                stream_compress(fh_in, fh_out, args.framing, args.bytesize)
            else:
                stream_decompress(fh_in, fh_out, args.framing, args.bytesize)
            fh_out.flush()
        finally:
            if args.output:
                fh_out.close()
    finally:
        if args.file:
            fh_in.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
