#!/usr/bin/env python
import argparse
from pysnappy.core import stream_compress, stream_decompress


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
    _profile = False
    if _profile:
        import pstats, cProfile
    fh_in = open(args.file, "rb")
    fh_out = open(args.output, "wb")
    if args.compress:
        if _profile:
            cProfile.runctx("stream_compress(fh_in, fh_out, args.framing, args.bytesize)", globals(), locals(), "Profile.prof")
        else:
            stream_compress(fh_in, fh_out, args.framing, args.bytesize)
    else:
        if _profile:
            cProfile.runctx("stream_decompress(fh_in, fh_out, args.framing, args.bytesize)", globals(), locals(), "Profile.prof")
        else:
            stream_decompress(fh_in, fh_out, args.framing, args.bytesize)

    if _profile:
        s = pstats.Stats("Profile.prof")
        s.strip_dirs().sort_stats("time").print_stats()

    fh_in.close()
    fh_out.close()


if __name__ == "__main__":
    main()
