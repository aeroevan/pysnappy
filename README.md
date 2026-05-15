You probably want python-snappy

## Hadoop framing compatibility

There is one case where you might prefer this library: reading Hadoop
snappy streams.

The Hadoop snappy format allows a "block" to contain one or more
subblocks. Real Hadoop writers (`BlockCompressorStream`) emit
multi-subblock blocks when an input exceeds the configured buffer size.

- pysnappy's `HadoopDecompressor` handles both single-subblock and
  multi-subblock blocks.
- python-snappy's `HadoopStreamDecompressor` only handles
  single-subblock blocks; on multi-subblock input it silently returns
  the first subblock and stashes the rest in an internal buffer that is
  never surfaced. Data loss with no error.

For the encoder, pysnappy emits multi-subblock blocks by default
(respecting the `buffer_size` argument). Pass `single_subblock=True` to
the `HadoopCompressor` constructor to emit one subblock per call, which
is byte-shape-compatible with python-snappy's encoder and slightly
faster.
