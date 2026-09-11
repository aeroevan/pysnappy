# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Cython wrapper around Google's Snappy. `core` binds the C API of a system
libsnappy; `crc32c` and `framing` are pure-Cython implementations of CRC32C and
of the framing2 (`.sz`) and Hadoop stream formats — those two link nothing.

The reason to prefer this over `python-snappy` is Hadoop multi-subblock
decoding (see README.md): real Hadoop `BlockCompressorStream` writers emit
blocks containing several subblocks, and `python-snappy`'s
`HadoopStreamDecompressor` silently drops all but the first. Any change to
`HadoopDecompressor` must keep multi-subblock blocks working, and
`HadoopCompressor(single_subblock=True)` must stay byte-shape-compatible with
python-snappy's encoder.

## Commands

Requires a system libsnappy with headers (`libsnappy-dev` / `snappy-devel`) and
a C++ toolchain + CMake. CI uses `uv`.

```bash
uv sync                       # builds the three extension modules and installs dev deps
uv run pytest
uv run pytest tests/test_pysnappy.py::TestHadoop::test_roundtrip   # single test
uv sync --reinstall-package pysnappy                               # force rebuild after editing .pyx/.pxd/CMakeLists.txt
```

Editing a `.pyx` or `.pxd` does **not** take effect until the extension is
rebuilt — a plain `uv run pytest` may silently test the previously built
modules.

The interop tests (`TestPythonSnappyInterop`) are skipped unless `python-snappy`
is importable; it is in the `dev` dependency group, so a bare `uv sync` gets it.
Losing that dependency turns the cross-compatibility suite into a no-op rather
than a failure.

## Build wiring

`pyproject.toml` → scikit-build-core → `CMakeLists.txt`. There is no `setup.py`
and no automatic `.pyx` globbing: each module has an explicit
`add_custom_command` that shells out to `python -m cython`, followed by
`python_add_library(...)`. **Adding a new `.pyx` means adding both blocks plus
the module name to the final `install(TARGETS ...)`.**

Only `core` links Snappy (`target_link_libraries(core PRIVATE Snappy::snappy)`)
and only `core` needs `LINKER_LANGUAGE CXX` — libsnappy is C++, reached through
the C shim declared in `snappy.pxd`.

## Module structure and the cimport graph

- `snappy.pxd` — `cdef extern` declarations for `snappy-c.h`. No implementation.
- `core.pyx` — raw compress/uncompress plus `stream_compress`/`stream_decompress`.
- `crc32c.pyx` — masked CRC32C.
- `framing.pyx` — the four stream codecs: `Compressor`/`Decompressor`
  (framing2) and `HadoopCompressor`/`HadoopDecompressor`, plus trivial
  `RawCompressor`/`RawDecompressor`.
- `__init__.py` re-exports only `compress`/`decompress`/`uncompress`; framing
  classes are imported from `pysnappy.framing` directly.
- `cli.py` is a driver for `stream_compress`/`stream_decompress`, exposed as the
  `pysnappy` console script via `[project.scripts]`. It reads stdin and writes
  stdout unless `-f`/`-o` are given, and decompresses unless `-c` is passed.
  It reaches the wheel because scikit-build-core auto-detects `src/pysnappy` as
  a package — `CMakeLists.txt` only installs the three extension modules.

`core` and `framing` cimport each other: `framing.pyx` pulls the buffer helpers
(`_compress_append`, `_uncompress_append`, `_raw_compress`,
`_max_compressed_len`) out of `core.pxd`, while `core.pyx` cimports the codec
classes out of `framing.pxd` for its streaming helpers. Cython handles this
because the cimports are declaration-only, but it means a signature change
ripples across both modules.

The `.pxd` files are the source of truth for `cdef class` attributes and for
`cdef`/`cpdef` signatures. A signature or attribute edited in a `.pyx` without
the matching `.pxd` edit fails at cythonize time. Note that the framing format
constants are *declared* in `framing.pxd` and *defined again* in `framing.pyx`
— both copies must be changed together.

## Performance idioms to preserve

This code was deliberately optimized (see commit "Lots of performance
improvements"); the awkward-looking patterns are load-bearing:

- Output is built by appending into a `bytearray` via `PyByteArray_Resize` and
  writing through a raw `char*`, rather than concatenating `bytes`. Helpers in
  `core.pxd` ending in `_append` do this and return the number of bytes added.
- **A `PyByteArray_Resize` invalidates every `PyByteArray_AS_STRING` pointer
  into that object.** Re-fetch the pointer after any resize of the buffer it
  points into.
- Every `snappy_*` call and the CRC inner loops run under `nogil`.
- Incremental decoders keep leftover input in `self._buf` with a separate
  `_buf_pos` cursor, and trim consumed bytes once in a `finally` — they do not
  `del` from the front of the buffer per chunk.
- `crc32c.pyx` embeds a C block with an SSE4.2 `_mm_crc32_*` path selected at
  runtime via `__builtin_cpu_supports`, falling back to the table loop. Keep
  both paths in agreement.
- `HadoopCompressor.add_chunk` has two distinct code paths (buffered
  multi-subblock, and the `single_subblock` fast path that compresses straight
  into a pre-sized `bytes`); both are covered by tests and both must round-trip.

## Tests

`tests/test_pysnappy.py` is entirely round-trip based against
`tests/iris.csv` — there are no golden compressed fixtures (they were removed
deliberately). Multi-buffer cases use `iris.csv * 256` to exceed the 131072-byte
Hadoop buffer and the 65536-byte framing2 chunk max. Cross-compatibility with
python-snappy is asserted in both directions for raw, framing2, and Hadoop.

## Releasing

Bump `version` in `pyproject.toml`, refresh `uv.lock`, commit as
`Release X.Y.Z`, then publish a GitHub release tagged `vX.Y.Z`. That triggers
`.github/workflows/release.yml`, which builds Snappy 1.2.2 statically from
source inside cibuildwheel for linux/macOS x86_64+arm64 and publishes to PyPI
via trusted publishing. Changing the vendored Snappy version means editing the
pinned URL in both `CIBW_BEFORE_ALL_LINUX` and `CIBW_BEFORE_ALL_MACOS`.
