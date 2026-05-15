cmake_minimum_required(VERSION 3.15...3.26)
project(${SKBUILD_PROJECT_NAME} LANGUAGES C)

find_package(
  Python
  COMPONENTS Interpreter Development.Module
  REQUIRED)

find_package(Snappy REQUIRED)

add_custom_command(
  OUTPUT src/pysnappy/crc32c.c
  COMMENT
    "Making ${CMAKE_CURRENT_BINARY_DIR}/src/pysnappy/crc32c.c from ${CMAKE_CURRENT_SOURCE_DIR}/src/pysnappy/crc32c.pyx"
  COMMAND Python::Interpreter -m cython
          "${CMAKE_CURRENT_SOURCE_DIR}/src/pysnappy/crc32c.pyx" --output-file src/pysnappy/crc32c.c
  DEPENDS src/pysnappy/crc32c.pyx
  VERBATIM)

python_add_library(crc32c MODULE src/pysnappy/crc32c.c WITH_SOABI)

add_custom_command(
  OUTPUT src/pysnappy/core.c
  COMMENT
    "Making ${CMAKE_CURRENT_BINARY_DIR}/src/pysnappy/core.c from ${CMAKE_CURRENT_SOURCE_DIR}/src/pysnappy/core.pyx"
  COMMAND Python::Interpreter -m cython
          "${CMAKE_CURRENT_SOURCE_DIR}/src/pysnappy/core.pyx" --output-file src/pysnappy/core.c
  DEPENDS src/pysnappy/core.pyx
  VERBATIM)

python_add_library(core MODULE src/pysnappy/core.c WITH_SOABI)
target_link_libraries(core PRIVATE Snappy::snappy)

add_custom_command(
  OUTPUT src/pysnappy/framing.c
  COMMENT
    "Making ${CMAKE_CURRENT_BINARY_DIR}/src/pysnappy/framing.c from ${CMAKE_CURRENT_SOURCE_DIR}/src/pysnappy/framing.pyx"
  COMMAND Python::Interpreter -m cython
          "${CMAKE_CURRENT_SOURCE_DIR}/src/pysnappy/framing.pyx" --output-file src/pysnappy/framing.c
  DEPENDS src/pysnappy/framing.pyx
  VERBATIM)

python_add_library(framing MODULE src/pysnappy/framing.c WITH_SOABI)


install(TARGETS core crc32c framing DESTINATION pysnappy)
