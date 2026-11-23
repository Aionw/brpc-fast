include_guard(GLOBAL)

if(TARGET brpc::brpc)
  return()
endif()

include(FetchContent)

if(POLICY CMP0135)
  cmake_policy(SET CMP0135 NEW)
endif()

# Find imported targets (ZLIB::ZLIB, absl::*, glog::glog) in the parent scope
# so they are visible to both brpc and any target linking brpc::brpc.
find_package(Protobuf REQUIRED)
find_package(ZLIB REQUIRED)
if(Protobuf_VERSION VERSION_GREATER 4.21)
  find_package(absl REQUIRED CONFIG)
endif()
find_package(glog QUIET CONFIG)
if(TARGET glog::glog AND DEFINED glog_VERSION
   AND NOT glog_VERSION VERSION_LESS "0.7.0")
  # glog 0.7+ requires GLOG_USE_GLOG_EXPORT when headers are included without
  # linking glog::glog directly (brpc links libglog by raw library path).
  add_compile_definitions(GLOG_USE_GLOG_EXPORT)
endif()

find_program(BRPC_FAST_GIT_EXECUTABLE git REQUIRED)

set(BRPC_VERSION 1.18.0)
# Patch applied to upstream brpc 1.18.0:
#  1. Makes leveldb optional via WITH_LEVELDB (disabling rpcz SpanDB).
#  2. Gates libprotoc / protoc-gen-mcpack on BUILD_BRPC_TOOLS and drops
#     -D__STRICT_ANSI__ for C++20/glibc compatibility.
#  3. Extends the GCC TLS-address-caching workaround in butil/thread_local.h
#     from aarch64 to all GCC targets (needed under bthread context switches).
set(_BRPC_PATCH_FILE
    "${CMAKE_CURRENT_LIST_DIR}/../patches/brpc-without-leveldb.patch")

if(FETCHCONTENT_BASE_DIR)
  set(_BRPC_GIT_CEILING "${FETCHCONTENT_BASE_DIR}")
else()
  set(_BRPC_GIT_CEILING "${CMAKE_BINARY_DIR}/_deps")
endif()

set(_BRPC_FETCH_EXTRA_ARGS)
if(CMAKE_VERSION VERSION_GREATER_EQUAL "3.28")
  list(APPEND _BRPC_FETCH_EXTRA_ARGS EXCLUDE_FROM_ALL)
endif()

FetchContent_Declare(
  brpc
  URL ${GH_MIRROR}https://github.com/apache/brpc/archive/refs/tags/${BRPC_VERSION}.tar.gz
  URL_HASH
    SHA256=763795b13309e353642105fe8b55583ebf5a19a0e3b398a5fb4f9268c728cafc
  PATCH_COMMAND
    ${CMAKE_COMMAND} -E env "GIT_CEILING_DIRECTORIES=${_BRPC_GIT_CEILING}"
    ${BRPC_FAST_GIT_EXECUTABLE} apply --whitespace=nowarn "${_BRPC_PATCH_FILE}"
    ${_BRPC_FETCH_EXTRA_ARGS})

# Save parent options that collide with brpc's option names so brpc does not
# pick up the parent project's BUILD_UNIT_TESTS / BUILD_SHARED_LIBS or clobber
# them.
if(DEFINED BUILD_UNIT_TESTS)
  set(_BRPC_FAST_HAD_BUILD_UNIT_TESTS TRUE)
  set(_BRPC_FAST_SAVED_BUILD_UNIT_TESTS "${BUILD_UNIT_TESTS}")
else()
  set(_BRPC_FAST_HAD_BUILD_UNIT_TESTS FALSE)
endif()

if(DEFINED BUILD_SHARED_LIBS)
  set(_BRPC_FAST_HAD_BUILD_SHARED_LIBS TRUE)
  set(_BRPC_FAST_SAVED_BUILD_SHARED_LIBS "${BUILD_SHARED_LIBS}")
else()
  set(_BRPC_FAST_HAD_BUILD_SHARED_LIBS FALSE)
endif()

if(DEFINED CMAKE_CXX_SCAN_FOR_MODULES)
  set(_BRPC_FAST_HAD_CXX_SCAN_FOR_MODULES TRUE)
  set(_BRPC_FAST_SAVED_CXX_SCAN_FOR_MODULES "${CMAKE_CXX_SCAN_FOR_MODULES}")
else()
  set(_BRPC_FAST_HAD_CXX_SCAN_FOR_MODULES FALSE)
endif()

# Configure brpc via normal (directory-scoped) variables; brpc uses
# cmake_minimum_required(VERSION 3.16...3.28) so CMP0077=NEW honors these.
set(WITH_GLOG ON)
set(WITH_LEVELDB OFF)
if(DEFINED BRPC_FAST_ENABLE_DEBUG_SYMBOLS)
  set(WITH_DEBUG_SYMBOLS "${BRPC_FAST_ENABLE_DEBUG_SYMBOLS}")
else()
  set(WITH_DEBUG_SYMBOLS OFF)
endif()
if(DEFINED BRPC_FAST_ENABLE_ASAN)
  set(WITH_ASAN "${BRPC_FAST_ENABLE_ASAN}")
else()
  set(WITH_ASAN OFF)
endif()
set(BUILD_UNIT_TESTS OFF)
set(BUILD_FUZZ_TESTS OFF)
set(BUILD_BRPC_TOOLS OFF)
set(DOWNLOAD_GTEST OFF)
set(BUILD_SHARED_LIBS OFF)
set(CMAKE_CXX_SCAN_FOR_MODULES OFF)

if(CMAKE_VERSION VERSION_GREATER_EQUAL "3.28")
  FetchContent_MakeAvailable(brpc)
else()
  FetchContent_GetProperties(brpc)
  if(NOT brpc_POPULATED)
    FetchContent_Populate(brpc)
    add_subdirectory(${brpc_SOURCE_DIR} ${brpc_BINARY_DIR} EXCLUDE_FROM_ALL)
  endif()
endif()

# Restore parent variables and clean up brpc-only directory-scoped knobs.
if(_BRPC_FAST_HAD_BUILD_UNIT_TESTS)
  set(BUILD_UNIT_TESTS "${_BRPC_FAST_SAVED_BUILD_UNIT_TESTS}")
else()
  unset(BUILD_UNIT_TESTS)
endif()

if(_BRPC_FAST_HAD_BUILD_SHARED_LIBS)
  set(BUILD_SHARED_LIBS "${_BRPC_FAST_SAVED_BUILD_SHARED_LIBS}")
else()
  unset(BUILD_SHARED_LIBS)
endif()

if(_BRPC_FAST_HAD_CXX_SCAN_FOR_MODULES)
  set(CMAKE_CXX_SCAN_FOR_MODULES "${_BRPC_FAST_SAVED_CXX_SCAN_FOR_MODULES}")
else()
  unset(CMAKE_CXX_SCAN_FOR_MODULES)
endif()

unset(WITH_GLOG)
unset(WITH_LEVELDB)
unset(WITH_DEBUG_SYMBOLS)
unset(WITH_ASAN)
unset(BUILD_FUZZ_TESTS)
unset(BUILD_BRPC_TOOLS)
unset(DOWNLOAD_GTEST)
unset(_BRPC_FAST_HAD_BUILD_UNIT_TESTS)
unset(_BRPC_FAST_SAVED_BUILD_UNIT_TESTS)
unset(_BRPC_FAST_HAD_BUILD_SHARED_LIBS)
unset(_BRPC_FAST_SAVED_BUILD_SHARED_LIBS)
unset(_BRPC_FAST_HAD_CXX_SCAN_FOR_MODULES)
unset(_BRPC_FAST_SAVED_CXX_SCAN_FOR_MODULES)
unset(_BRPC_GIT_CEILING)
unset(_BRPC_FETCH_EXTRA_ARGS)
unset(_BRPC_PATCH_FILE)

# Read brpc's resolved transitive link libraries and include paths.
get_directory_property(_BRPC_DYNAMIC_LIB DIRECTORY "${brpc_SOURCE_DIR}"
                                                   DEFINITION DYNAMIC_LIB)
get_directory_property(
  _BRPC_INCLUDE_DIRS DIRECTORY "${brpc_SOURCE_DIR}" DEFINITION
                               BRPC_COMMON_INCLUDE_DIRS)

# Strip brpc_common_config from brpc-static's public interface so brpc's
# internal build flags (-O2, NDEBUG, -fstrict-aliasing, -W, __const__, etc.)
# do not leak into downstream targets.
set_target_properties(brpc-static PROPERTIES INTERFACE_LINK_LIBRARIES "")

if(NOT TARGET brpc::brpc)
  add_library(brpc_fast_brpc INTERFACE)
  add_library(brpc::brpc ALIAS brpc_fast_brpc)
  target_link_libraries(brpc_fast_brpc INTERFACE brpc-static
                                                 ${_BRPC_DYNAMIC_LIB})
  if(TARGET glog::glog)
    target_link_libraries(brpc_fast_brpc INTERFACE glog::glog)
  endif()
  # Generated protobuf headers live in brpc_BINARY_DIR and must precede src/.
  target_include_directories(
    brpc_fast_brpc SYSTEM
    INTERFACE "${brpc_BINARY_DIR}" "${brpc_SOURCE_DIR}/src"
              ${_BRPC_INCLUDE_DIRS})
  target_compile_definitions(brpc_fast_brpc INTERFACE BRPC_WITH_GLOG=1
                                                      BRPC_WITH_LEVELDB=0)
  if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    target_compile_definitions(brpc_fast_brpc
                               INTERFACE BTHREAD_USE_FAST_PTHREAD_MUTEX)
  endif()
endif()

unset(_BRPC_DYNAMIC_LIB)
unset(_BRPC_INCLUDE_DIRS)
