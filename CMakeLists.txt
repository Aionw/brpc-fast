cmake_minimum_required(VERSION 3.16)
project(
  brpc_fast
  VERSION 0.1.0
  LANGUAGES CXX C)

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  set(_BRPC_FAST_IS_TOP_LEVEL ON)
else()
  set(_BRPC_FAST_IS_TOP_LEVEL OFF)
endif()

option(BRPC_FAST_BUILD_TESTS "Build brpc_fast unit tests"
       ${_BRPC_FAST_IS_TOP_LEVEL})
option(BRPC_FAST_BUILD_BENCHMARK "Build brpc_fast benchmark binary" OFF)
option(BRPC_FAST_ENABLE_ASAN "Enable AddressSanitizer in fetched bRPC" OFF)
option(BRPC_FAST_ENABLE_DEBUG_SYMBOLS "Enable debug symbols in fetched bRPC"
       OFF)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

include(FindBRPC)
include(BrpcFastGenerate)

# Standalone code generator: only links libprotobuf (descriptor.pb.h),
# no libprotoc-dev required.
add_executable(brpc_fast_codegen codegen/main.cpp)
target_compile_features(brpc_fast_codegen PRIVATE cxx_std_20)
if(TARGET protobuf::libprotobuf)
  target_link_libraries(brpc_fast_codegen PRIVATE protobuf::libprotobuf)
else()
  target_include_directories(brpc_fast_codegen PRIVATE ${PROTOBUF_INCLUDE_DIRS})
  target_link_libraries(brpc_fast_codegen PRIVATE ${PROTOBUF_LIBRARIES})
endif()
add_executable(brpc_fast::codegen ALIAS brpc_fast_codegen)

# Runtime library for brpc_fast codecs & BaiduMasterService routing.
add_library(brpc_fast STATIC src/service.cpp)
target_compile_features(brpc_fast PUBLIC cxx_std_20)
target_include_directories(
  brpc_fast
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:include>)
target_link_libraries(brpc_fast PUBLIC brpc::brpc)
set_target_properties(brpc_fast PROPERTIES POSITION_INDEPENDENT_CODE ON)
add_library(brpc_fast::brpc_fast ALIAS brpc_fast)

if(BRPC_FAST_BUILD_TESTS)
  enable_testing()

  if(NOT TARGET GTest::gtest_main AND NOT TARGET gtest_main)
    find_package(GTest QUIET)
  endif()
  if(TARGET GTest::gtest_main)
    set(_BRPC_FAST_GTEST_LIBS GTest::gtest GTest::gtest_main)
  elseif(TARGET gtest_main)
    set(_BRPC_FAST_GTEST_LIBS gtest gtest_main)
  else()
    include(FetchContent)
    if(POLICY CMP0135)
      cmake_policy(SET CMP0135 NEW)
    endif()
    set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
    FetchContent_Declare(
      googletest
      URL ${GH_MIRROR}https://github.com/google/googletest/archive/refs/tags/v1.14.0.tar.gz
      URL_HASH
        SHA256=8ad598c73ad796e0d8280b082cebd82a630d73e73cd3c70057938a6501bba5d7)
    FetchContent_MakeAvailable(googletest)
    set(_BRPC_FAST_GTEST_LIBS GTest::gtest GTest::gtest_main)
  endif()

  brpc_fast_generate(
    OUT_HDRS BRPC_FAST_TEST_HDRS
    OUT_SRCS BRPC_FAST_TEST_SRCS
    OUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated_tests"
    PROTO_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/tests"
    PROTO_FILES test_schema.proto test_schema_v2.proto)

  brpc_fast_generate(
    OUT_HDRS BRPC_FAST_PRESERVE_HDRS
    OUT_SRCS BRPC_FAST_PRESERVE_SRCS
    OUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated_tests"
    PROTO_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/tests"
    PROTO_FILES test_preserve.proto
    PRESERVE_UNKNOWN_FIELDS)

  add_library(brpc_fast_test_protos STATIC
              ${BRPC_FAST_TEST_SRCS} ${BRPC_FAST_TEST_HDRS}
              ${BRPC_FAST_PRESERVE_SRCS} ${BRPC_FAST_PRESERVE_HDRS})
  target_include_directories(
    brpc_fast_test_protos PUBLIC "${CMAKE_CURRENT_BINARY_DIR}/generated_tests")
  target_link_libraries(brpc_fast_test_protos PUBLIC brpc_fast::brpc_fast)

  add_executable(brpc_fast_codec_test tests/codec_test.cpp)
  target_link_libraries(brpc_fast_codec_test PRIVATE brpc_fast_test_protos
                                                     ${_BRPC_FAST_GTEST_LIBS})
  add_test(NAME brpc_fast_codec_test COMMAND brpc_fast_codec_test)

  add_executable(brpc_fast_rpc_test tests/rpc_test.cpp)
  target_link_libraries(brpc_fast_rpc_test PRIVATE brpc_fast_test_protos
                                                   ${_BRPC_FAST_GTEST_LIBS})
  add_test(NAME brpc_fast_rpc_test COMMAND brpc_fast_rpc_test)
endif()

if(BRPC_FAST_BUILD_BENCHMARK)
  brpc_fast_generate(
    OUT_HDRS BRPC_FAST_BENCH_HDRS
    OUT_SRCS BRPC_FAST_BENCH_SRCS
    OUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated_bench"
    PROTO_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/benchmarks"
    PROTO_FILES bench_schema.proto)

  add_executable(brpc_fast_codec_benchmark
                 benchmarks/codec_benchmark.cpp ${BRPC_FAST_BENCH_SRCS}
                 ${BRPC_FAST_BENCH_HDRS})
  target_include_directories(
    brpc_fast_codec_benchmark
    PRIVATE "${CMAKE_CURRENT_BINARY_DIR}/generated_bench")
  target_link_libraries(brpc_fast_codec_benchmark PRIVATE brpc_fast::brpc_fast)
endif()
