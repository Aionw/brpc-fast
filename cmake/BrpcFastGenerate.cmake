# Generate fast C++ struct + codec + bRPC service/stub headers and sources
# from .proto files using protoc + brpc_fast_codegen.
#
# Usage:
#   brpc_fast_generate(
#     OUT_HDRS <hdrs_var>
#     OUT_SRCS <srcs_var>
#     OUT_DIR <output_dir>
#     PROTO_ROOT <proto_root_dir>
#     PROTO_FILES <rel1.proto> [<rel2.proto> ...]
#     [IMPORT_DIRS <dir1> ...]
#     [PRESERVE_UNKNOWN_FIELDS]
#   )
function(brpc_fast_generate)
  cmake_parse_arguments(
    ARG "PRESERVE_UNKNOWN_FIELDS" "OUT_HDRS;OUT_SRCS;OUT_DIR;PROTO_ROOT"
    "PROTO_FILES;IMPORT_DIRS" ${ARGN})

  if(NOT ARG_OUT_DIR)
    set(ARG_OUT_DIR "${CMAKE_CURRENT_BINARY_DIR}")
  endif()
  if(NOT ARG_PROTO_ROOT)
    set(ARG_PROTO_ROOT "${CMAKE_CURRENT_SOURCE_DIR}")
  endif()

  set(_protoc_exec "${Protobuf_PROTOC_EXECUTABLE}")
  if(NOT _protoc_exec AND DEFINED PROTOBUF_PROTOC_EXECUTABLE)
    set(_protoc_exec "${PROTOBUF_PROTOC_EXECUTABLE}")
  endif()
  if(NOT _protoc_exec AND TARGET protobuf::protoc)
    set(_protoc_exec $<TARGET_FILE:protobuf::protoc>)
  endif()
  if(NOT _protoc_exec)
    message(
      FATAL_ERROR "protoc executable not found for brpc_fast_generate")
  endif()

  set(_import_flags "-I${ARG_PROTO_ROOT}")
  foreach(_dir IN LISTS ARG_IMPORT_DIRS)
    list(APPEND _import_flags "-I${_dir}")
  endforeach()

  set(_codegen_extra_flags)
  if(ARG_PRESERVE_UNKNOWN_FIELDS)
    list(APPEND _codegen_extra_flags "--preserve_unknown_fields")
  endif()

  set(_protoc_opt_flags)
  if(NOT Protobuf_VERSION OR Protobuf_VERSION VERSION_LESS "3.15")
    list(APPEND _protoc_opt_flags "--experimental_allow_proto3_optional")
  endif()

  set(_hdrs)
  set(_srcs)
  foreach(_proto IN LISTS ARG_PROTO_FILES)
    string(REGEX REPLACE "\\.proto$" "" _stem "${_proto}")
    set(_proto_abs "${ARG_PROTO_ROOT}/${_proto}")
    set(_desc "${ARG_OUT_DIR}/${_stem}.pb.desc")
    set(_hdr "${ARG_OUT_DIR}/${_stem}.fast.h")
    set(_src "${ARG_OUT_DIR}/${_stem}.fast.cc")

    get_filename_component(_desc_dir "${_desc}" DIRECTORY)
    set(_protoc_dep_flags)
    set(_depfile_args)
    if(CMAKE_GENERATOR MATCHES "Ninja"
       OR (CMAKE_VERSION VERSION_GREATER_EQUAL "3.20" AND CMAKE_GENERATOR
                                                          MATCHES "Makefiles"))
      set(_depfile "${ARG_OUT_DIR}/${_stem}.pb.d")
      set(_protoc_dep_flags "--dependency_out=${_depfile}")
      set(_depfile_args DEPFILE "${_depfile}")
    endif()

    if(POLICY CMP0116)
      cmake_policy(PUSH)
      cmake_policy(SET CMP0116 NEW)
    endif()
    add_custom_command(
      OUTPUT "${_desc}" "${_hdr}" "${_src}"
      COMMAND ${CMAKE_COMMAND} -E make_directory "${_desc_dir}"
      COMMAND
        "${_protoc_exec}" ${_import_flags} --include_imports
        ${_protoc_opt_flags} ${_protoc_dep_flags}
        "--descriptor_set_out=${_desc}" "${_proto_abs}"
      COMMAND
        $<TARGET_FILE:brpc_fast_codegen> "--descriptor_set=${_desc}"
        "--proto=${_proto}" "--out_dir=${ARG_OUT_DIR}" ${_codegen_extra_flags}
      DEPENDS "${_proto_abs}" brpc_fast_codegen ${_depfile_args}
      COMMENT "Generating brpc_fast C++ code for ${_proto}"
      VERBATIM)
    if(POLICY CMP0116)
      cmake_policy(POP)
    endif()

    list(APPEND _hdrs "${_hdr}")
    list(APPEND _srcs "${_src}")
  endforeach()

  if(ARG_OUT_HDRS)
    set(${ARG_OUT_HDRS}
        "${_hdrs}"
        PARENT_SCOPE)
  endif()
  if(ARG_OUT_SRCS)
    set(${ARG_OUT_SRCS}
        "${_srcs}"
        PARENT_SCOPE)
  endif()
endfunction()
