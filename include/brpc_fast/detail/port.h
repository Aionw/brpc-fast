#pragma once

#include <bit>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>

#if defined(_MSC_VER)
#define BRPC_FAST_NOINLINE __declspec(noinline)
#elif defined(__GNUC__) || defined(__clang__)
#define BRPC_FAST_NOINLINE __attribute__((noinline))
#else
#define BRPC_FAST_NOINLINE
#endif

namespace brpc_fast::detail {

// Wire format assumes IEEE-754 32-bit float, 64-bit double, and either little-
// or big-endian byte order (no mixed-endian platforms).
static_assert(sizeof(float) == 4 && std::numeric_limits<float>::is_iec559,
              "brpc_fast requires IEEE-754 32-bit float");
static_assert(sizeof(double) == 8 && std::numeric_limits<double>::is_iec559,
              "brpc_fast requires IEEE-754 64-bit double");
static_assert(std::endian::native == std::endian::little ||
                  std::endian::native == std::endian::big,
              "brpc_fast does not support mixed-endian architectures");

inline constexpr uint16_t ByteSwap16(uint16_t v) noexcept {
#if defined(__cpp_lib_byteswap) && __cpp_lib_byteswap >= 202110L
    return std::byteswap(v);
#elif defined(__GNUC__) || defined(__clang__)
    return __builtin_bswap16(v);
#elif defined(_MSC_VER)
    return _byteswap_ushort(v);
#else
    return static_cast<uint16_t>((v << 8) | (v >> 8));
#endif
}

inline constexpr uint32_t ByteSwap32(uint32_t v) noexcept {
#if defined(__cpp_lib_byteswap) && __cpp_lib_byteswap >= 202110L
    return std::byteswap(v);
#elif defined(__GNUC__) || defined(__clang__)
    return __builtin_bswap32(v);
#elif defined(_MSC_VER)
    return _byteswap_ulong(v);
#else
    return ((v & 0x000000FFu) << 24) | ((v & 0x0000FF00u) << 8) |
           ((v & 0x00FF0000u) >> 8) | ((v & 0xFF000000u) >> 24);
#endif
}

inline constexpr uint64_t ByteSwap64(uint64_t v) noexcept {
#if defined(__cpp_lib_byteswap) && __cpp_lib_byteswap >= 202110L
    return std::byteswap(v);
#elif defined(__GNUC__) || defined(__clang__)
    return __builtin_bswap64(v);
#elif defined(_MSC_VER)
    return _byteswap_uint64(v);
#else
    return ((v & 0x00000000000000FFULL) << 56) |
           ((v & 0x000000000000FF00ULL) << 40) |
           ((v & 0x0000000000FF0000ULL) << 24) |
           ((v & 0x00000000FF000000ULL) << 8) |
           ((v & 0x000000FF00000000ULL) >> 8) |
           ((v & 0x0000FF0000000000ULL) >> 24) |
           ((v & 0x00FF000000000000ULL) >> 40) |
           ((v & 0xFF00000000000000ULL) >> 56);
#endif
}

template <typename T>
inline T ByteSwapFixed(T v) noexcept {
    static_assert(sizeof(T) == 1 || sizeof(T) == 2 || sizeof(T) == 4 ||
                      sizeof(T) == 8,
                  "ByteSwapFixed supports 1-, 2-, 4-, or 8-byte values");
    if constexpr (sizeof(T) == 1) {
        return v;
    } else if constexpr (sizeof(T) == 2) {
        return std::bit_cast<T>(ByteSwap16(std::bit_cast<uint16_t>(v)));
    } else if constexpr (sizeof(T) == 4) {
        return std::bit_cast<T>(ByteSwap32(std::bit_cast<uint32_t>(v)));
    } else {
        return std::bit_cast<T>(ByteSwap64(std::bit_cast<uint64_t>(v)));
    }
}

template <typename T>
inline T HostToLittleEndian(T v) noexcept {
    if constexpr (std::endian::native == std::endian::big) {
        return ByteSwapFixed(v);
    } else {
        return v;
    }
}

template <typename T>
inline T LittleEndianToHost(T v) noexcept {
    return HostToLittleEndian(v);
}

}  // namespace brpc_fast::detail
