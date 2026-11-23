#pragma once

// Internal helper included by brpc_fast/detail/field_codec.h after ValueCodec
// and field classification traits are defined. Contains only slow-path Proto3
// schema-evolution converters (cross-width scalars, packed <-> singular,
// repeated <-> singular merging).
//
// Wire-format caveats: because kFixed4/kFixed8 store fixed little-endian
// payloads without signedness or float/int tags, widening/narrowing follows the
// destination type's signedness (e.g. uint32(0xFFFFFFFF) -> int64 sign-extends
// to -1; int32(-1) -> uint64 zero-extends to 4294967295), and changing a field
// between float/double and integer types reinterprets the raw IEEE-754 bits
// rather than skipping the field.

#include <cstddef>
#include <cstdint>
#include <string>
#include <type_traits>
#include <vector>

namespace brpc_fast::detail {

// Result of slow-path Proto3 schema-evolution conversion.
enum class CompatStatus : uint8_t {
    kDecoded,       // Decoded successfully and updated target.
    kEmpty,         // Valid empty packed/list payload; leave target untouched.
    kIncompatible,  // Genuinely incompatible wire type; caller skips/captures.
    kError,         // Malformed/truncated wire payload.
};

inline constexpr bool IsFixedScalarWireType(WireType wt) noexcept {
    return wt == WireType::kFixed1 || wt == WireType::kFixed4 ||
           wt == WireType::kFixed8;
}

inline constexpr bool IsPackedScalarWireType(WireType wt) noexcept {
    return wt == WireType::kPacked1 || wt == WireType::kPacked4 ||
           wt == WireType::kPacked8;
}

inline constexpr size_t PackedElementSize(WireType wt) noexcept {
    if (wt == WireType::kPacked1) return 1;
    if (wt == WireType::kPacked4) return 4;
    return 8;
}

inline constexpr WireType PackedElementWireType(WireType wt) noexcept {
    if (wt == WireType::kPacked1) return WireType::kFixed1;
    if (wt == WireType::kPacked4) return WireType::kFixed4;
    return WireType::kFixed8;
}

// Converts any 1/4/8-byte wire integer/bool/enum to a compatible scalar target.
template <typename T>
inline bool ConvertFixedScalarFromWire(FastReader& r, WireType wt,
                                       T& out) noexcept {
    if constexpr (std::is_same_v<T, bool>) {
        if (wt == WireType::kFixed1) return r.ReadBool(out);
        if (wt == WireType::kFixed4) {
            uint32_t v = 0;
            if (!r.ReadFixed<uint32_t>(v)) return false;
            out = (v != 0);
            return true;
        }
        if (wt == WireType::kFixed8) {
            uint64_t v = 0;
            if (!r.ReadFixed<uint64_t>(v)) return false;
            out = (v != 0);
            return true;
        }
        return false;
    } else if constexpr (std::is_enum_v<T>) {
        int32_t raw = 0;
        if (!ConvertFixedScalarFromWire<int32_t>(r, wt, raw)) return false;
        out = static_cast<T>(raw);
        return true;
    } else if constexpr (std::is_signed_v<T>) {
        if (wt == WireType::kFixed1) {
            uint8_t b = 0;
            if (!r.ReadU8(b)) return false;
            out = static_cast<T>(b != 0 ? 1 : 0);
            return true;
        }
        if (wt == WireType::kFixed4) {
            int32_t v = 0;
            if (!r.ReadFixed<int32_t>(v)) return false;
            out = static_cast<T>(v);
            return true;
        }
        if (wt == WireType::kFixed8) {
            int64_t v = 0;
            if (!r.ReadFixed<int64_t>(v)) return false;
            out = static_cast<T>(v);
            return true;
        }
        return false;
    } else if constexpr (std::is_unsigned_v<T>) {
        if (wt == WireType::kFixed1) {
            uint8_t b = 0;
            if (!r.ReadU8(b)) return false;
            out = static_cast<T>(b != 0 ? 1u : 0u);
            return true;
        }
        if (wt == WireType::kFixed4) {
            uint32_t v = 0;
            if (!r.ReadFixed<uint32_t>(v)) return false;
            out = static_cast<T>(v);
            return true;
        }
        if (wt == WireType::kFixed8) {
            uint64_t v = 0;
            if (!r.ReadFixed<uint64_t>(v)) return false;
            out = static_cast<T>(v);
            return true;
        }
        return false;
    } else if constexpr (std::is_same_v<T, float>) {
        return wt == WireType::kFixed4 && r.ReadFixed<float>(out);
    } else if constexpr (std::is_same_v<T, double>) {
        return wt == WireType::kFixed8 && r.ReadFixed<double>(out);
    } else {
        return false;
    }
}

// Shared helper for merging a kList of message elements into a singular message
// target (plain message, optional/IndirectOptional, or oneof branch).
// `prepare_target()` is invoked only when the list is non-empty and returns
// `std::pair<MsgT&, bool /*already_present*/>`.
template <typename MsgT, typename PrepareFn>
BRPC_FAST_NOINLINE inline CompatStatus DecodeMessageListMerge(
    FastReader& r, PrepareFn&& prepare_target) {
    uint32_t len = 0;
    if (!r.ReadVarint32(len)) return CompatStatus::kError;
    FastReader sub(FastReader::kUninitialized);
    if (!r.ReadSubReaderSlow(len, sub, /*check_depth=*/false)) {
        return CompatStatus::kError;
    }
    uint32_t count = 0;
    if (!sub.ReadVarint32(count) || count > sub.Remaining()) {
        return CompatStatus::kError;
    }
    if (count == 0) {
        return sub.Eof() ? CompatStatus::kEmpty : CompatStatus::kError;
    }
    auto [target, already_present] = prepare_target();
    for (uint32_t i = 0; i < count; ++i) {
        const bool ok = (i == 0 && !already_present)
                            ? ValueCodec<MsgT>::DecodeValue(sub, target)
                            : ValueCodec<MsgT>::DecodeValueMerge(sub, target);
        if (!ok) return CompatStatus::kError;
    }
    return sub.Eof() ? CompatStatus::kDecoded : CompatStatus::kError;
}

// Decodes a length-delimited packed scalar or kList field into a singular
// scalar/string/message target, applying Proto3 schema-evolution semantics:
// - scalars/strings: last element wins (empty list is a no-op)
// - messages: merge all elements in order
template <typename U>
BRPC_FAST_NOINLINE inline CompatStatus DecodeSingularCompatSlow(
    FastReader& r, WireType wt, U& target, bool already_present) {
    if constexpr (kIsVarintCompatScalar<U>) {
        if (IsFixedScalarWireType(wt)) {
            return ConvertFixedScalarFromWire<U>(r, wt, target)
                       ? CompatStatus::kDecoded
                       : CompatStatus::kError;
        }
        if (IsPackedScalarWireType(wt)) {
            uint32_t byte_len = 0;
            if (!r.ReadVarint32(byte_len)) return CompatStatus::kError;
            const size_t elem_sz = PackedElementSize(wt);
            if ((byte_len % elem_sz) != 0 || r.Remaining() < byte_len) {
                return CompatStatus::kError;
            }
            if (byte_len == 0) return CompatStatus::kEmpty;
            if (byte_len > elem_sz && !r.Skip(byte_len - elem_sz)) {
                return CompatStatus::kError;
            }
            return ConvertFixedScalarFromWire<U>(r, PackedElementWireType(wt),
                                                 target)
                       ? CompatStatus::kDecoded
                       : CompatStatus::kError;
        }
        return CompatStatus::kIncompatible;
    } else if constexpr (std::is_same_v<U, float> ||
                         std::is_same_v<U, double>) {
        constexpr WireType kExpectedPacked =
            std::is_same_v<U, float> ? WireType::kPacked4 : WireType::kPacked8;
        if (wt != kExpectedPacked) return CompatStatus::kIncompatible;
        uint32_t byte_len = 0;
        if (!r.ReadVarint32(byte_len)) return CompatStatus::kError;
        if ((byte_len % sizeof(U)) != 0 || r.Remaining() < byte_len) {
            return CompatStatus::kError;
        }
        if (byte_len == 0) return CompatStatus::kEmpty;
        if (byte_len > sizeof(U) && !r.Skip(byte_len - sizeof(U))) {
            return CompatStatus::kError;
        }
        return r.ReadFixed<U>(target) ? CompatStatus::kDecoded
                                      : CompatStatus::kError;
    } else if constexpr (std::is_same_v<U, std::string>) {
        if (wt != WireType::kList) return CompatStatus::kIncompatible;
        uint32_t len = 0;
        if (!r.ReadVarint32(len)) return CompatStatus::kError;
        FastReader sub(FastReader::kUninitialized);
        if (!r.ReadSubReaderSlow(len, sub, /*check_depth=*/false)) {
            return CompatStatus::kError;
        }
        uint32_t count = 0;
        if (!sub.ReadVarint32(count) || count > sub.Remaining()) {
            return CompatStatus::kError;
        }
        if (count == 0) {
            return sub.Eof() ? CompatStatus::kEmpty : CompatStatus::kError;
        }
        for (uint32_t i = 0; i + 1 < count; ++i) {
            uint32_t slen = 0;
            if (!sub.ReadVarint32(slen) || !sub.Skip(slen)) {
                return CompatStatus::kError;
            }
        }
        if (!sub.ReadString(target)) return CompatStatus::kError;
        return sub.Eof() ? CompatStatus::kDecoded : CompatStatus::kError;
    } else if constexpr (kIsMessageOrBox<U>) {
        if (wt != WireType::kList) return CompatStatus::kIncompatible;
        return DecodeMessageListMerge<U>(r, [&]() -> std::pair<U&, bool> {
            return {target, already_present};
        });
    } else {
        return CompatStatus::kIncompatible;
    }
}

template <bool Overwrite, typename E>
BRPC_FAST_NOINLINE inline CompatStatus DecodeRepeatedCompatSlow(
    FastReader& r, WireType wt, std::vector<E>& vec, bool first) {
    if constexpr (kIsVarintCompatScalar<E>) {
        if (IsFixedScalarWireType(wt)) {
            E elem{};
            if (!ConvertFixedScalarFromWire<E>(r, wt, elem)) {
                return CompatStatus::kError;
            }
            if constexpr (Overwrite) {
                if (first) vec.clear();
            }
            vec.push_back(elem);
            return CompatStatus::kDecoded;
        }
        if (IsPackedScalarWireType(wt)) {
            uint32_t byte_len = 0;
            if (!r.ReadVarint32(byte_len)) return CompatStatus::kError;
            const size_t elem_sz = PackedElementSize(wt);
            if ((byte_len % elem_sz) != 0 || r.Remaining() < byte_len) {
                return CompatStatus::kError;
            }
            const size_t count = byte_len / elem_sz;
            if constexpr (Overwrite) {
                if (first) vec.clear();
            }
            vec.reserve(vec.size() + count);
            const WireType elem_wt = PackedElementWireType(wt);
            for (size_t i = 0; i < count; ++i) {
                E elem{};
                if (!ConvertFixedScalarFromWire<E>(r, elem_wt, elem)) {
                    return CompatStatus::kError;
                }
                vec.push_back(elem);
            }
            return CompatStatus::kDecoded;
        }
        return CompatStatus::kIncompatible;
    } else if constexpr (std::is_same_v<E, float> ||
                         std::is_same_v<E, double>) {
        constexpr WireType kExpectedFixed =
            std::is_same_v<E, float> ? WireType::kFixed4 : WireType::kFixed8;
        if (wt != kExpectedFixed) return CompatStatus::kIncompatible;
        E elem{};
        if (!r.ReadFixed<E>(elem)) return CompatStatus::kError;
        if constexpr (Overwrite) {
            if (first) vec.clear();
        }
        vec.push_back(elem);
        return CompatStatus::kDecoded;
    } else if constexpr (std::is_same_v<E, std::string>) {
        if (wt != WireType::kBytes) return CompatStatus::kIncompatible;
        if constexpr (Overwrite) {
            if (first) {
                vec.resize(1);
                return r.ReadString(vec[0]) ? CompatStatus::kDecoded
                                            : CompatStatus::kError;
            }
        }
        return r.ReadStringEmplace(vec) ? CompatStatus::kDecoded
                                        : CompatStatus::kError;
    } else if constexpr (FastMessageLike<E>) {
        if (wt != WireType::kBytes) return CompatStatus::kIncompatible;
        if constexpr (Overwrite) {
            if (first) {
                vec.resize(1);
                return ValueCodec<E>::DecodeValue(r, vec[0])
                           ? CompatStatus::kDecoded
                           : CompatStatus::kError;
            }
        }
        vec.emplace_back();
        return ValueCodec<E>::DecodeValue(r, vec.back())
                   ? CompatStatus::kDecoded
                   : CompatStatus::kError;
    } else {
        return CompatStatus::kIncompatible;
    }
}

}  // namespace brpc_fast::detail
