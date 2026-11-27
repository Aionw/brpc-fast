#pragma once

// Internal helper included by brpc_fast/detail/field_codec.h after ValueCodec,
// CompatStatus, and field classification traits are defined. Contains:
// 1. Slow-path Proto3 schema-evolution converters (exact PB signedness for
//    32->64 widening, float/double isolation from integers, packed/const/narrow
//    /bit-packed <-> singular, repeated <-> singular merging).
// 2. Columnar (SoA) encoding/decoding helpers for flat scalar repeated
//    messages, preserving full Proto3 forward/backward schema evolution.

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace brpc_fast::detail {

// ---- IndirectBox unwrapping helpers ----

template <typename T>
struct UnwrapIndirectBox {
    using type = T;
    static inline T& Ref(T& v) noexcept { return v; }
    static inline const T& Ref(const T& v) noexcept { return v; }
};

template <typename U>
struct UnwrapIndirectBox<IndirectBox<U>> {
    using type = U;
    static inline U& Ref(IndirectBox<U>& v) { return v.get(); }
    static inline const U& Ref(const IndirectBox<U>& v) { return v.get(); }
};

template <typename T>
using UnwrapIndirectBox_t = typename UnwrapIndirectBox<T>::type;

// ---- WireType / SeqKind classification ----

inline constexpr bool IsVarintFixedWireType(WireType wt) noexcept {
    return wt == WireType::kFixed1 || wt == WireType::kSigned4 ||
           wt == WireType::kUnsigned4 || wt == WireType::kInt8;
}

inline constexpr bool IsVarintPackedSeqKind(SeqKind kind) noexcept {
    return kind == SeqKind::kBool || kind == SeqKind::kSigned4 ||
           kind == SeqKind::kUnsigned4 || kind == SeqKind::kInt8 ||
           kind == SeqKind::kNarrowU8 || kind == SeqKind::kNarrowI8 ||
           kind == SeqKind::kNarrowU16 || kind == SeqKind::kNarrowI16;
}

inline constexpr bool IsVarintConstSeqKind(SeqKind kind) noexcept {
    return kind == SeqKind::kConstBool || kind == SeqKind::kConstS4 ||
           kind == SeqKind::kConstU4 || kind == SeqKind::kConstI8;
}

inline constexpr size_t PackedSeqElemBytes(SeqKind kind) noexcept {
    switch (kind) {
        case SeqKind::kBool:
        case SeqKind::kNarrowU8:
        case SeqKind::kNarrowI8:
            return 1;
        case SeqKind::kNarrowU16:
        case SeqKind::kNarrowI16:
            return 2;
        case SeqKind::kSigned4:
        case SeqKind::kUnsigned4:
        case SeqKind::kFloat4:
            return 4;
        case SeqKind::kInt8:
        case SeqKind::kFloat8:
            return 8;
        default:
            return 0;
    }
}

inline constexpr size_t ConstSeqValBytes(SeqKind kind) noexcept {
    switch (kind) {
        case SeqKind::kConstBool:
            return 1;
        case SeqKind::kConstS4:
        case SeqKind::kConstU4:
        case SeqKind::kConstF4:
            return 4;
        case SeqKind::kConstI8:
        case SeqKind::kConstF8:
            return 8;
        default:
            return 0;
    }
}

// ---- Exact Proto3 scalar conversion (preserves sender signedness!) ----

// Converts a signed wire integer (sign-extended to int64_t) to any
// varint-compatible target (bool, int32, uint32, int64, uint64, enum).
template <typename TargetT>
inline constexpr TargetT CastFromSigned64(int64_t v) noexcept {
    if constexpr (std::is_same_v<TargetT, bool>) {
        return v != 0;
    } else if constexpr (std::is_enum_v<TargetT>) {
        return static_cast<TargetT>(static_cast<int32_t>(v));
    } else {
        return static_cast<TargetT>(v);
    }
}

// Converts an unsigned wire integer (zero-extended to uint64_t) to any
// varint-compatible target (bool, int32, uint32, int64, uint64, enum).
template <typename TargetT>
inline constexpr TargetT CastFromUnsigned64(uint64_t v) noexcept {
    if constexpr (std::is_same_v<TargetT, bool>) {
        return v != 0;
    } else if constexpr (std::is_enum_v<TargetT>) {
        return static_cast<TargetT>(static_cast<int32_t>(v));
    } else {
        return static_cast<TargetT>(v);
    }
}

// Converts a singular varint-compatible wire scalar (kFixed1, kSigned4,
// kUnsigned4, kInt8) to `out` with exact Proto3 sign/zero-extension semantics:
// - kSigned4 (int32/enum): sign-extends to 64-bit (int32(-1) -> uint64_max)
// - kUnsigned4 (uint32):   zero-extends to 64-bit (uint32_max -> +4294967295)
// - kInt8 (int64/uint64):  64-bit bit-preserving / low-32 truncation
// Rejects kFloat4 / kFloat8.
template <typename T>
inline bool ConvertVarintFixedFromWire(FastReader& r, WireType wt,
                                       T& out) noexcept {
    static_assert(kIsVarintCompatScalar<T>);
    switch (wt) {
        case WireType::kFixed1: {
            uint8_t b = 0;
            if (!r.ReadU8(b)) return false;
            out = CastFromUnsigned64<T>(b != 0 ? 1ULL : 0ULL);
            return true;
        }
        case WireType::kSigned4: {
            int32_t v = 0;
            if (!r.ReadFixed<int32_t>(v)) return false;
            out = CastFromSigned64<T>(static_cast<int64_t>(v));
            return true;
        }
        case WireType::kUnsigned4: {
            uint32_t v = 0;
            if (!r.ReadFixed<uint32_t>(v)) return false;
            out = CastFromUnsigned64<T>(static_cast<uint64_t>(v));
            return true;
        }
        case WireType::kInt8: {
            uint64_t v = 0;
            if (!r.ReadFixed<uint64_t>(v)) return false;
            out = CastFromUnsigned64<T>(v);
            return true;
        }
        default:
            return false;
    }
}

template <typename T>
inline bool ConvertVarintElemFromSeq(FastReader& r, SeqKind kind,
                                     T& out) noexcept {
    static_assert(kIsVarintCompatScalar<T>);
    switch (kind) {
        case SeqKind::kBool:
        case SeqKind::kConstBool: {
            uint8_t b = 0;
            if (!r.ReadU8(b)) return false;
            out = CastFromUnsigned64<T>(b != 0 ? 1ULL : 0ULL);
            return true;
        }
        case SeqKind::kNarrowU8: {
            uint8_t v = 0;
            if (!r.ReadU8(v)) return false;
            out = CastFromUnsigned64<T>(static_cast<uint64_t>(v));
            return true;
        }
        case SeqKind::kNarrowI8: {
            uint8_t raw = 0;
            if (!r.ReadU8(raw)) return false;
            out = CastFromSigned64<T>(
                static_cast<int64_t>(static_cast<int8_t>(raw)));
            return true;
        }
        case SeqKind::kNarrowU16: {
            uint16_t v = 0;
            if (!r.ReadFixed<uint16_t>(v)) return false;
            out = CastFromUnsigned64<T>(static_cast<uint64_t>(v));
            return true;
        }
        case SeqKind::kNarrowI16: {
            int16_t v = 0;
            if (!r.ReadFixed<int16_t>(v)) return false;
            out = CastFromSigned64<T>(static_cast<int64_t>(v));
            return true;
        }
        case SeqKind::kSigned4:
        case SeqKind::kConstS4: {
            int32_t v = 0;
            if (!r.ReadFixed<int32_t>(v)) return false;
            out = CastFromSigned64<T>(static_cast<int64_t>(v));
            return true;
        }
        case SeqKind::kUnsigned4:
        case SeqKind::kConstU4: {
            uint32_t v = 0;
            if (!r.ReadFixed<uint32_t>(v)) return false;
            out = CastFromUnsigned64<T>(static_cast<uint64_t>(v));
            return true;
        }
        case SeqKind::kInt8:
        case SeqKind::kConstI8: {
            uint64_t v = 0;
            if (!r.ReadFixed<uint64_t>(v)) return false;
            out = CastFromUnsigned64<T>(v);
            return true;
        }
        default:
            return false;
    }
}

template <typename T>
inline T ConvertVarintFromContiguous(const uint8_t*& cur,
                                     SeqKind kind) noexcept {
    static_assert(kIsVarintCompatScalar<T>);
    switch (kind) {
        case SeqKind::kBool: {
            const uint8_t b = *cur++;
            return CastFromUnsigned64<T>(b != 0 ? 1ULL : 0ULL);
        }
        case SeqKind::kNarrowU8: {
            const uint8_t v = *cur++;
            return CastFromUnsigned64<T>(static_cast<uint64_t>(v));
        }
        case SeqKind::kNarrowI8: {
            const int8_t v = static_cast<int8_t>(*cur++);
            return CastFromSigned64<T>(static_cast<int64_t>(v));
        }
        case SeqKind::kNarrowU16: {
            uint16_t raw = 0;
            std::memcpy(&raw, cur, 2);
            cur += 2;
            return CastFromUnsigned64<T>(
                static_cast<uint64_t>(LittleEndianToHost(raw)));
        }
        case SeqKind::kNarrowI16: {
            int16_t raw = 0;
            std::memcpy(&raw, cur, 2);
            cur += 2;
            return CastFromSigned64<T>(
                static_cast<int64_t>(LittleEndianToHost(raw)));
        }
        case SeqKind::kSigned4: {
            int32_t raw = 0;
            std::memcpy(&raw, cur, 4);
            cur += 4;
            return CastFromSigned64<T>(
                static_cast<int64_t>(LittleEndianToHost(raw)));
        }
        case SeqKind::kUnsigned4: {
            uint32_t raw = 0;
            std::memcpy(&raw, cur, 4);
            cur += 4;
            return CastFromUnsigned64<T>(
                static_cast<uint64_t>(LittleEndianToHost(raw)));
        }
        case SeqKind::kInt8: {
            uint64_t raw = 0;
            std::memcpy(&raw, cur, 8);
            cur += 8;
            return CastFromUnsigned64<T>(LittleEndianToHost(raw));
        }
        default:
            return T{};
    }
}

// ---- Bit-packed bool decoding helpers ----

template <bool Overwrite>
inline bool DecodeBitBoolBodyContiguous(const uint8_t* cur, const uint8_t* end,
                                        std::vector<bool>& vec) {
    uint32_t count = 0;
    if (!ReadVarint32Contiguous(cur, end, count)) [[unlikely]] {
        return false;
    }
    const uint64_t expected_bytes = (static_cast<uint64_t>(count) + 7u) >> 3;
    if (static_cast<size_t>(end - cur) != expected_bytes) [[unlikely]] {
        return false;
    }
    const size_t base = Overwrite ? 0 : vec.size();
    vec.resize(base + count);
    uint32_t i = 0;
    for (; i + 8 <= count; i += 8) {
        const uint8_t byte = *cur++;
        vec[base + i] = (byte & 0x01u) != 0;
        vec[base + i + 1] = (byte & 0x02u) != 0;
        vec[base + i + 2] = (byte & 0x04u) != 0;
        vec[base + i + 3] = (byte & 0x08u) != 0;
        vec[base + i + 4] = (byte & 0x10u) != 0;
        vec[base + i + 5] = (byte & 0x20u) != 0;
        vec[base + i + 6] = (byte & 0x40u) != 0;
        vec[base + i + 7] = (byte & 0x80u) != 0;
    }
    if (i < count) {
        const uint8_t byte = *cur++;
        for (uint32_t bit = 0; i < count; ++i, ++bit) {
            vec[base + i] = ((byte >> bit) & 1u) != 0;
        }
    }
    return true;
}

template <bool Overwrite>
inline bool DecodeBitBoolSeq(FastReader& r, std::vector<bool>& vec) {
    uint32_t body_len = 0;
    const uint8_t* span = nullptr;
    const SpanReadStatus st = r.TryReadContiguousSpan(body_len, span);
    if (st == SpanReadStatus::kContiguous) [[likely]] {
        return DecodeBitBoolBodyContiguous<Overwrite>(span, span + body_len,
                                                      vec);
    }
    if (st == SpanReadStatus::kError) {
        return false;
    }
    FastReader sub(FastReader::kUninitialized);
    if (!r.ReadSubReaderSlow(body_len, sub, /*check_depth=*/false)) {
        return false;
    }
    uint32_t count = 0;
    if (!sub.ReadVarint32(count)) {
        return false;
    }
    const uint64_t expected_bytes = (static_cast<uint64_t>(count) + 7u) >> 3;
    if (sub.Remaining() != expected_bytes) {
        return false;
    }
    const size_t base = Overwrite ? 0 : vec.size();
    vec.resize(base + count);
    uint32_t i = 0;
    for (; i + 8 <= count; i += 8) {
        uint8_t byte = 0;
        if (!sub.ReadU8(byte)) return false;
        vec[base + i] = (byte & 0x01u) != 0;
        vec[base + i + 1] = (byte & 0x02u) != 0;
        vec[base + i + 2] = (byte & 0x04u) != 0;
        vec[base + i + 3] = (byte & 0x08u) != 0;
        vec[base + i + 4] = (byte & 0x10u) != 0;
        vec[base + i + 5] = (byte & 0x20u) != 0;
        vec[base + i + 6] = (byte & 0x40u) != 0;
        vec[base + i + 7] = (byte & 0x80u) != 0;
    }
    if (i < count) {
        uint8_t byte = 0;
        if (!sub.ReadU8(byte)) return false;
        for (uint32_t bit = 0; i < count; ++i, ++bit) {
            vec[base + i] = ((byte >> bit) & 1u) != 0;
        }
    }
    return true;
}

// ---- Singular message list & columnar merge helpers ----

// Shared helper for merging a kBytesList of message elements into a singular
// message target (plain message, optional/IndirectOptional, or oneof branch).
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

template <typename MsgT, typename PrepareFn>
BRPC_FAST_NOINLINE inline CompatStatus DecodeMessageColumnarMerge(
    FastReader& r, PrepareFn&& prepare_target) {
    if (r.RemainingDepth() <= 0) [[unlikely]] {
        return CompatStatus::kError;
    }
    uint32_t len = 0;
    if (!r.ReadVarint32(len)) return CompatStatus::kError;
    FastReader sub(FastReader::kUninitialized);
    if (!r.ReadSubReaderSlow(len, sub, /*check_depth=*/true)) {
        return CompatStatus::kError;
    }
    uint32_t count = 0;
    if (!sub.ReadVarint32(count)) {
        return CompatStatus::kError;
    }
    if (count == 0) {
        return sub.Eof() ? CompatStatus::kEmpty : CompatStatus::kError;
    }
    using InnerT = UnwrapIndirectBox_t<MsgT>;
    if (static_cast<uint64_t>(count) * sizeof(InnerT) > kMaxExpandedSeqBytes) {
        return CompatStatus::kError;
    }
    auto [target, already_present] = prepare_target();
    return InnerT::DecodeColumnarSingular(
        sub, count, UnwrapIndirectBox<MsgT>::Ref(target), already_present);
}

// ---- Slow-path Proto3 schema-evolution decoders ----

// Decodes a kSeq field (whose 1-byte `raw_kind` was already consumed) into a
// singular target `U`, applying Proto3 schema-evolution semantics:
// - scalars/strings: last element wins (empty seq -> kEmpty)
// - messages: merge all elements in order (both kBytesList and kColumnar)
template <typename U>
BRPC_FAST_NOINLINE inline CompatStatus DecodeSingularFromSeq(
    FastReader& r, uint8_t raw_kind, U& target, bool already_present) {
    const SeqKind kind = static_cast<SeqKind>(raw_kind);
    if constexpr (kIsVarintCompatScalar<U>) {
        if (IsVarintPackedSeqKind(kind)) {
            uint32_t byte_len = 0;
            if (!r.ReadVarint32(byte_len)) return CompatStatus::kError;
            const size_t elem_sz = PackedSeqElemBytes(kind);
            if ((byte_len % elem_sz) != 0 || r.Remaining() < byte_len) {
                return CompatStatus::kError;
            }
            if (byte_len == 0) return CompatStatus::kEmpty;
            if (byte_len > elem_sz && !r.Skip(byte_len - elem_sz)) {
                return CompatStatus::kError;
            }
            return ConvertVarintElemFromSeq<U>(r, kind, target)
                       ? CompatStatus::kDecoded
                       : CompatStatus::kError;
        }
        if (kind == SeqKind::kBitBool) {
            uint32_t body_len = 0;
            if (!r.ReadVarint32(body_len)) return CompatStatus::kError;
            FastReader sub(FastReader::kUninitialized);
            if (!r.ReadSubReaderSlow(body_len, sub, /*check_depth=*/false)) {
                return CompatStatus::kError;
            }
            uint32_t count = 0;
            if (!sub.ReadVarint32(count)) return CompatStatus::kError;
            const uint64_t expected_bytes =
                (static_cast<uint64_t>(count) + 7u) >> 3;
            if (sub.Remaining() != expected_bytes) return CompatStatus::kError;
            if (count == 0) return CompatStatus::kEmpty;
            if (expected_bytes > 1 && !sub.Skip(expected_bytes - 1)) {
                return CompatStatus::kError;
            }
            uint8_t last_byte = 0;
            if (!sub.ReadU8(last_byte)) return CompatStatus::kError;
            const uint32_t bit = (count - 1u) & 7u;
            target =
                CastFromUnsigned64<U>(((last_byte >> bit) & 1u) ? 1ULL : 0ULL);
            return CompatStatus::kDecoded;
        }
        if (IsVarintConstSeqKind(kind)) {
            uint32_t body_len = 0;
            if (!r.ReadVarint32(body_len)) return CompatStatus::kError;
            FastReader sub(FastReader::kUninitialized);
            if (!r.ReadSubReaderSlow(body_len, sub, /*check_depth=*/false)) {
                return CompatStatus::kError;
            }
            uint32_t count = 0;
            if (!sub.ReadVarint32(count) ||
                sub.Remaining() != ConstSeqValBytes(kind)) {
                return CompatStatus::kError;
            }
            if (count == 0) {
                return sub.Skip(ConstSeqValBytes(kind)) ? CompatStatus::kEmpty
                                                        : CompatStatus::kError;
            }
            return ConvertVarintElemFromSeq<U>(sub, kind, target)
                       ? CompatStatus::kDecoded
                       : CompatStatus::kError;
        }
        return CompatStatus::kIncompatible;
    } else if constexpr (std::is_same_v<U, float> ||
                         std::is_same_v<U, double>) {
        constexpr SeqKind kExpectedPacked =
            std::is_same_v<U, float> ? SeqKind::kFloat4 : SeqKind::kFloat8;
        constexpr SeqKind kExpectedConst =
            std::is_same_v<U, float> ? SeqKind::kConstF4 : SeqKind::kConstF8;
        if (kind == kExpectedPacked) {
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
        }
        if (kind == kExpectedConst) {
            uint32_t body_len = 0;
            if (!r.ReadVarint32(body_len)) return CompatStatus::kError;
            FastReader sub(FastReader::kUninitialized);
            if (!r.ReadSubReaderSlow(body_len, sub, /*check_depth=*/false)) {
                return CompatStatus::kError;
            }
            uint32_t count = 0;
            if (!sub.ReadVarint32(count) || sub.Remaining() != sizeof(U)) {
                return CompatStatus::kError;
            }
            if (count == 0) {
                return sub.Skip(sizeof(U)) ? CompatStatus::kEmpty
                                           : CompatStatus::kError;
            }
            return sub.ReadFixed<U>(target) ? CompatStatus::kDecoded
                                            : CompatStatus::kError;
        }
        return CompatStatus::kIncompatible;
    } else if constexpr (std::is_same_v<U, std::string>) {
        if (kind != SeqKind::kBytesList) return CompatStatus::kIncompatible;
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
        if (kind == SeqKind::kBytesList) {
            return DecodeMessageListMerge<U>(r, [&]() -> std::pair<U&, bool> {
                return {target, already_present};
            });
        }
        if (kind == SeqKind::kColumnar) {
            return DecodeMessageColumnarMerge<U>(
                r, [&]() -> std::pair<U&, bool> {
                    return {target, already_present};
                });
        }
        return CompatStatus::kIncompatible;
    } else {
        return CompatStatus::kIncompatible;
    }
}

// Decodes a non-kSeq mismatched WireType into a singular target `U`.
template <typename U>
BRPC_FAST_NOINLINE inline CompatStatus DecodeSingularNonSeqCompat(
    FastReader& r, WireType wt, U& target) {
    if constexpr (kIsVarintCompatScalar<U>) {
        if (IsVarintFixedWireType(wt)) {
            return ConvertVarintFixedFromWire<U>(r, wt, target)
                       ? CompatStatus::kDecoded
                       : CompatStatus::kError;
        }
        return CompatStatus::kIncompatible;
    } else {
        (void)r;
        (void)wt;
        (void)target;
        return CompatStatus::kIncompatible;
    }
}

// Decodes a non-canonical kSeq (whose 1-byte `raw_kind` was already consumed)
// into `std::vector<E>`.
template <bool Overwrite, typename E>
BRPC_FAST_NOINLINE inline CompatStatus DecodeRepeatedFromSeqCompat(
    FastReader& r, uint8_t raw_kind, std::vector<E>& vec, bool first) {
    const SeqKind kind = static_cast<SeqKind>(raw_kind);
    const bool reset_existing = Overwrite && first;
    if constexpr (kIsVarintCompatScalar<E>) {
        if (IsVarintPackedSeqKind(kind)) {
            uint32_t byte_len = 0;
            if (!r.ReadVarint32(byte_len)) return CompatStatus::kError;
            const size_t elem_sz = PackedSeqElemBytes(kind);
            if ((byte_len % elem_sz) != 0 || r.Remaining() < byte_len) {
                return CompatStatus::kError;
            }
            const size_t count = byte_len / elem_sz;
            if (reset_existing) vec.clear();
            const size_t base = vec.size();
            vec.resize(base + count);
            for (size_t i = 0; i < count; ++i) {
                E elem{};
                if (!ConvertVarintElemFromSeq<E>(r, kind, elem)) {
                    return CompatStatus::kError;
                }
                vec[base + i] = elem;
            }
            return CompatStatus::kDecoded;
        }
        if (kind == SeqKind::kBitBool) {
            uint32_t body_len = 0;
            if (!r.ReadVarint32(body_len)) return CompatStatus::kError;
            FastReader sub(FastReader::kUninitialized);
            if (!r.ReadSubReaderSlow(body_len, sub, /*check_depth=*/false)) {
                return CompatStatus::kError;
            }
            uint32_t count = 0;
            if (!sub.ReadVarint32(count)) return CompatStatus::kError;
            const uint64_t expected_bytes =
                (static_cast<uint64_t>(count) + 7u) >> 3;
            if (sub.Remaining() != expected_bytes) return CompatStatus::kError;
            if (reset_existing) vec.clear();
            const size_t base = vec.size();
            vec.resize(base + count);
            uint32_t i = 0;
            for (; i + 8 <= count; i += 8) {
                uint8_t byte = 0;
                if (!sub.ReadU8(byte)) return CompatStatus::kError;
                for (uint32_t b = 0; b < 8; ++b) {
                    vec[base + i + b] = CastFromUnsigned64<E>(
                        ((byte >> b) & 1u) ? 1ULL : 0ULL);
                }
            }
            if (i < count) {
                uint8_t byte = 0;
                if (!sub.ReadU8(byte)) return CompatStatus::kError;
                for (uint32_t b = 0; i < count; ++i, ++b) {
                    vec[base + i] = CastFromUnsigned64<E>(
                        ((byte >> b) & 1u) ? 1ULL : 0ULL);
                }
            }
            return CompatStatus::kDecoded;
        }
        if (IsVarintConstSeqKind(kind)) {
            uint32_t body_len = 0;
            if (!r.ReadVarint32(body_len)) return CompatStatus::kError;
            FastReader sub(FastReader::kUninitialized);
            if (!r.ReadSubReaderSlow(body_len, sub, /*check_depth=*/false)) {
                return CompatStatus::kError;
            }
            uint32_t count = 0;
            if (!sub.ReadVarint32(count) ||
                sub.Remaining() != ConstSeqValBytes(kind)) {
                return CompatStatus::kError;
            }
            constexpr size_t kElemBytes =
                std::is_same_v<E, bool> ? 1u : sizeof(E);
            if (static_cast<uint64_t>(count) * kElemBytes >
                kMaxExpandedSeqBytes) {
                return CompatStatus::kError;
            }
            E elem{};
            if (!ConvertVarintElemFromSeq<E>(sub, kind, elem)) {
                return CompatStatus::kError;
            }
            if (reset_existing) vec.clear();
            const size_t base = vec.size();
            vec.resize(base + count);
            std::fill_n(vec.begin() + static_cast<std::ptrdiff_t>(base), count,
                        elem);
            return CompatStatus::kDecoded;
        }
        return CompatStatus::kIncompatible;
    } else if constexpr (std::is_same_v<E, float> ||
                         std::is_same_v<E, double>) {
        constexpr SeqKind kExpectedConst =
            std::is_same_v<E, float> ? SeqKind::kConstF4 : SeqKind::kConstF8;
        if (kind != kExpectedConst) return CompatStatus::kIncompatible;
        uint32_t body_len = 0;
        if (!r.ReadVarint32(body_len)) return CompatStatus::kError;
        FastReader sub(FastReader::kUninitialized);
        if (!r.ReadSubReaderSlow(body_len, sub, /*check_depth=*/false)) {
            return CompatStatus::kError;
        }
        uint32_t count = 0;
        if (!sub.ReadVarint32(count) || sub.Remaining() != sizeof(E)) {
            return CompatStatus::kError;
        }
        if (static_cast<uint64_t>(count) * sizeof(E) > kMaxExpandedSeqBytes) {
            return CompatStatus::kError;
        }
        E elem{};
        if (!sub.ReadFixed<E>(elem)) return CompatStatus::kError;
        if (reset_existing) vec.clear();
        const size_t base = vec.size();
        vec.resize(base + count);
        std::fill_n(vec.data() + base, count, elem);
        return CompatStatus::kDecoded;
    } else if constexpr (FastMessageLike<E>) {
        if (kind != SeqKind::kColumnar) return CompatStatus::kIncompatible;
        if (r.RemainingDepth() <= 0) [[unlikely]] {
            return CompatStatus::kError;
        }
        uint32_t body_len = 0;
        if (!r.ReadVarint32(body_len)) return CompatStatus::kError;
        FastReader sub(FastReader::kUninitialized);
        if (!r.ReadSubReaderSlow(body_len, sub, /*check_depth=*/true)) {
            return CompatStatus::kError;
        }
        const bool ok = reset_existing
                            ? E::template DecodeColumnarVec<true>(sub, vec)
                            : E::template DecodeColumnarVec<false>(sub, vec);
        return ok ? CompatStatus::kDecoded : CompatStatus::kError;
    } else {
        (void)r;
        (void)raw_kind;
        (void)vec;
        (void)first;
        return CompatStatus::kIncompatible;
    }
}

// Decodes a singular (non-kSeq) wire field into `std::vector<E>`.
template <bool Overwrite, typename E>
BRPC_FAST_NOINLINE inline CompatStatus DecodeRepeatedFromNonSeqCompat(
    FastReader& r, WireType wt, std::vector<E>& vec, bool first) {
    const bool reset_existing = Overwrite && first;
    if constexpr (kIsVarintCompatScalar<E>) {
        if (!IsVarintFixedWireType(wt)) return CompatStatus::kIncompatible;
        E elem{};
        if (!ConvertVarintFixedFromWire<E>(r, wt, elem)) {
            return CompatStatus::kError;
        }
        if (reset_existing) vec.clear();
        vec.push_back(elem);
        return CompatStatus::kDecoded;
    } else if constexpr (std::is_same_v<E, float> ||
                         std::is_same_v<E, double>) {
        constexpr WireType kExpected =
            std::is_same_v<E, float> ? WireType::kFloat4 : WireType::kFloat8;
        if (wt != kExpected) return CompatStatus::kIncompatible;
        E elem{};
        if (!r.ReadFixed<E>(elem)) return CompatStatus::kError;
        if (reset_existing) vec.clear();
        vec.push_back(elem);
        return CompatStatus::kDecoded;
    } else if constexpr (std::is_same_v<E, std::string>) {
        if (wt != WireType::kBytes) return CompatStatus::kIncompatible;
        if (reset_existing) {
            vec.resize(1);
            return r.ReadString(vec[0]) ? CompatStatus::kDecoded
                                        : CompatStatus::kError;
        }
        return r.ReadStringEmplace(vec) ? CompatStatus::kDecoded
                                        : CompatStatus::kError;
    } else if constexpr (FastMessageLike<E>) {
        if (wt != WireType::kBytes) return CompatStatus::kIncompatible;
        if (reset_existing) {
            vec.resize(1);
            return ValueCodec<E>::DecodeValue(r, vec[0])
                       ? CompatStatus::kDecoded
                       : CompatStatus::kError;
        }
        vec.emplace_back();
        return ValueCodec<E>::DecodeValue(r, vec.back())
                   ? CompatStatus::kDecoded
                   : CompatStatus::kError;
    } else {
        return CompatStatus::kIncompatible;
    }
}

// ---- Columnar (SoA) encoding & decoding helpers ----

enum class ColumnEncoding : uint32_t {
    kOmit = 0,
    kConst = 1,
    kNarrowU8 = 2,
    kNarrowI8 = 3,
    kNarrowU16 = 4,
    kNarrowI16 = 5,
    kSigned4 = 6,
    kUnsigned4 = 7,
    kNative = 8,
};

template <typename E>
inline bool ScalarBitEqual(E a, E b) noexcept {
    if constexpr (std::is_same_v<E, float>) {
        return std::bit_cast<uint32_t>(a) == std::bit_cast<uint32_t>(b);
    } else if constexpr (std::is_same_v<E, double>) {
        return std::bit_cast<uint64_t>(a) == std::bit_cast<uint64_t>(b);
    } else {
        return a == b;
    }
}

template <typename E>
inline consteval SeqKind ConstSeqKindFor() {
    if constexpr (std::is_same_v<E, bool>) {
        return SeqKind::kConstBool;
    } else if constexpr (std::is_enum_v<E> || std::is_same_v<E, int32_t>) {
        return SeqKind::kConstS4;
    } else if constexpr (std::is_same_v<E, uint32_t>) {
        return SeqKind::kConstU4;
    } else if constexpr (std::is_same_v<E, int64_t> ||
                         std::is_same_v<E, uint64_t>) {
        return SeqKind::kConstI8;
    } else if constexpr (std::is_same_v<E, float>) {
        return SeqKind::kConstF4;
    } else {
        static_assert(std::is_same_v<E, double>);
        return SeqKind::kConstF8;
    }
}

template <typename E>
inline consteval SeqKind NativeSeqKindFor() {
    if constexpr (std::is_same_v<E, bool>) {
        return SeqKind::kBitBool;
    } else if constexpr (std::is_enum_v<E> || std::is_same_v<E, int32_t>) {
        return SeqKind::kSigned4;
    } else if constexpr (std::is_same_v<E, uint32_t>) {
        return SeqKind::kUnsigned4;
    } else if constexpr (std::is_same_v<E, int64_t> ||
                         std::is_same_v<E, uint64_t>) {
        return SeqKind::kInt8;
    } else if constexpr (std::is_same_v<E, float>) {
        return SeqKind::kFloat4;
    } else {
        static_assert(std::is_same_v<E, double>);
        return SeqKind::kFloat8;
    }
}

template <uint32_t FieldNumber, typename M, typename F>
inline size_t SizeScalarColumn(const M* data, uint32_t count, F M::* member,
                               SizeTape& tape) {
    const size_t slot = tape.Reserve();
    constexpr size_t kHdr = SeqHeaderSize(FieldNumber);
    ColumnEncoding enc = ColumnEncoding::kNative;
    size_t body_len = 0;

    if constexpr (std::is_same_v<F, bool>) {
        const bool first = data[0].*member;
        bool all_same = true;
        for (uint32_t i = 1; i < count; ++i) {
            if (data[i].*member != first) {
                all_same = false;
                break;
            }
        }
        if (all_same) {
            if (!first) {
                enc = ColumnEncoding::kOmit;
            } else {
                enc = ColumnEncoding::kConst;
                body_len = Varint32Size(count) + 1u;
            }
        } else {
            enc = ColumnEncoding::kNative;
            body_len = Varint32Size(count) + ((static_cast<size_t>(count) + 7u) >> 3);
        }
    } else if constexpr (std::is_same_v<F, float> ||
                         std::is_same_v<F, double>) {
        using BitsT = std::conditional_t<std::is_same_v<F, float>, uint32_t,
                                         uint64_t>;
        const BitsT first = std::bit_cast<BitsT>(data[0].*member);
        BitsT or_acc = first;
        BitsT diff_acc = 0;
        for (uint32_t i = 1; i < count; ++i) {
            const BitsT b = std::bit_cast<BitsT>(data[i].*member);
            or_acc |= b;
            diff_acc |= (b ^ first);
        }
        if (or_acc == 0) {
            enc = ColumnEncoding::kOmit;
        } else if (diff_acc == 0) {
            enc = ColumnEncoding::kConst;
            body_len = Varint32Size(count) + sizeof(F);
        } else {
            enc = ColumnEncoding::kNative;
            body_len = static_cast<size_t>(count) * sizeof(F);
        }
    } else if constexpr (std::is_unsigned_v<F>) {
        const F first = data[0].*member;
        F or_acc = first;
        F diff_acc = 0;
        for (uint32_t i = 1; i < count; ++i) {
            const F v = data[i].*member;
            or_acc |= v;
            diff_acc |= (v ^ first);
        }
        if (or_acc == 0) {
            enc = ColumnEncoding::kOmit;
        } else if (diff_acc == 0) {
            enc = ColumnEncoding::kConst;
            body_len = Varint32Size(count) + sizeof(F);
        } else if (or_acc <= 0xFFu) {
            enc = ColumnEncoding::kNarrowU8;
            body_len = count;
        } else if (or_acc <= 0xFFFFu) {
            enc = ColumnEncoding::kNarrowU16;
            body_len = static_cast<size_t>(count) * 2u;
        } else if constexpr (sizeof(F) == 8) {
            if (or_acc <= 0xFFFFFFFFULL) {
                enc = ColumnEncoding::kUnsigned4;
                body_len = static_cast<size_t>(count) * 4u;
            } else {
                enc = ColumnEncoding::kNative;
                body_len = static_cast<size_t>(count) * 8u;
            }
        } else {
            enc = ColumnEncoding::kNative;
            body_len = static_cast<size_t>(count) * sizeof(F);
        }
    } else {
        // Signed integer (int32_t, int64_t) or enum (int32_t underlying).
        using S = std::conditional_t<std::is_enum_v<F>, int32_t, F>;
        const S first = static_cast<S>(data[0].*member);
        S min_v = first;
        S max_v = first;
        for (uint32_t i = 1; i < count; ++i) {
            const S v = static_cast<S>(data[i].*member);
            if (v < min_v) min_v = v;
            if (v > max_v) max_v = v;
        }
        if (min_v == 0 && max_v == 0) {
            enc = ColumnEncoding::kOmit;
        } else if (min_v == max_v) {
            enc = ColumnEncoding::kConst;
            body_len = Varint32Size(count) + sizeof(S);
        } else if (min_v >= -128 && max_v <= 127) {
            enc = ColumnEncoding::kNarrowI8;
            body_len = count;
        } else if (min_v >= 0 && max_v <= 255) {
            enc = ColumnEncoding::kNarrowU8;
            body_len = count;
        } else if (min_v >= -32768 && max_v <= 32767) {
            enc = ColumnEncoding::kNarrowI16;
            body_len = static_cast<size_t>(count) * 2u;
        } else if (min_v >= 0 && max_v <= 65535) {
            enc = ColumnEncoding::kNarrowU16;
            body_len = static_cast<size_t>(count) * 2u;
        } else if constexpr (sizeof(S) == 8) {
            if (min_v >= std::numeric_limits<int32_t>::min() &&
                max_v <= std::numeric_limits<int32_t>::max()) {
                enc = ColumnEncoding::kSigned4;
                body_len = static_cast<size_t>(count) * 4u;
            } else if (min_v >= 0 && max_v <= 0xFFFFFFFFLL) {
                enc = ColumnEncoding::kUnsigned4;
                body_len = static_cast<size_t>(count) * 4u;
            } else {
                enc = ColumnEncoding::kNative;
                body_len = static_cast<size_t>(count) * 8u;
            }
        } else {
            enc = ColumnEncoding::kNative;
            body_len = static_cast<size_t>(count) * sizeof(S);
        }
    }

    tape.Set(slot, static_cast<uint32_t>(enc));
    if (enc == ColumnEncoding::kOmit) {
        return 0;
    }
    return kHdr + Varint32Size(static_cast<uint32_t>(body_len)) + body_len;
}

template <uint32_t FieldNumber, typename M, typename F>
inline uint8_t* EncodeScalarColumnUnchecked(uint8_t* dst, const M* data,
                                            uint32_t count, F M::* member,
                                            const uint32_t*& tape) noexcept {
    const auto enc = static_cast<ColumnEncoding>(*tape++);
    if (enc == ColumnEncoding::kOmit) {
        return dst;
    }
    if (enc == ColumnEncoding::kConst) {
        constexpr SeqKind kConstKind = ConstSeqKindFor<F>();
        dst = WriteConstSeqHeader<FieldNumber, kConstKind>(dst);
        if constexpr (std::is_same_v<F, bool>) {
            dst = WriteVarint32Raw(dst, Varint32Size(count) + 1u);
            dst = WriteVarint32Raw(dst, count);
            *dst++ = (data[0].*member) ? 1 : 0;
        } else if constexpr (std::is_enum_v<F>) {
            dst = WriteVarint32Raw(dst, Varint32Size(count) + 4u);
            dst = WriteVarint32Raw(dst, count);
            dst = WriteFixedRaw<int32_t>(dst,
                                         static_cast<int32_t>(data[0].*member));
        } else {
            dst = WriteVarint32Raw(
                dst, Varint32Size(count) + static_cast<uint32_t>(sizeof(F)));
            dst = WriteVarint32Raw(dst, count);
            dst = WriteFixedRaw<F>(dst, data[0].*member);
        }
        return dst;
    }

    if constexpr (std::is_same_v<F, bool>) {
        dst = WriteConstSeqHeader<FieldNumber, SeqKind::kBitBool>(dst);
        const uint32_t byte_cnt = (count + 7u) >> 3;
        dst = WriteVarint32Raw(dst, Varint32Size(count) + byte_cnt);
        dst = WriteVarint32Raw(dst, count);
        uint32_t i = 0;
        for (; i + 8 <= count; i += 8) {
            const uint8_t byte = static_cast<uint8_t>(
                ((data[i].*member) ? 1u : 0u) |
                (((data[i + 1].*member) ? 1u : 0u) << 1) |
                (((data[i + 2].*member) ? 1u : 0u) << 2) |
                (((data[i + 3].*member) ? 1u : 0u) << 3) |
                (((data[i + 4].*member) ? 1u : 0u) << 4) |
                (((data[i + 5].*member) ? 1u : 0u) << 5) |
                (((data[i + 6].*member) ? 1u : 0u) << 6) |
                (((data[i + 7].*member) ? 1u : 0u) << 7));
            *dst++ = byte;
        }
        if (i < count) {
            uint8_t byte = 0;
            for (uint32_t bit = 0; i < count; ++i, ++bit) {
                if (data[i].*member) byte |= static_cast<uint8_t>(1u << bit);
            }
            *dst++ = byte;
        }
        return dst;
    } else if constexpr (std::is_same_v<F, float> ||
                         std::is_same_v<F, double>) {
        dst = WriteConstSeqHeader<FieldNumber, NativeSeqKindFor<F>()>(dst);
        dst = WriteVarint32Raw(dst, count * static_cast<uint32_t>(sizeof(F)));
        for (uint32_t i = 0; i < count; ++i) {
            dst = WriteFixedRaw<F>(dst, data[i].*member);
        }
        return dst;
    } else {
        switch (enc) {
            case ColumnEncoding::kNarrowU8:
                dst = WriteConstSeqHeader<FieldNumber, SeqKind::kNarrowU8>(dst);
                dst = WriteVarint32Raw(dst, count);
                for (uint32_t i = 0; i < count; ++i) {
                    *dst++ = static_cast<uint8_t>(data[i].*member);
                }
                return dst;
            case ColumnEncoding::kNarrowI8:
                dst = WriteConstSeqHeader<FieldNumber, SeqKind::kNarrowI8>(dst);
                dst = WriteVarint32Raw(dst, count);
                for (uint32_t i = 0; i < count; ++i) {
                    *dst++ = static_cast<uint8_t>(
                        static_cast<int8_t>(data[i].*member));
                }
                return dst;
            case ColumnEncoding::kNarrowU16:
                dst =
                    WriteConstSeqHeader<FieldNumber, SeqKind::kNarrowU16>(dst);
                dst = WriteVarint32Raw(dst, count * 2u);
                for (uint32_t i = 0; i < count; ++i) {
                    dst = WriteFixedRaw<uint16_t>(
                        dst, static_cast<uint16_t>(data[i].*member));
                }
                return dst;
            case ColumnEncoding::kNarrowI16:
                dst =
                    WriteConstSeqHeader<FieldNumber, SeqKind::kNarrowI16>(dst);
                dst = WriteVarint32Raw(dst, count * 2u);
                for (uint32_t i = 0; i < count; ++i) {
                    dst = WriteFixedRaw<int16_t>(
                        dst, static_cast<int16_t>(data[i].*member));
                }
                return dst;
            case ColumnEncoding::kSigned4:
                dst = WriteConstSeqHeader<FieldNumber, SeqKind::kSigned4>(dst);
                dst = WriteVarint32Raw(dst, count * 4u);
                for (uint32_t i = 0; i < count; ++i) {
                    dst = WriteFixedRaw<int32_t>(
                        dst, static_cast<int32_t>(data[i].*member));
                }
                return dst;
            case ColumnEncoding::kUnsigned4:
                dst =
                    WriteConstSeqHeader<FieldNumber, SeqKind::kUnsigned4>(dst);
                dst = WriteVarint32Raw(dst, count * 4u);
                for (uint32_t i = 0; i < count; ++i) {
                    dst = WriteFixedRaw<uint32_t>(
                        dst, static_cast<uint32_t>(data[i].*member));
                }
                return dst;
            default: {
                dst = WriteConstSeqHeader<FieldNumber, NativeSeqKindFor<F>()>(
                    dst);
                using RawT = std::conditional_t<std::is_enum_v<F>, int32_t, F>;
                dst = WriteVarint32Raw(
                    dst, count * static_cast<uint32_t>(sizeof(RawT)));
                for (uint32_t i = 0; i < count; ++i) {
                    dst = WriteFixedRaw<RawT>(
                        dst, static_cast<RawT>(data[i].*member));
                }
                return dst;
            }
        }
    }
}

// Generic scalar column reader invoking `emit(uint32_t index, ScalarT value)`
// for each of the `count` elements in the column. Validates exact element
// count. Returns:
// - kDecoded: read all `count` elements cleanly
// - kIncompatible: wire type or SeqKind incompatible with `ScalarT` (caller
//   must skip remaining seq body if `wt == kSeq` or skip field if non-kSeq)
// - kError: malformed/truncated payload or mismatched element count
template <typename ScalarT, typename EmitFn>
inline CompatStatus ReadScalarColumn(FastReader& r, SeqKind kind,
                                     uint32_t count, EmitFn&& emit) {
    if constexpr (kIsVarintCompatScalar<ScalarT>) {
        if (IsVarintPackedSeqKind(kind)) {
            const size_t elem_sz = PackedSeqElemBytes(kind);
            const uint64_t expected_bytes =
                static_cast<uint64_t>(count) * elem_sz;
            uint32_t body_len = 0;
            const uint8_t* span = nullptr;
            const SpanReadStatus st = r.TryReadContiguousSpan(body_len, span);
            if (st == SpanReadStatus::kError) return CompatStatus::kError;
            if (body_len != expected_bytes) return CompatStatus::kError;
            if (st == SpanReadStatus::kContiguous) [[likely]] {
                const uint8_t* p = span;
                for (uint32_t i = 0; i < count; ++i) {
                    emit(i, ConvertVarintFromContiguous<ScalarT>(p, kind));
                }
                return CompatStatus::kDecoded;
            }
            FastReader col_r(FastReader::kUninitialized);
            if (!r.ReadSubReaderSlow(body_len, col_r, /*check_depth=*/false)) {
                return CompatStatus::kError;
            }
            for (uint32_t i = 0; i < count; ++i) {
                ScalarT v{};
                if (!ConvertVarintElemFromSeq<ScalarT>(col_r, kind, v)) {
                    return CompatStatus::kError;
                }
                emit(i, v);
            }
            return CompatStatus::kDecoded;
        }
        if (kind == SeqKind::kBitBool) {
            uint32_t body_len = 0;
            const uint8_t* span = nullptr;
            const SpanReadStatus st = r.TryReadContiguousSpan(body_len, span);
            if (st == SpanReadStatus::kError) return CompatStatus::kError;
            if (st == SpanReadStatus::kContiguous) [[likely]] {
                const uint8_t* p = span;
                const uint8_t* pend = span + body_len;
                uint32_t n = 0;
                if (!ReadVarint32Contiguous(p, pend, n) || n != count) {
                    return CompatStatus::kError;
                }
                const uint64_t expected_bytes =
                    (static_cast<uint64_t>(count) + 7u) >> 3;
                if (static_cast<size_t>(pend - p) != expected_bytes) {
                    return CompatStatus::kError;
                }
                uint32_t i = 0;
                for (; i + 8 <= count; i += 8) {
                    const uint8_t byte = *p++;
                    for (uint32_t b = 0; b < 8; ++b) {
                        emit(i + b, CastFromUnsigned64<ScalarT>(
                                        ((byte >> b) & 1u) ? 1ULL : 0ULL));
                    }
                }
                if (i < count) {
                    const uint8_t byte = *p++;
                    for (uint32_t b = 0; i < count; ++i, ++b) {
                        emit(i, CastFromUnsigned64<ScalarT>(
                                    ((byte >> b) & 1u) ? 1ULL : 0ULL));
                    }
                }
                return CompatStatus::kDecoded;
            }
            FastReader col_r(FastReader::kUninitialized);
            if (!r.ReadSubReaderSlow(body_len, col_r, /*check_depth=*/false)) {
                return CompatStatus::kError;
            }
            uint32_t n = 0;
            if (!col_r.ReadVarint32(n) || n != count) {
                return CompatStatus::kError;
            }
            const uint64_t expected_bytes =
                (static_cast<uint64_t>(count) + 7u) >> 3;
            if (col_r.Remaining() != expected_bytes) {
                return CompatStatus::kError;
            }
            uint32_t i = 0;
            for (; i + 8 <= count; i += 8) {
                uint8_t byte = 0;
                if (!col_r.ReadU8(byte)) return CompatStatus::kError;
                for (uint32_t b = 0; b < 8; ++b) {
                    emit(i + b, CastFromUnsigned64<ScalarT>(
                                    ((byte >> b) & 1u) ? 1ULL : 0ULL));
                }
            }
            if (i < count) {
                uint8_t byte = 0;
                if (!col_r.ReadU8(byte)) return CompatStatus::kError;
                for (uint32_t b = 0; i < count; ++i, ++b) {
                    emit(i, CastFromUnsigned64<ScalarT>(
                                ((byte >> b) & 1u) ? 1ULL : 0ULL));
                }
            }
            return CompatStatus::kDecoded;
        }
        if (IsVarintConstSeqKind(kind)) {
            uint32_t body_len = 0;
            if (!r.ReadVarint32(body_len)) return CompatStatus::kError;
            FastReader col_r(FastReader::kUninitialized);
            if (!r.ReadSubReaderSlow(body_len, col_r, /*check_depth=*/false)) {
                return CompatStatus::kError;
            }
            uint32_t n = 0;
            if (!col_r.ReadVarint32(n) || n != count ||
                col_r.Remaining() != ConstSeqValBytes(kind)) {
                return CompatStatus::kError;
            }
            ScalarT v{};
            if (!ConvertVarintElemFromSeq<ScalarT>(col_r, kind, v)) {
                return CompatStatus::kError;
            }
            for (uint32_t i = 0; i < count; ++i) {
                emit(i, v);
            }
            return CompatStatus::kDecoded;
        }
        return CompatStatus::kIncompatible;
    } else if constexpr (std::is_same_v<ScalarT, float> ||
                         std::is_same_v<ScalarT, double>) {
        constexpr SeqKind kPacked =
            std::is_same_v<ScalarT, float> ? SeqKind::kFloat4 : SeqKind::kFloat8;
        constexpr SeqKind kConst = std::is_same_v<ScalarT, float>
                                       ? SeqKind::kConstF4
                                       : SeqKind::kConstF8;
        if (kind == kPacked) {
            const uint64_t expected_bytes =
                static_cast<uint64_t>(count) * sizeof(ScalarT);
            uint32_t body_len = 0;
            const uint8_t* span = nullptr;
            const SpanReadStatus st = r.TryReadContiguousSpan(body_len, span);
            if (st == SpanReadStatus::kError) return CompatStatus::kError;
            if (body_len != expected_bytes) return CompatStatus::kError;
            if (st == SpanReadStatus::kContiguous) [[likely]] {
                const uint8_t* p = span;
                for (uint32_t i = 0; i < count; ++i) {
                    ScalarT raw{};
                    std::memcpy(&raw, p, sizeof(ScalarT));
                    p += sizeof(ScalarT);
                    emit(i, LittleEndianToHost(raw));
                }
                return CompatStatus::kDecoded;
            }
            FastReader col_r(FastReader::kUninitialized);
            if (!r.ReadSubReaderSlow(body_len, col_r, /*check_depth=*/false)) {
                return CompatStatus::kError;
            }
            for (uint32_t i = 0; i < count; ++i) {
                ScalarT v{};
                if (!col_r.ReadFixed<ScalarT>(v)) return CompatStatus::kError;
                emit(i, v);
            }
            return CompatStatus::kDecoded;
        }
        if (kind == kConst) {
            uint32_t body_len = 0;
            if (!r.ReadVarint32(body_len)) return CompatStatus::kError;
            FastReader col_r(FastReader::kUninitialized);
            if (!r.ReadSubReaderSlow(body_len, col_r, /*check_depth=*/false)) {
                return CompatStatus::kError;
            }
            uint32_t n = 0;
            if (!col_r.ReadVarint32(n) || n != count ||
                col_r.Remaining() != sizeof(ScalarT)) {
                return CompatStatus::kError;
            }
            ScalarT v{};
            if (!col_r.ReadFixed<ScalarT>(v)) return CompatStatus::kError;
            for (uint32_t i = 0; i < count; ++i) {
                emit(i, v);
            }
            return CompatStatus::kDecoded;
        }
        return CompatStatus::kIncompatible;
    } else {
        (void)r;
        (void)kind;
        (void)count;
        (void)emit;
        return CompatStatus::kIncompatible;
    }
}

template <typename T>
inline constexpr bool kIsFixedScalarField =
    kIsVarintCompatScalar<T> || std::is_same_v<T, float> ||
    std::is_same_v<T, double>;

// Decodes one column inside a kColumnar payload into `vec[base..base+count-1]`
// for regular member `M::*member`. Handles exact scalars, cross-width scalar
// evolution, scalar -> optional<scalar>, scalar -> repeated<scalar>, and
// skips incompatible columns cleanly.
template <typename M, typename FieldT>
inline bool DecodeColumnIntoField(FastReader& r, WireType wt,
                                  std::vector<M>& vec, size_t base,
                                  uint32_t count, FieldT M::* member,
                                  uint64_t& seen_cols, int col_bit) {
    if (wt != WireType::kSeq) {
        return r.SkipUnknownField(wt);
    }
    uint8_t raw_kind = 0;
    if (!r.ReadU8(raw_kind)) return false;
    const SeqKind kind = static_cast<SeqKind>(raw_kind);
    M* dst = vec.data() + base;

    if constexpr (kIsFixedScalarField<FieldT>) {
        const CompatStatus st = ReadScalarColumn<FieldT>(
            r, kind, count,
            [dst, member](uint32_t i, FieldT v) { dst[i].*member = v; });
        if (st == CompatStatus::kDecoded) {
            if (col_bit >= 0) seen_cols |= (uint64_t{1} << col_bit);
            return true;
        }
        return st == CompatStatus::kIncompatible ? r.SkipSeqBody() : false;
    } else if constexpr (IsOptionalLike<FieldT>::value) {
        using U = typename FieldT::value_type;
        if constexpr (kIsFixedScalarField<U>) {
            const CompatStatus st = ReadScalarColumn<U>(
                r, kind, count, [dst, member](uint32_t i, U v) {
                    if (IsProto3Default(v)) {
                        (dst[i].*member).reset();
                    } else {
                        dst[i].*member = v;
                    }
                });
            if (st == CompatStatus::kDecoded) {
                if (col_bit >= 0) seen_cols |= (uint64_t{1} << col_bit);
                return true;
            }
            return st == CompatStatus::kIncompatible ? r.SkipSeqBody() : false;
        } else {
            return r.SkipSeqBody();
        }
    } else if constexpr (IsVector<FieldT>::value) {
        using E = typename FieldT::value_type;
        if constexpr (kIsFixedScalarField<E>) {
            const CompatStatus st = ReadScalarColumn<E>(
                r, kind, count, [dst, member](uint32_t i, E v) {
                    auto& vref = dst[i].*member;
                    vref.clear();
                    if (!IsProto3Default(v)) {
                        vref.push_back(v);
                    }
                });
            if (st == CompatStatus::kDecoded) {
                if (col_bit >= 0) seen_cols |= (uint64_t{1} << col_bit);
                return true;
            }
            return st == CompatStatus::kIncompatible ? r.SkipSeqBody() : false;
        } else {
            return r.SkipSeqBody();
        }
    } else {
        return r.SkipSeqBody();
    }
}

template <size_t VariantIdx, typename M, typename VariantT>
inline bool DecodeColumnIntoOneof(FastReader& r, WireType wt,
                                  std::vector<M>& vec, size_t base,
                                  uint32_t count, VariantT M::* oneof_member) {
    if (wt != WireType::kSeq) {
        return r.SkipUnknownField(wt);
    }
    uint8_t raw_kind = 0;
    if (!r.ReadU8(raw_kind)) return false;
    const SeqKind kind = static_cast<SeqKind>(raw_kind);
    using BranchT = std::variant_alternative_t<VariantIdx, VariantT>;
    if constexpr (kIsFixedScalarField<BranchT>) {
        M* dst = vec.data() + base;
        const CompatStatus st = ReadScalarColumn<BranchT>(
            r, kind, count, [dst, oneof_member](uint32_t i, BranchT v) {
                if (!IsProto3Default(v)) {
                    (dst[i].*oneof_member).template emplace<VariantIdx>(v);
                }
            });
        if (st == CompatStatus::kDecoded) return true;
        return st == CompatStatus::kIncompatible ? r.SkipSeqBody() : false;
    } else {
        return r.SkipSeqBody();
    }
}

// Decodes one column inside a kColumnar payload into a singular message field
// using Proto3 merge semantics (last non-default element wins).
template <typename FieldT>
inline bool DecodeColumnIntoSingularField(FastReader& r, WireType wt,
                                          uint32_t count, FieldT& field) {
    if (wt != WireType::kSeq) {
        return r.SkipUnknownField(wt);
    }
    uint8_t raw_kind = 0;
    if (!r.ReadU8(raw_kind)) return false;
    const SeqKind kind = static_cast<SeqKind>(raw_kind);

    if constexpr (kIsFixedScalarField<FieldT>) {
        bool found = false;
        FieldT last{};
        const CompatStatus st = ReadScalarColumn<FieldT>(
            r, kind, count, [&](uint32_t, FieldT v) {
                if (!IsProto3Default(v)) {
                    last = v;
                    found = true;
                }
            });
        if (st == CompatStatus::kDecoded) {
            if (found) field = last;
            return true;
        }
        return st == CompatStatus::kIncompatible ? r.SkipSeqBody() : false;
    } else if constexpr (IsOptionalLike<FieldT>::value) {
        using U = typename FieldT::value_type;
        if constexpr (kIsFixedScalarField<U>) {
            bool found = false;
            U last{};
            const CompatStatus st =
                ReadScalarColumn<U>(r, kind, count, [&](uint32_t, U v) {
                    if (!IsProto3Default(v)) {
                        last = v;
                        found = true;
                    }
                });
            if (st == CompatStatus::kDecoded) {
                if (found) field = last;
                return true;
            }
            return st == CompatStatus::kIncompatible ? r.SkipSeqBody() : false;
        } else {
            return r.SkipSeqBody();
        }
    } else if constexpr (IsVector<FieldT>::value) {
        using E = typename FieldT::value_type;
        if constexpr (kIsFixedScalarField<E>) {
            const CompatStatus st =
                ReadScalarColumn<E>(r, kind, count, [&](uint32_t, E v) {
                    if (!IsProto3Default(v)) {
                        field.push_back(v);
                    }
                });
            if (st == CompatStatus::kDecoded) return true;
            return st == CompatStatus::kIncompatible ? r.SkipSeqBody() : false;
        } else {
            return r.SkipSeqBody();
        }
    } else {
        return r.SkipSeqBody();
    }
}

template <size_t VariantIdx, typename VariantT>
inline bool DecodeColumnIntoSingularOneof(FastReader& r, WireType wt,
                                          uint32_t count, VariantT& oneof) {
    if (wt != WireType::kSeq) {
        return r.SkipUnknownField(wt);
    }
    uint8_t raw_kind = 0;
    if (!r.ReadU8(raw_kind)) return false;
    const SeqKind kind = static_cast<SeqKind>(raw_kind);
    using BranchT = std::variant_alternative_t<VariantIdx, VariantT>;
    if constexpr (kIsFixedScalarField<BranchT>) {
        bool found = false;
        BranchT last{};
        const CompatStatus st =
            ReadScalarColumn<BranchT>(r, kind, count, [&](uint32_t, BranchT v) {
                if (!IsProto3Default(v)) {
                    last = v;
                    found = true;
                }
            });
        if (st == CompatStatus::kDecoded) {
            if (found) oneof.template emplace<VariantIdx>(last);
            return true;
        }
        return st == CompatStatus::kIncompatible ? r.SkipSeqBody() : false;
    } else {
        return r.SkipSeqBody();
    }
}

}  // namespace brpc_fast::detail
