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

// ---- IndirectBox unwrapping & WireType / SeqKind classification ----

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

template <typename T>
inline constexpr bool kIsFixedScalarField =
    kIsVarintCompatScalar<T> || std::is_same_v<T, float> ||
    std::is_same_v<T, double>;

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

template <typename ScalarT>
inline constexpr bool IsCompatibleScalarSeqKind(SeqKind kind) noexcept {
    if constexpr (kIsVarintCompatScalar<ScalarT>) {
        return IsVarintPackedSeqKind(kind) || kind == SeqKind::kBitBool ||
               IsVarintConstSeqKind(kind);
    } else if constexpr (std::is_same_v<ScalarT, float>) {
        return kind == SeqKind::kFloat4 || kind == SeqKind::kConstF4;
    } else if constexpr (std::is_same_v<ScalarT, double>) {
        return kind == SeqKind::kFloat8 || kind == SeqKind::kConstF8;
    } else {
        return false;
    }
}

// ---- Exact Proto3 scalar conversion (preserves sender signedness!) ----

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
inline bool ConvertVarintElemFromSeqWithZero(FastReader& r, SeqKind kind,
                                             T& out,
                                             bool& wire_is_zero) noexcept {
    static_assert(kIsVarintCompatScalar<T>);
    switch (kind) {
        case SeqKind::kBool:
        case SeqKind::kConstBool: {
            uint8_t b = 0;
            if (!r.ReadU8(b)) return false;
            wire_is_zero = (b == 0);
            out = CastFromUnsigned64<T>(b != 0 ? 1ULL : 0ULL);
            return true;
        }
        case SeqKind::kNarrowU8: {
            uint8_t v = 0;
            if (!r.ReadU8(v)) return false;
            wire_is_zero = (v == 0);
            out = CastFromUnsigned64<T>(static_cast<uint64_t>(v));
            return true;
        }
        case SeqKind::kNarrowI8: {
            uint8_t raw = 0;
            if (!r.ReadU8(raw)) return false;
            wire_is_zero = (raw == 0);
            out = CastFromSigned64<T>(
                static_cast<int64_t>(static_cast<int8_t>(raw)));
            return true;
        }
        case SeqKind::kNarrowU16: {
            uint16_t v = 0;
            if (!r.ReadFixed<uint16_t>(v)) return false;
            wire_is_zero = (v == 0);
            out = CastFromUnsigned64<T>(static_cast<uint64_t>(v));
            return true;
        }
        case SeqKind::kNarrowI16: {
            int16_t v = 0;
            if (!r.ReadFixed<int16_t>(v)) return false;
            wire_is_zero = (v == 0);
            out = CastFromSigned64<T>(static_cast<int64_t>(v));
            return true;
        }
        case SeqKind::kSigned4:
        case SeqKind::kConstS4: {
            int32_t v = 0;
            if (!r.ReadFixed<int32_t>(v)) return false;
            wire_is_zero = (v == 0);
            out = CastFromSigned64<T>(static_cast<int64_t>(v));
            return true;
        }
        case SeqKind::kUnsigned4:
        case SeqKind::kConstU4: {
            uint32_t v = 0;
            if (!r.ReadFixed<uint32_t>(v)) return false;
            wire_is_zero = (v == 0);
            out = CastFromUnsigned64<T>(static_cast<uint64_t>(v));
            return true;
        }
        case SeqKind::kInt8:
        case SeqKind::kConstI8: {
            uint64_t v = 0;
            if (!r.ReadFixed<uint64_t>(v)) return false;
            wire_is_zero = (v == 0);
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
    bool unused_zero = false;
    return ConvertVarintElemFromSeqWithZero<T>(r, kind, out, unused_zero);
}

// ---- Unified low-level packed / bit-bool / const payload helpers ----

// Invokes `emit(size_t index, ScalarT value, bool wire_is_zero)` across `count`
// contiguous elements of a varint-packed SeqKind with switch hoisted outside
// the loop.
template <typename ScalarT, typename EmitFn>
inline bool ForEachPackedVarintContiguous(const uint8_t* p, SeqKind kind,
                                          size_t count, EmitFn&& emit) {
    static_assert(kIsVarintCompatScalar<ScalarT>);
    auto run_fixed = [&]<typename WireT>() {
        for (size_t i = 0; i < count; ++i) {
            WireT raw{};
            std::memcpy(&raw, p, sizeof(WireT));
            p += sizeof(WireT);
            const WireT v = LittleEndianToHost(raw);
            if constexpr (std::is_signed_v<WireT>) {
                emit(i, CastFromSigned64<ScalarT>(static_cast<int64_t>(v)),
                     v == 0);
            } else {
                emit(i, CastFromUnsigned64<ScalarT>(static_cast<uint64_t>(v)),
                     v == 0);
            }
        }
    };
    switch (kind) {
        case SeqKind::kBool:
            for (size_t i = 0; i < count; ++i) {
                const uint8_t b = *p++;
                emit(i, CastFromUnsigned64<ScalarT>(b != 0 ? 1ULL : 0ULL),
                     b == 0);
            }
            return true;
        case SeqKind::kNarrowU8:
            run_fixed.template operator()<uint8_t>();
            return true;
        case SeqKind::kNarrowI8:
            run_fixed.template operator()<int8_t>();
            return true;
        case SeqKind::kNarrowU16:
            run_fixed.template operator()<uint16_t>();
            return true;
        case SeqKind::kNarrowI16:
            run_fixed.template operator()<int16_t>();
            return true;
        case SeqKind::kSigned4:
            run_fixed.template operator()<int32_t>();
            return true;
        case SeqKind::kUnsigned4:
            run_fixed.template operator()<uint32_t>();
            return true;
        case SeqKind::kInt8:
            run_fixed.template operator()<uint64_t>();
            return true;
        default:
            return false;
    }
}

template <typename EmitFn>
inline void ForEachBitBoolContiguous(const uint8_t* p, size_t count,
                                     EmitFn&& emit) {
    size_t i = 0;
    for (; i + 8 <= count; i += 8) {
        const uint8_t byte = *p++;
        for (uint32_t b = 0; b < 8; ++b) {
            emit(i + b, ((byte >> b) & 1u) != 0);
        }
    }
    if (i < count) {
        const uint8_t byte = *p++;
        for (uint32_t b = 0; i < count; ++i, ++b) {
            emit(i, ((byte >> b) & 1u) != 0);
        }
    }
}

template <typename EmitFn>
inline bool ForEachBitBoolReader(FastReader& r, size_t count, EmitFn&& emit) {
    size_t i = 0;
    for (; i + 8 <= count; i += 8) {
        uint8_t byte = 0;
        if (!r.ReadU8(byte)) return false;
        for (uint32_t b = 0; b < 8; ++b) {
            emit(i + b, ((byte >> b) & 1u) != 0);
        }
    }
    if (i < count) {
        uint8_t byte = 0;
        if (!r.ReadU8(byte)) return false;
        for (uint32_t b = 0; i < count; ++i, ++b) {
            emit(i, ((byte >> b) & 1u) != 0);
        }
    }
    return true;
}

// Unified reader for any scalar kConst* payload (varint-compatible or
// float/double). Returns kIncompatible if `kind` is not a compatible const
// SeqKind for `ScalarT`, kError if malformed, or kDecoded on success.
template <typename ScalarT>
inline CompatStatus ReadConstScalarPayload(FastReader& r, SeqKind kind,
                                           uint32_t& count_out,
                                           ScalarT& val_out,
                                           bool& wire_is_default_out) {
    if constexpr (kIsVarintCompatScalar<ScalarT>) {
        if (!IsVarintConstSeqKind(kind)) return CompatStatus::kIncompatible;
        uint32_t body_len = 0;
        if (!r.ReadVarint32(body_len)) return CompatStatus::kError;
        FastReader sub(FastReader::kUninitialized);
        if (!r.ReadSubReaderSlow(body_len, sub, /*check_depth=*/false)) {
            return CompatStatus::kError;
        }
        if (!sub.ReadVarint32(count_out) ||
            sub.Remaining() != ConstSeqValBytes(kind)) {
            return CompatStatus::kError;
        }
        return ConvertVarintElemFromSeqWithZero<ScalarT>(
                   sub, kind, val_out, wire_is_default_out)
                   ? CompatStatus::kDecoded
                   : CompatStatus::kError;
    } else if constexpr (std::is_same_v<ScalarT, float> ||
                         std::is_same_v<ScalarT, double>) {
        constexpr SeqKind kExpectedConst =
            std::is_same_v<ScalarT, float> ? SeqKind::kConstF4
                                           : SeqKind::kConstF8;
        if (kind != kExpectedConst) return CompatStatus::kIncompatible;
        uint32_t body_len = 0;
        if (!r.ReadVarint32(body_len)) return CompatStatus::kError;
        FastReader sub(FastReader::kUninitialized);
        if (!r.ReadSubReaderSlow(body_len, sub, /*check_depth=*/false)) {
            return CompatStatus::kError;
        }
        if (!sub.ReadVarint32(count_out) || sub.Remaining() != sizeof(ScalarT)) {
            return CompatStatus::kError;
        }
        if (!sub.ReadFixed<ScalarT>(val_out)) return CompatStatus::kError;
        wire_is_default_out = IsProto3Default(val_out);
        return CompatStatus::kDecoded;
    } else {
        (void)r;
        (void)kind;
        (void)count_out;
        (void)val_out;
        (void)wire_is_default_out;
        return CompatStatus::kIncompatible;
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
    ForEachBitBoolContiguous(
        cur, count, [&](size_t i, bool bit) { vec[base + i] = bit; });
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
    return ForEachBitBoolReader(
        sub, count, [&](size_t i, bool bit) { vec[base + i] = bit; });
}

// ---- Singular message list & columnar merge helpers ----

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
    if (!sub.ConsumeExpandedBudget(static_cast<uint64_t>(count) *
                                   sizeof(InnerT))) {
        return CompatStatus::kError;
    }
    auto [target, already_present] = prepare_target();
    return InnerT::DecodeColumnarSingular(
        sub, count, UnwrapIndirectBox<MsgT>::Ref(target), already_present);
}

// ---- Slow-path Proto3 schema-evolution decoders ----

template <typename U>
BRPC_FAST_NOINLINE inline CompatStatus DecodeSingularFromSeq(
    FastReader& r, uint8_t raw_kind, U& target, bool already_present) {
    const SeqKind kind = static_cast<SeqKind>(raw_kind);
    if constexpr (kIsFixedScalarField<U>) {
        // Handle kConst* uniformly for both varint-compatible and float/double.
        uint32_t count = 0;
        U val{};
        bool unused_def = false;
        const CompatStatus cst =
            ReadConstScalarPayload<U>(r, kind, count, val, unused_def);
        if (cst != CompatStatus::kIncompatible) {
            if (cst == CompatStatus::kError) return CompatStatus::kError;
            if (count == 0) return CompatStatus::kEmpty;
            target = val;
            return CompatStatus::kDecoded;
        }
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
                uint32_t count_bits = 0;
                if (!sub.ReadVarint32(count_bits)) return CompatStatus::kError;
                const uint64_t expected_bytes =
                    (static_cast<uint64_t>(count_bits) + 7u) >> 3;
                if (sub.Remaining() != expected_bytes) {
                    return CompatStatus::kError;
                }
                if (count_bits == 0) return CompatStatus::kEmpty;
                if (expected_bytes > 1 && !sub.Skip(expected_bytes - 1)) {
                    return CompatStatus::kError;
                }
                uint8_t last_byte = 0;
                if (!sub.ReadU8(last_byte)) return CompatStatus::kError;
                const uint32_t bit = (count_bits - 1u) & 7u;
                target = CastFromUnsigned64<U>(((last_byte >> bit) & 1u) ? 1ULL
                                                                         : 0ULL);
                return CompatStatus::kDecoded;
            }
        } else {
            constexpr SeqKind kExpectedPacked =
                std::is_same_v<U, float> ? SeqKind::kFloat4 : SeqKind::kFloat8;
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

template <bool Overwrite, typename E>
BRPC_FAST_NOINLINE inline CompatStatus DecodeRepeatedFromSeqCompat(
    FastReader& r, uint8_t raw_kind, std::vector<E>& vec, bool first) {
    const SeqKind kind = static_cast<SeqKind>(raw_kind);
    const bool reset_existing = Overwrite && first;
    if constexpr (kIsFixedScalarField<E>) {
        // Handle kConst* uniformly for both varint-compatible and float/double.
        uint32_t const_count = 0;
        E elem{};
        bool unused_def = false;
        const CompatStatus cst =
            ReadConstScalarPayload<E>(r, kind, const_count, elem, unused_def);
        if (cst != CompatStatus::kIncompatible) {
            if (cst == CompatStatus::kError) return CompatStatus::kError;
            constexpr size_t kElemBytes =
                std::is_same_v<E, bool> ? 1u : sizeof(E);
            if (!r.ConsumeExpandedBudget(static_cast<uint64_t>(const_count) *
                                         kElemBytes)) {
                return CompatStatus::kError;
            }
            if (reset_existing) vec.clear();
            const size_t base = vec.size();
            vec.resize(base + const_count);
            std::fill_n(vec.begin() + static_cast<std::ptrdiff_t>(base),
                        const_count, elem);
            return CompatStatus::kDecoded;
        }
        if constexpr (kIsVarintCompatScalar<E>) {
            if (IsVarintPackedSeqKind(kind)) {
                uint32_t byte_len = 0;
                const uint8_t* span = nullptr;
                const SpanReadStatus st =
                    r.TryReadContiguousSpan(byte_len, span);
                if (st == SpanReadStatus::kError) return CompatStatus::kError;
                const size_t elem_sz = PackedSeqElemBytes(kind);
                if ((byte_len % elem_sz) != 0) return CompatStatus::kError;
                if (st == SpanReadStatus::kStraddling &&
                    r.Remaining() < byte_len) {
                    return CompatStatus::kError;
                }
                const size_t count = byte_len / elem_sz;
                constexpr size_t kDstElemBytes =
                    std::is_same_v<E, bool> ? 1u : sizeof(E);
                if (kDstElemBytes > elem_sz &&
                    !r.ConsumeExpandedBudget(static_cast<uint64_t>(count) *
                                             kDstElemBytes)) {
                    return CompatStatus::kError;
                }
                if (reset_existing) vec.clear();
                const size_t base = vec.size();
                vec.resize(base + count);
                if (st == SpanReadStatus::kContiguous) [[likely]] {
                    return ForEachPackedVarintContiguous<E>(
                               span, kind, count,
                               [&](size_t i, E v, bool) { vec[base + i] = v; })
                               ? CompatStatus::kDecoded
                               : CompatStatus::kError;
                }
                for (size_t i = 0; i < count; ++i) {
                    E v{};
                    if (!ConvertVarintElemFromSeq<E>(r, kind, v)) {
                        return CompatStatus::kError;
                    }
                    vec[base + i] = v;
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
                if (sub.Remaining() != expected_bytes) {
                    return CompatStatus::kError;
                }
                constexpr size_t kElemBytes =
                    std::is_same_v<E, bool> ? 1u : sizeof(E);
                if (!sub.ConsumeExpandedBudget(static_cast<uint64_t>(count) *
                                               kElemBytes)) {
                    return CompatStatus::kError;
                }
                if (reset_existing) vec.clear();
                const size_t base = vec.size();
                vec.resize(base + count);
                return ForEachBitBoolReader(
                           sub, count,
                           [&](size_t i, bool bit) {
                               vec[base + i] =
                                   CastFromUnsigned64<E>(bit ? 1ULL : 0ULL);
                           })
                           ? CompatStatus::kDecoded
                           : CompatStatus::kError;
            }
        }
        return CompatStatus::kIncompatible;
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

// ---- Columnar (SoA) encoding helpers ----

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

template <uint32_t FieldNumber, typename M, typename F>
inline size_t SizeScalarColumn(const M* data, uint32_t count, F M::* member,
                               SizeTape& tape) {
    const size_t slot = tape.Reserve();
    constexpr size_t kHdr = SeqHeaderSize(FieldNumber);
    ColumnEncoding enc = ColumnEncoding::kNative;
    size_t body_len = 0;

    if constexpr (std::is_same_v<F, bool>) {
        const bool first = data[0].*member;
        const bool all_same =
            std::all_of(data + 1, data + count,
                        [first, member](const M& m) { return m.*member == first; });
        if (all_same) {
            if (!first) {
                enc = ColumnEncoding::kOmit;
            } else {
                enc = ColumnEncoding::kConst;
                body_len = Varint32Size(count) + 1u;
            }
        } else {
            enc = ColumnEncoding::kNative;
            body_len =
                Varint32Size(count) + ((static_cast<size_t>(count) + 7u) >> 3);
        }
    } else if constexpr (std::is_same_v<F, float> ||
                         std::is_same_v<F, double>) {
        using BitsT =
            std::conditional_t<std::is_same_v<F, float>, uint32_t, uint64_t>;
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
        using S = std::conditional_t<std::is_enum_v<F>, int32_t, F>;
        const S first = static_cast<S>(data[0].*member);
        S min_v = first;
        S max_v = first;
        for (uint32_t i = 1; i < count; ++i) {
            const S v = static_cast<S>(data[i].*member);
            min_v = std::min(min_v, v);
            max_v = std::max(max_v, v);
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
        using RawT = std::conditional_t<
            std::is_same_v<F, bool>, uint8_t,
            std::conditional_t<std::is_enum_v<F>, int32_t, F>>;
        dst = WriteVarint32Raw(
            dst, Varint32Size(count) + static_cast<uint32_t>(sizeof(RawT)));
        dst = WriteVarint32Raw(dst, count);
        if constexpr (std::is_same_v<F, bool>) {
            *dst++ = (data[0].*member) ? 1 : 0;
        } else {
            dst = WriteFixedRaw<RawT>(dst, static_cast<RawT>(data[0].*member));
        }
        return dst;
    }

    if constexpr (std::is_same_v<F, bool>) {
        dst = WriteConstSeqHeader<FieldNumber, SeqKind::kBitBool>(dst);
        const size_t count_sz = count;
        const size_t byte_cnt = (count_sz + 7u) >> 3;
        dst = WriteVarint32Raw(
            dst, static_cast<uint32_t>(Varint32Size(count) + byte_cnt));
        dst = WriteVarint32Raw(dst, count);
        size_t i = 0;
        for (; i + 8 <= count_sz; i += 8) {
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
        if (i < count_sz) {
            uint8_t byte = 0;
            for (uint32_t bit = 0; i < count_sz; ++i, ++bit) {
                if (data[i].*member) byte |= static_cast<uint8_t>(1u << bit);
            }
            *dst++ = byte;
        }
        return dst;
    } else {
        auto emit_fixed = [&]<SeqKind Kind, typename WireT>() -> uint8_t* {
            dst = WriteConstSeqHeader<FieldNumber, Kind>(dst);
            dst = WriteVarint32Raw(
                dst, count * static_cast<uint32_t>(sizeof(WireT)));
            for (uint32_t i = 0; i < count; ++i) {
                dst = WriteFixedRaw<WireT>(dst,
                                           static_cast<WireT>(data[i].*member));
            }
            return dst;
        };
        if constexpr (std::is_same_v<F, float> || std::is_same_v<F, double>) {
            return emit_fixed.template operator()<NativeSeqKindFor<F>(), F>();
        } else {
            switch (enc) {
                case ColumnEncoding::kNarrowU8:
                    return emit_fixed
                        .template operator()<SeqKind::kNarrowU8, uint8_t>();
                case ColumnEncoding::kNarrowI8:
                    return emit_fixed
                        .template operator()<SeqKind::kNarrowI8, int8_t>();
                case ColumnEncoding::kNarrowU16:
                    return emit_fixed
                        .template operator()<SeqKind::kNarrowU16, uint16_t>();
                case ColumnEncoding::kNarrowI16:
                    return emit_fixed
                        .template operator()<SeqKind::kNarrowI16, int16_t>();
                case ColumnEncoding::kSigned4:
                    return emit_fixed
                        .template operator()<SeqKind::kSigned4, int32_t>();
                case ColumnEncoding::kUnsigned4:
                    return emit_fixed
                        .template operator()<SeqKind::kUnsigned4, uint32_t>();
                default: {
                    using RawT =
                        std::conditional_t<std::is_enum_v<F>, int32_t, F>;
                    return emit_fixed
                        .template operator()<NativeSeqKindFor<F>(), RawT>();
                }
            }
        }
    }
}

// ---- Columnar (SoA) decoding helpers ----

// Invokes `emit(uint32_t index, ScalarT value, bool wire_is_default)` for each
// of the `count` elements. `wire_is_default` is evaluated on the pre-narrowed
// wire value so narrowing conversions (e.g. uint64(1ULL<<32) -> uint32(0))
// retain exact Proto3 row-wise presence semantics.
template <typename ScalarT, typename EmitFn>
inline CompatStatus ReadScalarColumn(FastReader& r, SeqKind kind,
                                     uint32_t count, EmitFn&& emit) {
    if constexpr (!kIsFixedScalarField<ScalarT>) {
        (void)r;
        (void)kind;
        (void)count;
        (void)emit;
        return CompatStatus::kIncompatible;
    } else {
        uint32_t const_n = 0;
        ScalarT const_val{};
        bool const_def = false;
        const CompatStatus cst = ReadConstScalarPayload<ScalarT>(
            r, kind, const_n, const_val, const_def);
        if (cst != CompatStatus::kIncompatible) {
            if (cst == CompatStatus::kError || const_n != count) {
                return CompatStatus::kError;
            }
            for (uint32_t i = 0; i < count; ++i) {
                emit(i, const_val, const_def);
            }
            return CompatStatus::kDecoded;
        }

        if constexpr (kIsVarintCompatScalar<ScalarT>) {
            if (IsVarintPackedSeqKind(kind)) {
                const uint64_t expected_bytes =
                    static_cast<uint64_t>(count) * PackedSeqElemBytes(kind);
                uint32_t body_len = 0;
                const uint8_t* span = nullptr;
                const SpanReadStatus st =
                    r.TryReadContiguousSpan(body_len, span);
                if (st == SpanReadStatus::kError ||
                    body_len != expected_bytes) {
                    return CompatStatus::kError;
                }
                if (st == SpanReadStatus::kContiguous) [[likely]] {
                    return ForEachPackedVarintContiguous<ScalarT>(
                               span, kind, count,
                               [&](size_t i, ScalarT v, bool wire_zero) {
                                   emit(static_cast<uint32_t>(i), v, wire_zero);
                               })
                               ? CompatStatus::kDecoded
                               : CompatStatus::kError;
                }
                FastReader col_r(FastReader::kUninitialized);
                if (!r.ReadSubReaderSlow(body_len, col_r,
                                         /*check_depth=*/false)) {
                    return CompatStatus::kError;
                }
                for (uint32_t i = 0; i < count; ++i) {
                    ScalarT v{};
                    bool wire_zero = false;
                    if (!ConvertVarintElemFromSeqWithZero<ScalarT>(
                            col_r, kind, v, wire_zero)) {
                        return CompatStatus::kError;
                    }
                    emit(i, v, wire_zero);
                }
                return CompatStatus::kDecoded;
            }
            if (kind == SeqKind::kBitBool) {
                uint32_t body_len = 0;
                const uint8_t* span = nullptr;
                const SpanReadStatus st =
                    r.TryReadContiguousSpan(body_len, span);
                if (st == SpanReadStatus::kError) return CompatStatus::kError;
                const uint64_t expected_bytes =
                    (static_cast<uint64_t>(count) + 7u) >> 3;
                if (st == SpanReadStatus::kContiguous) [[likely]] {
                    const uint8_t* p = span;
                    const uint8_t* pend = span + body_len;
                    uint32_t n = 0;
                    if (!ReadVarint32Contiguous(p, pend, n) || n != count ||
                        static_cast<size_t>(pend - p) != expected_bytes) {
                        return CompatStatus::kError;
                    }
                    ForEachBitBoolContiguous(p, count, [&](size_t i, bool bit) {
                        emit(static_cast<uint32_t>(i),
                             CastFromUnsigned64<ScalarT>(bit ? 1ULL : 0ULL),
                             !bit);
                    });
                    return CompatStatus::kDecoded;
                }
                FastReader col_r(FastReader::kUninitialized);
                if (!r.ReadSubReaderSlow(body_len, col_r,
                                         /*check_depth=*/false)) {
                    return CompatStatus::kError;
                }
                uint32_t n = 0;
                if (!col_r.ReadVarint32(n) || n != count ||
                    col_r.Remaining() != expected_bytes) {
                    return CompatStatus::kError;
                }
                return ForEachBitBoolReader(
                           col_r, count,
                           [&](size_t i, bool bit) {
                               emit(static_cast<uint32_t>(i),
                                    CastFromUnsigned64<ScalarT>(bit ? 1ULL
                                                                    : 0ULL),
                                    !bit);
                           })
                           ? CompatStatus::kDecoded
                           : CompatStatus::kError;
            }
            return CompatStatus::kIncompatible;
        } else {
            constexpr SeqKind kPacked = std::is_same_v<ScalarT, float>
                                            ? SeqKind::kFloat4
                                            : SeqKind::kFloat8;
            if (kind != kPacked) return CompatStatus::kIncompatible;
            const uint64_t expected_bytes =
                static_cast<uint64_t>(count) * sizeof(ScalarT);
            uint32_t body_len = 0;
            const uint8_t* span = nullptr;
            const SpanReadStatus st = r.TryReadContiguousSpan(body_len, span);
            if (st == SpanReadStatus::kError || body_len != expected_bytes) {
                return CompatStatus::kError;
            }
            if (st == SpanReadStatus::kContiguous) [[likely]] {
                const uint8_t* p = span;
                for (uint32_t i = 0; i < count; ++i) {
                    ScalarT raw{};
                    std::memcpy(&raw, p, sizeof(ScalarT));
                    p += sizeof(ScalarT);
                    const ScalarT v = LittleEndianToHost(raw);
                    emit(i, v, IsProto3Default(v));
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
                emit(i, v, IsProto3Default(v));
            }
            return CompatStatus::kDecoded;
        }
    }
}

// Finds the last element in a scalar column whose pre-narrowed source wire
// value is non-default, returning its converted value and row index. Runs in
// O(1) for kConst* columns.
template <typename ScalarT>
inline CompatStatus ReadScalarColumnLastNonDefault(FastReader& r, SeqKind kind,
                                                   uint32_t count,
                                                   ScalarT& out_val,
                                                   uint32_t& out_row,
                                                   bool& out_found) {
    out_found = false;
    uint32_t const_n = 0;
    ScalarT const_val{};
    bool const_def = false;
    const CompatStatus cst = ReadConstScalarPayload<ScalarT>(
        r, kind, const_n, const_val, const_def);
    if (cst != CompatStatus::kIncompatible) {
        if (cst == CompatStatus::kError || const_n != count) {
            return CompatStatus::kError;
        }
        if (count > 0 && !const_def) {
            out_val = const_val;
            out_row = count - 1u;
            out_found = true;
        }
        return CompatStatus::kDecoded;
    }
    return ReadScalarColumn<ScalarT>(
        r, kind, count, [&](uint32_t i, ScalarT v, bool wire_is_default) {
            if (!wire_is_default) {
                out_val = v;
                out_row = i;
                out_found = true;
            }
        });
}

// ---- Columnar unknown-field preservation helpers (PRESERVE_UNKNOWN_FIELDS) ---

template <typename RawT>
inline void AppendUnknownFixedField(UnknownFieldBuffer& out,
                                    uint32_t field_number, WireType wt,
                                    RawT v) {
    uint8_t tmp[16];
    uint8_t* p = WriteVarint32Raw(tmp, MakeTag(field_number, wt));
    p = WriteFixedRaw<RawT>(p, v);
    out.MutableBytes().append(reinterpret_cast<const char*>(tmp),
                              static_cast<size_t>(p - tmp));
}

template <typename Visitor>
inline bool DispatchScalarSeqKind(SeqKind kind, Visitor&& visit,
                                  FastReader& fallback_r) {
    switch (kind) {
        case SeqKind::kBool:
        case SeqKind::kBitBool:
        case SeqKind::kConstBool:
            return visit.template operator()<uint8_t, WireType::kFixed1>();
        case SeqKind::kSigned4:
        case SeqKind::kConstS4:
        case SeqKind::kNarrowI8:
        case SeqKind::kNarrowI16:
            return visit.template operator()<int32_t, WireType::kSigned4>();
        case SeqKind::kUnsigned4:
        case SeqKind::kConstU4:
        case SeqKind::kNarrowU8:
        case SeqKind::kNarrowU16:
            return visit.template operator()<uint32_t, WireType::kUnsigned4>();
        case SeqKind::kInt8:
        case SeqKind::kConstI8:
            return visit.template operator()<uint64_t, WireType::kInt8>();
        case SeqKind::kFloat4:
        case SeqKind::kConstF4:
            return visit.template operator()<float, WireType::kFloat4>();
        case SeqKind::kFloat8:
        case SeqKind::kConstF8:
            return visit.template operator()<double, WireType::kFloat8>();
        default:
            return fallback_r.SkipSeqBody();
    }
}

template <typename M>
BRPC_FAST_NOINLINE inline bool CaptureUnknownColumnSeqIntoVec(
    FastReader& r, uint32_t field_number, SeqKind kind, std::vector<M>& vec,
    size_t base, uint32_t count) {
    return DispatchScalarSeqKind(
        kind,
        [&]<typename RawT, WireType Wt>() -> bool {
            const size_t per_row_bytes =
                TagSize(field_number, Wt) + sizeof(RawT) + 48u;
            if (IsVarintConstSeqKind(kind) || kind == SeqKind::kConstF4 ||
                kind == SeqKind::kConstF8) {
                RawT val{};
                uint32_t last_row = 0;
                bool found = false;
                if (ReadScalarColumnLastNonDefault<RawT>(
                        r, kind, count, val, last_row, found) !=
                    CompatStatus::kDecoded) {
                    return false;
                }
                if (!found) return true;
                if (!r.ConsumeExpandedBudget(static_cast<uint64_t>(count) *
                                             per_row_bytes)) {
                    return false;
                }
                M* dst = vec.data() + base;
                for (uint32_t i = 0; i < count; ++i) {
                    AppendUnknownFixedField<RawT>(dst[i]._unknown_fields,
                                                  field_number, Wt, val);
                }
                return true;
            }
            if (!r.ConsumeExpandedBudget(static_cast<uint64_t>(count) *
                                         per_row_bytes)) {
                return false;
            }
            M* dst = vec.data() + base;
            return ReadScalarColumn<RawT>(
                       r, kind, count,
                       [dst, field_number](uint32_t i, RawT v,
                                           bool wire_is_default) {
                           if (!wire_is_default) {
                               AppendUnknownFixedField<RawT>(
                                   dst[i]._unknown_fields, field_number, Wt, v);
                           }
                       }) == CompatStatus::kDecoded;
        },
        r);
}

template <typename M>
inline bool CaptureUnknownColumnIntoVec(FastReader& r, uint32_t field_number,
                                        WireType wt, std::vector<M>& vec,
                                        size_t base, uint32_t count) {
    if (wt != WireType::kSeq) {
        return r.SkipUnknownField(wt);
    }
    uint8_t raw_kind = 0;
    if (!r.ReadU8(raw_kind)) return false;
    return CaptureUnknownColumnSeqIntoVec(
        r, field_number, static_cast<SeqKind>(raw_kind), vec, base, count);
}

BRPC_FAST_NOINLINE inline bool CaptureUnknownColumnSeqIntoSingular(
    FastReader& r, uint32_t field_number, SeqKind kind, uint32_t count,
    UnknownFieldBuffer& out) {
    return DispatchScalarSeqKind(
        kind,
        [&]<typename RawT, WireType Wt>() -> bool {
            RawT val{};
            uint32_t last_row = 0;
            bool found = false;
            if (ReadScalarColumnLastNonDefault<RawT>(
                    r, kind, count, val, last_row, found) !=
                CompatStatus::kDecoded) {
                return false;
            }
            if (found) {
                AppendUnknownFixedField<RawT>(out, field_number, Wt, val);
            }
            return true;
        },
        r);
}

inline bool CaptureUnknownColumnIntoSingular(FastReader& r,
                                             uint32_t field_number, WireType wt,
                                             uint32_t count,
                                             UnknownFieldBuffer& out) {
    if (wt != WireType::kSeq) {
        return r.SkipUnknownField(wt);
    }
    uint8_t raw_kind = 0;
    if (!r.ReadU8(raw_kind)) return false;
    return CaptureUnknownColumnSeqIntoSingular(
        r, field_number, static_cast<SeqKind>(raw_kind), count, out);
}

// ---- Columnar per-field & outer-loop decode helpers ----

enum class ColumnDispatchStatus : uint8_t {
    kHandled,
    kUnknown,
    kError,
};

template <bool PreserveUnknown, typename M>
inline bool HandleIncompatVecColumn(FastReader& r, uint32_t field_number,
                                    SeqKind kind, std::vector<M>& vec,
                                    size_t base, uint32_t count) {
    if constexpr (PreserveUnknown) {
        return CaptureUnknownColumnSeqIntoVec(r, field_number, kind, vec, base,
                                              count);
    } else {
        (void)field_number;
        (void)kind;
        (void)vec;
        (void)base;
        (void)count;
        return r.SkipSeqBody();
    }
}

template <bool PreserveUnknown>
inline bool HandleIncompatSingularColumn(FastReader& r, uint32_t field_number,
                                         SeqKind kind, uint32_t count,
                                         UnknownFieldBuffer* unknown) {
    if constexpr (PreserveUnknown) {
        if (unknown != nullptr) {
            return CaptureUnknownColumnSeqIntoSingular(r, field_number, kind,
                                                       count, *unknown);
        }
    }
    (void)field_number;
    (void)kind;
    (void)count;
    (void)unknown;
    return r.SkipSeqBody();
}

template <bool PreserveUnknown = false, typename M, typename FieldT>
inline bool DecodeColumnIntoField(FastReader& r, uint32_t field_number,
                                  WireType wt, std::vector<M>& vec, size_t base,
                                  uint32_t count, FieldT M::* member,
                                  uint64_t& seen_cols, int col_bit) {
    if (wt != WireType::kSeq) return r.SkipUnknownField(wt);
    uint8_t raw_kind = 0;
    if (!r.ReadU8(raw_kind)) return false;
    const SeqKind kind = static_cast<SeqKind>(raw_kind);
    M* dst = vec.data() + base;

    auto finish = [&](CompatStatus st) -> bool {
        if (st == CompatStatus::kDecoded) {
            if (col_bit >= 0) seen_cols |= (uint64_t{1} << col_bit);
            return true;
        }
        return st == CompatStatus::kIncompatible
                   ? HandleIncompatVecColumn<PreserveUnknown>(
                         r, field_number, kind, vec, base, count)
                   : false;
    };

    if constexpr (kIsFixedScalarField<FieldT>) {
        return finish(ReadScalarColumn<FieldT>(
            r, kind, count,
            [dst, member](uint32_t i, FieldT v, bool) { dst[i].*member = v; }));
    } else if constexpr (IsOptionalLike<FieldT>::value) {
        using U = typename FieldT::value_type;
        if constexpr (kIsFixedScalarField<U>) {
            return finish(ReadScalarColumn<U>(
                r, kind, count,
                [dst, member](uint32_t i, U v, bool wire_is_default) {
                    if (wire_is_default) {
                        (dst[i].*member).reset();
                    } else {
                        dst[i].*member = v;
                    }
                }));
        }
        return finish(CompatStatus::kIncompatible);
    } else if constexpr (IsVector<FieldT>::value) {
        using E = typename FieldT::value_type;
        if constexpr (kIsFixedScalarField<E>) {
            if (!IsCompatibleScalarSeqKind<E>(kind)) {
                return finish(CompatStatus::kIncompatible);
            }
            if (!r.ConsumeExpandedBudget(static_cast<uint64_t>(count) *
                                         (sizeof(E) + 32u))) {
                return false;
            }
            return finish(ReadScalarColumn<E>(
                r, kind, count,
                [dst, member](uint32_t i, E v, bool wire_is_default) {
                    auto& vref = dst[i].*member;
                    vref.clear();
                    if (!wire_is_default) vref.push_back(v);
                }));
        }
        return finish(CompatStatus::kIncompatible);
    } else {
        return finish(CompatStatus::kIncompatible);
    }
}

template <size_t VariantIdx, bool PreserveUnknown = false, typename M,
          typename VariantT>
inline bool DecodeColumnIntoOneof(FastReader& r, uint32_t field_number,
                                  WireType wt, std::vector<M>& vec, size_t base,
                                  uint32_t count, VariantT M::* oneof_member) {
    if (wt != WireType::kSeq) return r.SkipUnknownField(wt);
    uint8_t raw_kind = 0;
    if (!r.ReadU8(raw_kind)) return false;
    const SeqKind kind = static_cast<SeqKind>(raw_kind);
    using BranchT = std::variant_alternative_t<VariantIdx, VariantT>;
    if constexpr (kIsFixedScalarField<BranchT>) {
        M* dst = vec.data() + base;
        const CompatStatus st = ReadScalarColumn<BranchT>(
            r, kind, count,
            [dst, oneof_member](uint32_t i, BranchT v, bool wire_is_default) {
                if (!wire_is_default) {
                    (dst[i].*oneof_member).template emplace<VariantIdx>(v);
                }
            });
        if (st == CompatStatus::kDecoded) return true;
        return st == CompatStatus::kIncompatible
                   ? HandleIncompatVecColumn<PreserveUnknown>(
                         r, field_number, kind, vec, base, count)
                   : false;
    } else {
        return HandleIncompatVecColumn<PreserveUnknown>(r, field_number, kind,
                                                        vec, base, count);
    }
}

template <bool PreserveUnknown = false, typename FieldT>
inline bool DecodeColumnIntoSingularField(
    FastReader& r, uint32_t field_number, WireType wt, uint32_t count,
    FieldT& field, UnknownFieldBuffer* unknown = nullptr) {
    if (wt != WireType::kSeq) return r.SkipUnknownField(wt);
    uint8_t raw_kind = 0;
    if (!r.ReadU8(raw_kind)) return false;
    const SeqKind kind = static_cast<SeqKind>(raw_kind);

    auto decode_last = [&]<typename ScalarT>() -> bool {
        bool found = false;
        uint32_t last_row = 0;
        ScalarT last{};
        const CompatStatus st = ReadScalarColumnLastNonDefault<ScalarT>(
            r, kind, count, last, last_row, found);
        if (st == CompatStatus::kDecoded) {
            if (found) field = last;
            return true;
        }
        return st == CompatStatus::kIncompatible
                   ? HandleIncompatSingularColumn<PreserveUnknown>(
                         r, field_number, kind, count, unknown)
                   : false;
    };

    if constexpr (kIsFixedScalarField<FieldT>) {
        return decode_last.template operator()<FieldT>();
    } else if constexpr (IsOptionalLike<FieldT>::value) {
        using U = typename FieldT::value_type;
        if constexpr (kIsFixedScalarField<U>) {
            return decode_last.template operator()<U>();
        }
        return HandleIncompatSingularColumn<PreserveUnknown>(
            r, field_number, kind, count, unknown);
    } else if constexpr (IsVector<FieldT>::value) {
        using E = typename FieldT::value_type;
        if constexpr (kIsFixedScalarField<E>) {
            if (!IsCompatibleScalarSeqKind<E>(kind)) {
                return HandleIncompatSingularColumn<PreserveUnknown>(
                    r, field_number, kind, count, unknown);
            }
            constexpr size_t kElemBytes =
                std::is_same_v<E, bool> ? 1u : sizeof(E);
            if (!r.ConsumeExpandedBudget(static_cast<uint64_t>(count) *
                                         kElemBytes)) {
                return false;
            }
            return ReadScalarColumn<E>(
                       r, kind, count,
                       [&](uint32_t, E v, bool wire_is_default) {
                           if (!wire_is_default) field.push_back(v);
                       }) == CompatStatus::kDecoded;
        }
        return HandleIncompatSingularColumn<PreserveUnknown>(
            r, field_number, kind, count, unknown);
    } else {
        return HandleIncompatSingularColumn<PreserveUnknown>(
            r, field_number, kind, count, unknown);
    }
}

template <size_t VariantIdx, bool PreserveUnknown = false, typename VariantT>
inline bool DecodeColumnIntoSingularOneof(
    FastReader& r, uint32_t field_number, WireType wt, uint32_t count,
    VariantT& oneof, int64_t& oneof_last_row,
    UnknownFieldBuffer* unknown = nullptr) {
    if (wt != WireType::kSeq) return r.SkipUnknownField(wt);
    uint8_t raw_kind = 0;
    if (!r.ReadU8(raw_kind)) return false;
    const SeqKind kind = static_cast<SeqKind>(raw_kind);
    using BranchT = std::variant_alternative_t<VariantIdx, VariantT>;
    if constexpr (kIsFixedScalarField<BranchT>) {
        bool found = false;
        uint32_t last_row = 0;
        BranchT last{};
        const CompatStatus st = ReadScalarColumnLastNonDefault<BranchT>(
            r, kind, count, last, last_row, found);
        if (st == CompatStatus::kDecoded) {
            if (found && static_cast<int64_t>(last_row) >= oneof_last_row) {
                oneof_last_row = static_cast<int64_t>(last_row);
                oneof.template emplace<VariantIdx>(last);
            }
            return true;
        }
        return st == CompatStatus::kIncompatible
                   ? HandleIncompatSingularColumn<PreserveUnknown>(
                         r, field_number, kind, count, unknown)
                   : false;
    } else {
        return HandleIncompatSingularColumn<PreserveUnknown>(
            r, field_number, kind, count, unknown);
    }
}

// Generic outer framing for generated DecodeColumnarVec / DecodeColumnarSingular
// methods, keeping generated message headers compact.
template <bool Overwrite, bool PreserveUnknown, typename M,
          typename PrepareReusedFn, typename DispatchFn, typename ResetUnseenFn>
inline bool RunColumnarVecDecode(FastReader& sub, std::vector<M>& vec,
                                 PrepareReusedFn&& prepare_reused,
                                 DispatchFn&& dispatch,
                                 ResetUnseenFn&& reset_unseen) {
    uint32_t count = 0;
    if (!sub.ReadVarint32(count)) return false;
    if (!sub.ConsumeExpandedBudget(static_cast<uint64_t>(count) * sizeof(M))) {
        return false;
    }
    const size_t existing = vec.size();
    const size_t base = Overwrite ? 0u : existing;
    const size_t reuse_end =
        Overwrite ? std::min<size_t>(count, existing) : base;
    vec.resize(base + count);
    if constexpr (Overwrite) {
        if (reuse_end > base) {
            prepare_reused(base, reuse_end);
        }
    }
    while (!sub.Eof()) {
        uint32_t tag = 0;
        if (!sub.ReadTag(tag)) return false;
        const uint32_t field_number = TagFieldNumber(tag);
        const WireType wt = TagWireType(tag);
        if (field_number == 0u) return false;
        const ColumnDispatchStatus st = dispatch(field_number, wt, base, count);
        if (st == ColumnDispatchStatus::kError) return false;
        if (st == ColumnDispatchStatus::kUnknown) {
            if constexpr (PreserveUnknown) {
                if (!CaptureUnknownColumnIntoVec(sub, field_number, wt, vec,
                                                 base, count)) {
                    return false;
                }
            } else {
                if (!sub.SkipUnknownField(wt)) return false;
            }
        }
    }
    if constexpr (Overwrite) {
        if (reuse_end > base) {
            reset_unseen(base, reuse_end);
        }
    }
    return true;
}

template <bool Overwrite, bool PreserveUnknown, typename M>
inline bool DecodeColumnarVecNoScalars(FastReader& sub, std::vector<M>& vec) {
    return RunColumnarVecDecode<Overwrite, PreserveUnknown>(
        sub, vec,
        [&](size_t base, size_t reuse_end) {
            for (size_t i = base; i < reuse_end; ++i) {
                vec[i].Clear();
            }
        },
        [](uint32_t, WireType, size_t, uint32_t) noexcept {
            return ColumnDispatchStatus::kUnknown;
        },
        [](size_t, size_t) noexcept {});
}

template <bool PreserveUnknown, typename M, typename DispatchFn>
inline CompatStatus RunColumnarSingularDecode(FastReader& sub, uint32_t count,
                                              M& target, bool already_present,
                                              DispatchFn&& dispatch) {
    if (!already_present) {
        target.Clear();
    }
    while (!sub.Eof()) {
        uint32_t tag = 0;
        if (!sub.ReadTag(tag)) return CompatStatus::kError;
        const uint32_t field_number = TagFieldNumber(tag);
        const WireType wt = TagWireType(tag);
        if (field_number == 0u) return CompatStatus::kError;
        const ColumnDispatchStatus st = dispatch(field_number, wt);
        if (st == ColumnDispatchStatus::kError) return CompatStatus::kError;
        if (st == ColumnDispatchStatus::kUnknown) {
            if constexpr (PreserveUnknown) {
                if (!CaptureUnknownColumnIntoSingular(
                        sub, field_number, wt, count, target._unknown_fields)) {
                    return CompatStatus::kError;
                }
            } else {
                if (!sub.SkipUnknownField(wt)) return CompatStatus::kError;
            }
        }
    }
    return CompatStatus::kDecoded;
}

template <bool PreserveUnknown, typename M>
inline CompatStatus DecodeColumnarSingularNoScalars(FastReader& sub,
                                                    uint32_t count, M& target,
                                                    bool already_present) {
    return RunColumnarSingularDecode<PreserveUnknown>(
        sub, count, target, already_present,
        [](uint32_t, WireType) noexcept {
            return ColumnDispatchStatus::kUnknown;
        });
}

}  // namespace brpc_fast::detail
