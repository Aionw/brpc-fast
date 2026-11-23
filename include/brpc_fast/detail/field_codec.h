#pragma once

#include "brpc_fast/detail/buffer_pool.h"
#include "brpc_fast/detail/port.h"
#include "brpc_fast/detail/wire_reader.h"
#include "brpc_fast/indirect.h"

#include <algorithm>
#include <bit>
#include <concepts>
#include <cstdint>
#include <optional>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

namespace brpc_fast::detail {

// ---- Message concept & ValueCodec traits ----

template <typename T>
concept FastMessageLike =
    requires(const T& cm, T& m, SizeTape& st, uint8_t* dst, const uint32_t*& tp,
             const uint8_t* cur, const uint8_t* end, int depth, FastReader& r) {
        { T::kUsesSizeTape } -> std::convertible_to<bool>;
        { cm.ComputeSize(st) } -> std::same_as<size_t>;
        { cm.EncodeUnchecked(dst, tp) } -> std::same_as<uint8_t*>;
        { m.DecodeContiguous(cur, end, depth) } -> std::same_as<bool>;
        { m.template DecodeLoop<false>(r) } -> std::same_as<bool>;
        { m.template DecodeLoop<true>(r) } -> std::same_as<bool>;
        { m.Decode(r) } -> std::same_as<bool>;
        { m.Clear() } -> std::same_as<void>;
    };

template <typename T, typename Enable = void>
struct ValueCodec;

// bool -> kFixed1
template <>
struct ValueCodec<bool> {
    static constexpr WireType kWireType = WireType::kFixed1;

    static inline size_t SizeValue(bool, SizeTape&) noexcept { return 1; }
    static inline uint8_t* EncodeValueUnchecked(uint8_t* dst, bool v,
                                                const uint32_t*&) noexcept {
        *dst = v ? 1 : 0;
        return dst + 1;
    }
    static inline bool DecodeValue(FastReader& r, bool& v) noexcept {
        return r.ReadBool(v);
    }
    static inline bool DecodeValueContiguous(const uint8_t*& cur,
                                             const uint8_t* end, int,
                                             bool& v) noexcept {
        if (cur >= end) [[unlikely]] {
            return false;
        }
        v = (*cur++ != 0);
        return true;
    }
};

// 4-byte arithmetic types (int32_t, uint32_t, float) -> kFixed4
template <typename T>
struct ValueCodec<
    T, std::enable_if_t<std::is_arithmetic_v<T> && !std::is_same_v<T, bool> &&
                        sizeof(T) == 4>> {
    static constexpr WireType kWireType = WireType::kFixed4;

    static inline size_t SizeValue(T, SizeTape&) noexcept { return 4; }
    static inline uint8_t* EncodeValueUnchecked(uint8_t* dst, T v,
                                                const uint32_t*&) noexcept {
        return WriteFixedRaw<T>(dst, v);
    }
    static inline bool DecodeValue(FastReader& r, T& v) noexcept {
        return r.ReadFixed<T>(v);
    }
    static inline bool DecodeValueContiguous(const uint8_t*& cur,
                                             const uint8_t* end, int,
                                             T& v) noexcept {
        return ReadFixedContiguous<T>(cur, end, v);
    }
};

// 8-byte arithmetic types (int64_t, uint64_t, double) -> kFixed8
template <typename T>
struct ValueCodec<T,
                  std::enable_if_t<std::is_arithmetic_v<T> && sizeof(T) == 8>> {
    static constexpr WireType kWireType = WireType::kFixed8;

    static inline size_t SizeValue(T, SizeTape&) noexcept { return 8; }
    static inline uint8_t* EncodeValueUnchecked(uint8_t* dst, T v,
                                                const uint32_t*&) noexcept {
        return WriteFixedRaw<T>(dst, v);
    }
    static inline bool DecodeValue(FastReader& r, T& v) noexcept {
        return r.ReadFixed<T>(v);
    }
    static inline bool DecodeValueContiguous(const uint8_t*& cur,
                                             const uint8_t* end, int,
                                             T& v) noexcept {
        return ReadFixedContiguous<T>(cur, end, v);
    }
};

// Enums -> kFixed4 (int32_t underlying value)
template <typename T>
struct ValueCodec<T, std::enable_if_t<std::is_enum_v<T>>> {
    static constexpr WireType kWireType = WireType::kFixed4;

    static inline size_t SizeValue(T, SizeTape&) noexcept { return 4; }
    static inline uint8_t* EncodeValueUnchecked(uint8_t* dst, T v,
                                                const uint32_t*&) noexcept {
        return WriteFixedRaw<int32_t>(dst, static_cast<int32_t>(v));
    }
    static inline bool DecodeValue(FastReader& r, T& v) noexcept {
        int32_t raw = 0;
        if (!r.ReadFixed<int32_t>(raw)) {
            return false;
        }
        v = static_cast<T>(raw);
        return true;
    }
    static inline bool DecodeValueContiguous(const uint8_t*& cur,
                                             const uint8_t* end, int,
                                             T& v) noexcept {
        int32_t raw = 0;
        if (!ReadFixedContiguous<int32_t>(cur, end, raw)) {
            return false;
        }
        v = static_cast<T>(raw);
        return true;
    }
};

// std::string (string & bytes) -> kBytes
template <>
struct ValueCodec<std::string> {
    static constexpr WireType kWireType = WireType::kBytes;

    static inline size_t SizeValue(const std::string& v, SizeTape&) noexcept {
        const size_t len = v.size();
        return Varint32Size(static_cast<uint32_t>(len)) + len;
    }
    static inline uint8_t* EncodeValueUnchecked(uint8_t* dst,
                                                const std::string& v,
                                                const uint32_t*&) noexcept {
        return WriteLengthPrefixedBytesRaw(dst, v.data(), v.size());
    }
    static inline bool DecodeValue(FastReader& r, std::string& v) {
        return r.ReadString(v);
    }
    static inline bool DecodeValueContiguous(const uint8_t*& cur,
                                             const uint8_t* end, int,
                                             std::string& v) {
        return ReadStringContiguous(cur, end, v);
    }
};

// Generated messages -> kBytes
template <FastMessageLike T>
struct ValueCodec<T> {
    static constexpr WireType kWireType = WireType::kBytes;

    static inline size_t SizeValue(const T& v, SizeTape& tape) {
        const size_t slot = tape.Reserve();
        const size_t body_len = v.ComputeSize(tape);
        tape.Set(slot, static_cast<uint32_t>(body_len));
        return Varint32Size(static_cast<uint32_t>(body_len)) + body_len;
    }

    static inline uint8_t* EncodeValueUnchecked(
        uint8_t* dst, const T& v, const uint32_t*& tape) noexcept {
        const uint32_t body_len = *tape++;
        dst = WriteVarint32Raw(dst, body_len);
        return v.EncodeUnchecked(dst, tape);
    }

    // Overwrites `v` in-place (reusing heap buffers, resetting unseen fields).
    static inline bool DecodeValue(FastReader& r, T& v) {
        if (r.RemainingDepth() <= 0) [[unlikely]] {
            return false;
        }
        uint32_t len = 0;
        const uint8_t* span = nullptr;
        const SpanReadStatus st = r.TryReadContiguousSpan(len, span);
        if (st == SpanReadStatus::kContiguous) [[likely]] {
            return v.DecodeContiguous(span, span + len, r.RemainingDepth() - 1);
        }
        if (st == SpanReadStatus::kError) {
            return false;
        }
        FastReader sub(FastReader::kUninitialized);
        if (!r.ReadSubReaderSlow(len, sub, /*check_depth=*/true)) {
            return false;
        }
        return v.template DecodeLoop<true>(sub);
    }

    // Merges wire payload into existing `v` (preserving fields already set in
    // earlier occurrences of the same message tag, matching Proto3 semantics).
    static inline bool DecodeValueMerge(FastReader& r, T& v) {
        if (r.RemainingDepth() <= 0) [[unlikely]] {
            return false;
        }
        uint32_t len = 0;
        const uint8_t* span = nullptr;
        const SpanReadStatus st = r.TryReadContiguousSpan(len, span);
        if (st == SpanReadStatus::kContiguous) [[likely]] {
            FastReader sub(span, len, r.RemainingDepth() - 1);
            return v.template DecodeLoop<false>(sub);
        }
        if (st == SpanReadStatus::kError) {
            return false;
        }
        FastReader sub(FastReader::kUninitialized);
        if (!r.ReadSubReaderSlow(len, sub, /*check_depth=*/true)) {
            return false;
        }
        return v.template DecodeLoop<false>(sub);
    }

    static inline bool DecodeValueContiguous(const uint8_t*& cur,
                                             const uint8_t* end, int depth,
                                             T& v) {
        if (depth <= 0) [[unlikely]] {
            return false;
        }
        uint32_t len = 0;
        if (!ReadVarint32Contiguous(cur, end, len)) [[unlikely]] {
            return false;
        }
        if (static_cast<size_t>(end - cur) < len) [[unlikely]] {
            return false;
        }
        const uint8_t* sub_cur = cur;
        cur += len;
        return v.DecodeContiguous(sub_cur, sub_cur + len, depth - 1);
    }
};

// IndirectBox<T> delegates to ValueCodec<T>
template <typename T>
struct ValueCodec<IndirectBox<T>> {
    static constexpr WireType kWireType = WireType::kBytes;

    static inline size_t SizeValue(const IndirectBox<T>& v, SizeTape& tape) {
        if (!v.has_value()) [[unlikely]] {
            const size_t slot = tape.Reserve();
            tape.Set(slot, 0u);
            return 1;
        }
        return ValueCodec<T>::SizeValue(v.get(), tape);
    }
    static inline uint8_t* EncodeValueUnchecked(
        uint8_t* dst, const IndirectBox<T>& v, const uint32_t*& tape) noexcept {
        if (!v.has_value()) [[unlikely]] {
            ++tape;
            *dst++ = 0;
            return dst;
        }
        return ValueCodec<T>::EncodeValueUnchecked(dst, v.get(), tape);
    }
    static inline bool DecodeValue(FastReader& r, IndirectBox<T>& v) {
        return ValueCodec<T>::DecodeValue(r, v.get());
    }
    static inline bool DecodeValueMerge(FastReader& r, IndirectBox<T>& v) {
        return ValueCodec<T>::DecodeValueMerge(r, v.get());
    }
    static inline bool DecodeValueContiguous(const uint8_t*& cur,
                                             const uint8_t* end, int depth,
                                             IndirectBox<T>& v) {
        return ValueCodec<T>::DecodeValueContiguous(cur, end, depth, v.get());
    }
};

// ---- Container & field classification traits ----

template <typename T>
inline constexpr bool kIsBulkCopyableElement =
    (std::is_arithmetic_v<T> && !std::is_same_v<T, bool> &&
     (sizeof(T) == 4 || sizeof(T) == 8)) ||
    (std::is_enum_v<T> && sizeof(T) == 4);

// Proto3 varint-compatible scalar types: bool, int32, uint32, int64, uint64,
// enum (NOT float/double).
template <typename T>
inline constexpr bool kIsVarintCompatScalar =
    std::is_integral_v<T> || std::is_enum_v<T>;

template <typename T>
struct IsOptionalLike : std::false_type {};
template <typename U>
struct IsOptionalLike<std::optional<U>> : std::true_type {};
template <typename U>
struct IsOptionalLike<IndirectOptional<U>> : std::true_type {};

template <typename T>
struct IsIndirectBox : std::false_type {};
template <typename U>
struct IsIndirectBox<IndirectBox<U>> : std::true_type {};

template <typename T>
struct IsVector : std::false_type {};
template <typename U, typename Alloc>
struct IsVector<std::vector<U, Alloc>> : std::true_type {};

template <typename T>
struct IsUnorderedMap : std::false_type {};
template <typename K, typename V, typename H, typename E, typename A>
struct IsUnorderedMap<std::unordered_map<K, V, H, E, A>> : std::true_type {};

template <typename T>
inline constexpr bool kIsMessageOrBox =
    FastMessageLike<T> || IsIndirectBox<T>::value;

// Compile-time WireType deduction for any generated struct field type.
template <typename T>
consteval WireType FieldWireType() {
    if constexpr (IsUnorderedMap<T>::value) {
        return WireType::kList;
    } else if constexpr (IsVector<T>::value) {
        using E = typename T::value_type;
        if constexpr (std::is_same_v<E, bool>) {
            return WireType::kPacked1;
        } else if constexpr (kIsBulkCopyableElement<E> && sizeof(E) == 4) {
            return WireType::kPacked4;
        } else if constexpr (kIsBulkCopyableElement<E> && sizeof(E) == 8) {
            return WireType::kPacked8;
        } else {
            return WireType::kList;
        }
    } else if constexpr (IsOptionalLike<T>::value) {
        return ValueCodec<typename T::value_type>::kWireType;
    } else {
        return ValueCodec<T>::kWireType;
    }
}

template <uint32_t FieldNumber, typename T>
inline constexpr uint32_t kFieldTag = MakeTag(FieldNumber, FieldWireType<T>());

// ---- Pool-friendly ResetField helpers ----

inline constexpr size_t kMaxRetainedBytes = 512 * 1024;
inline constexpr size_t kMaxRetainedElements = 16384;

template <typename Container>
BRPC_FAST_NOINLINE inline void DropOutlierContainer(Container& c) {
    Container().swap(c);
}

inline void ClearString(std::string& s) {
    if (!s.empty()) {
        s.clear();
    }
    if (s.capacity() > kMaxRetainedBytes) [[unlikely]] {
        DropOutlierContainer(s);
    }
}

template <typename T>
inline void ClearVector(std::vector<T>& vec) {
    if (!vec.empty()) {
        vec.clear();
    }
    if constexpr (std::is_same_v<T, bool>) {
        if (vec.capacity() > kMaxRetainedBytes * 8) [[unlikely]] {
            DropOutlierContainer(vec);
        }
    } else if constexpr (kIsBulkCopyableElement<T>) {
        if (vec.capacity() * sizeof(T) > kMaxRetainedBytes) [[unlikely]] {
            DropOutlierContainer(vec);
        }
    } else {
        if (vec.capacity() > kMaxRetainedElements) [[unlikely]] {
            DropOutlierContainer(vec);
        }
    }
}

template <typename K, typename V, typename H, typename E, typename A>
inline void ClearMap(std::unordered_map<K, V, H, E, A>& m) {
    if (!m.empty()) {
        m.clear();
    }
    if (m.bucket_count() > kMaxRetainedElements) [[unlikely]] {
        DropOutlierContainer(m);
    }
}

template <typename T>
inline void ResetField(T& field) {
    if constexpr (IsUnorderedMap<T>::value) {
        ClearMap(field);
    } else if constexpr (IsVector<T>::value) {
        ClearVector(field);
    } else if constexpr (IsOptionalLike<T>::value) {
        field.reset();
    } else if constexpr (std::is_same_v<T, std::string>) {
        ClearString(field);
    } else if constexpr (FastMessageLike<T>) {
        field.Clear();
    } else if constexpr (IsIndirectBox<T>::value) {
        if (field.has_value()) {
            field.get().Clear();
        }
    } else {
        field = T{};
    }
}

// ---- Proto3 default check (preserves -0.0 sign bit per Proto3 spec) ----

template <typename T>
inline bool IsProto3Default(const T& v) noexcept {
    if constexpr (std::is_same_v<T, std::string>) {
        return v.empty();
    } else if constexpr (std::is_enum_v<T>) {
        return static_cast<int32_t>(v) == 0;
    } else if constexpr (std::is_same_v<T, float>) {
        return std::bit_cast<uint32_t>(v) == 0u;
    } else if constexpr (std::is_same_v<T, double>) {
        return std::bit_cast<uint64_t>(v) == 0ULL;
    } else if constexpr (std::is_arithmetic_v<T>) {
        return v == T{};
    } else {
        return false;
    }
}

// ---- Forward declarations for mutual recursion (MapEntry <-> FieldLoop) ----

template <bool Overwrite, typename T>
inline bool DecodeFieldInLoop(FastReader& r, uint32_t tag, WireType wt,
                              T& field, uint64_t& seen_mask, int bit_idx,
                              UnknownFieldBuffer* unknown = nullptr);

template <uint32_t FieldNumber, bool OmitProto3Default = false, typename T>
inline size_t SizeField(const T& v, SizeTape& tape);

template <uint32_t FieldNumber, bool OmitProto3Default = false, typename T>
inline uint8_t* EncodeFieldUnchecked(uint8_t* dst, const T& v,
                                     const uint32_t*& tape) noexcept;

}  // namespace brpc_fast::detail

#include "brpc_fast/detail/schema_compat.h"

namespace brpc_fast::detail {

// ---- Repeated decode (unified Overwrite/Append templates) ----

template <bool Overwrite, typename T>
inline bool DecodeRepeatedBodyContiguous(const uint8_t* cur, const uint8_t* end,
                                         int depth, std::vector<T>& vec) {
    uint32_t count = 0;
    if (!ReadVarint32Contiguous(cur, end, count)) [[unlikely]] {
        return false;
    }
    if (count > static_cast<size_t>(end - cur)) [[unlikely]] {
        return false;
    }
    const size_t base = Overwrite ? 0 : vec.size();
    const size_t target = base + count;
    const size_t existing = vec.size();
    const size_t reuse_end =
        Overwrite ? std::min<size_t>(count, existing) : base;

    if constexpr (std::is_same_v<T, std::string>) {
        if (reuse_end > base) {
            std::string* dst = vec.data();
            for (size_t i = base; i < reuse_end; ++i) {
                if (!ReadStringContiguous(cur, end, dst[i])) [[unlikely]] {
                    return false;
                }
            }
        }
        if (target > existing) {
            vec.reserve(target);
            for (size_t i = std::max(base, existing); i < target; ++i) {
                uint32_t slen = 0;
                if (!ReadVarint32Contiguous(cur, end, slen) ||
                    static_cast<size_t>(end - cur) < slen) [[unlikely]] {
                    return false;
                }
                vec.emplace_back(reinterpret_cast<const char*>(cur), slen);
                cur += slen;
            }
        } else if constexpr (Overwrite) {
            if (target < existing) {
                vec.resize(target);
            }
        }
    } else {
        if (reuse_end > base) {
            T* dst = vec.data();
            for (size_t i = base; i < reuse_end; ++i) {
                if (!ValueCodec<T>::DecodeValueContiguous(
                        cur, end, depth, dst[i])) [[unlikely]] {
                    return false;
                }
            }
        }
        if (target > existing) {
            vec.reserve(target);
            for (size_t i = std::max(base, existing); i < target; ++i) {
                vec.emplace_back();
                if (!ValueCodec<T>::DecodeValueContiguous(
                        cur, end, depth, vec.back())) [[unlikely]] {
                    return false;
                }
            }
        } else if constexpr (Overwrite) {
            if (target < existing) {
                vec.resize(target);
            }
        }
    }
    return cur == end;
}

template <bool Overwrite, typename T>
inline bool DecodeRepeatedFieldContiguous(const uint8_t*& cur,
                                          const uint8_t* end, int depth,
                                          std::vector<T>& vec) {
    if constexpr (kIsBulkCopyableElement<T>) {
        uint32_t byte_len = 0;
        if (!ReadVarint32Contiguous(cur, end, byte_len)) [[unlikely]] {
            return false;
        }
        if ((byte_len % sizeof(T)) != 0 ||
            static_cast<size_t>(end - cur) < byte_len) [[unlikely]] {
            return false;
        }
        const size_t count = byte_len / sizeof(T);
        const size_t base = Overwrite ? 0 : vec.size();
        vec.resize(base + count);
        if (byte_len > 0) {
            std::memcpy(vec.data() + base, cur, byte_len);
            if constexpr (std::endian::native == std::endian::big) {
                for (size_t i = base; i < base + count; ++i) {
                    vec[i] = ByteSwapFixed(vec[i]);
                }
            }
            cur += byte_len;
        }
        return true;
    } else if constexpr (std::is_same_v<T, bool>) {
        uint32_t count = 0;
        if (!ReadVarint32Contiguous(cur, end, count)) [[unlikely]] {
            return false;
        }
        if (static_cast<size_t>(end - cur) < count) [[unlikely]] {
            return false;
        }
        const size_t base = Overwrite ? 0 : vec.size();
        vec.resize(base + count);
        for (uint32_t i = 0; i < count; ++i) {
            vec[base + i] = (*cur++ != 0);
        }
        return true;
    } else {
        uint32_t body_len = 0;
        if (!ReadVarint32Contiguous(cur, end, body_len)) [[unlikely]] {
            return false;
        }
        if (static_cast<size_t>(end - cur) < body_len) [[unlikely]] {
            return false;
        }
        const uint8_t* sub_cur = cur;
        cur += body_len;
        return DecodeRepeatedBodyContiguous<Overwrite>(
            sub_cur, sub_cur + body_len, depth, vec);
    }
}

template <bool Overwrite, typename T>
inline bool DecodeRepeatedField(FastReader& r, std::vector<T>& vec) {
    if constexpr (kIsBulkCopyableElement<T>) {
        uint32_t byte_len = 0;
        if (!r.ReadVarint32(byte_len)) {
            return false;
        }
        if ((byte_len % sizeof(T)) != 0 || r.Remaining() < byte_len)
            [[unlikely]] {
            return false;
        }
        const size_t count = byte_len / sizeof(T);
        const size_t base = Overwrite ? 0 : vec.size();
        vec.resize(base + count);
        if (byte_len > 0) {
            if (!r.ReadBytesRaw(vec.data() + base, byte_len)) {
                return false;
            }
            if constexpr (std::endian::native == std::endian::big) {
                for (size_t i = base; i < base + count; ++i) {
                    vec[i] = ByteSwapFixed(vec[i]);
                }
            }
        }
        return true;
    } else if constexpr (std::is_same_v<T, bool>) {
        uint32_t count = 0;
        if (!r.ReadVarint32(count)) {
            return false;
        }
        if (r.Remaining() < count) [[unlikely]] {
            return false;
        }
        const size_t base = Overwrite ? 0 : vec.size();
        vec.resize(base + count);
        for (uint32_t i = 0; i < count; ++i) {
            uint8_t b = 0;
            if (!r.ReadU8(b)) {
                return false;
            }
            vec[base + i] = (b != 0);
        }
        return true;
    } else {
        uint32_t len = 0;
        const uint8_t* span = nullptr;
        const SpanReadStatus st = r.TryReadContiguousSpan(len, span);
        if (st == SpanReadStatus::kContiguous) [[likely]] {
            return DecodeRepeatedBodyContiguous<Overwrite>(
                span, span + len, r.RemainingDepth(), vec);
        }
        if (st == SpanReadStatus::kError) {
            return false;
        }
        FastReader sub(FastReader::kUninitialized);
        if (!r.ReadSubReaderSlow(len, sub, /*check_depth=*/false)) {
            return false;
        }
        uint32_t count = 0;
        if (!sub.ReadVarint32(count)) {
            return false;
        }
        if (count > sub.Remaining()) [[unlikely]] {
            return false;
        }
        const size_t base = Overwrite ? 0 : vec.size();
        const size_t target = base + count;
        const size_t existing = vec.size();
        const size_t reuse_end =
            Overwrite ? std::min<size_t>(count, existing) : base;

        if (reuse_end > base) {
            T* dst = vec.data();
            for (size_t i = base; i < reuse_end; ++i) {
                if (!ValueCodec<T>::DecodeValue(sub, dst[i])) {
                    return false;
                }
            }
        }
        if (target > existing) {
            vec.reserve(target);
            if constexpr (std::is_same_v<T, std::string>) {
                for (size_t i = std::max(base, existing); i < target; ++i) {
                    if (!sub.ReadStringEmplace(vec)) {
                        return false;
                    }
                }
            } else {
                for (size_t i = std::max(base, existing); i < target; ++i) {
                    vec.emplace_back();
                    if (!ValueCodec<T>::DecodeValue(sub, vec.back())) {
                        return false;
                    }
                }
            }
        } else if constexpr (Overwrite) {
            if (target < existing) {
                vec.resize(target);
            }
        }
        return sub.Eof();
    }
}

// ---- MapEntry-based map decode (wire-compatible with repeated Entry) ----

template <typename K, typename V, typename H, typename E, typename A>
BRPC_FAST_NOINLINE inline bool DecodeMapEntryLoop(
    FastReader& sub, std::unordered_map<K, V, H, E, A>& m) {
    K key{};
    V val{};
    uint64_t seen = 0;
    while (!sub.Eof()) {
        uint32_t tag = 0;
        if (!sub.ReadTag(tag)) {
            return false;
        }
        const uint32_t fn = TagFieldNumber(tag);
        const WireType wt = TagWireType(tag);
        switch (fn) {
            case 0u:
                return false;
            case 1u:
                if (!DecodeFieldInLoop<true>(sub, tag, wt, key, seen, 0,
                                             nullptr)) {
                    return false;
                }
                break;
            case 2u:
                if (!DecodeFieldInLoop<true>(sub, tag, wt, val, seen, 1,
                                             nullptr)) {
                    return false;
                }
                break;
            default:
                if (!sub.SkipUnknownField(wt)) {
                    return false;
                }
                break;
        }
    }
    m.insert_or_assign(std::move(key), std::move(val));
    return true;
}

template <typename K, typename V, typename H, typename E, typename A>
inline bool DecodeMapEntryContiguous(const uint8_t* ecur, const uint8_t* eend,
                                     int depth,
                                     std::unordered_map<K, V, H, E, A>& m) {
    const uint8_t* const orig_cur = ecur;
    K key{};
    V val{};
    constexpr uint32_t kKeyTag = MakeTag(1u, ValueCodec<K>::kWireType);
    constexpr uint32_t kValTag = MakeTag(2u, ValueCodec<V>::kWireType);
    if (ecur < eend && *ecur == static_cast<uint8_t>(kKeyTag)) {
        ++ecur;
        if (!ValueCodec<K>::DecodeValueContiguous(ecur, eend, depth, key))
            [[unlikely]] {
            return false;
        }
    }
    if (ecur < eend && *ecur == static_cast<uint8_t>(kValTag)) {
        ++ecur;
        if (!ValueCodec<V>::DecodeValueContiguous(ecur, eend, depth, val))
            [[unlikely]] {
            return false;
        }
    }
    if (ecur == eend) [[likely]] {
        m.insert_or_assign(std::move(key), std::move(val));
        return true;
    }
    FastReader sub(orig_cur, static_cast<size_t>(eend - orig_cur), depth);
    return DecodeMapEntryLoop(sub, m);
}

template <typename K, typename V, typename H, typename E, typename A>
inline bool DecodeMapSingleEntry(FastReader& r,
                                 std::unordered_map<K, V, H, E, A>& m) {
    uint32_t len = 0;
    const uint8_t* span = nullptr;
    const SpanReadStatus st = r.TryReadContiguousSpan(len, span);
    if (st == SpanReadStatus::kContiguous) [[likely]] {
        return DecodeMapEntryContiguous(span, span + len, r.RemainingDepth(),
                                        m);
    }
    if (st == SpanReadStatus::kError) {
        return false;
    }
    FastReader sub(FastReader::kUninitialized);
    if (!r.ReadSubReaderSlow(len, sub, /*check_depth=*/false)) {
        return false;
    }
    return DecodeMapEntryLoop(sub, m);
}

template <typename K, typename V, typename H, typename E, typename A>
inline bool DecodeMapBodyContiguous(const uint8_t* cur, const uint8_t* end,
                                    int depth,
                                    std::unordered_map<K, V, H, E, A>& m) {
    uint32_t count = 0;
    if (!ReadVarint32Contiguous(cur, end, count)) [[unlikely]] {
        return false;
    }
    if (count > static_cast<size_t>(end - cur)) [[unlikely]] {
        return false;
    }
    m.reserve(m.size() + count);
    for (uint32_t i = 0; i < count; ++i) {
        uint32_t elen = 0;
        if (!ReadVarint32Contiguous(cur, end, elen) ||
            static_cast<size_t>(end - cur) < elen) [[unlikely]] {
            return false;
        }
        const uint8_t* ecur = cur;
        cur += elen;
        if (!DecodeMapEntryContiguous(ecur, ecur + elen, depth, m))
            [[unlikely]] {
            return false;
        }
    }
    return cur == end;
}

template <typename K, typename V, typename H, typename E, typename A>
inline bool DecodeMapField(FastReader& r,
                           std::unordered_map<K, V, H, E, A>& m) {
    uint32_t len = 0;
    const uint8_t* span = nullptr;
    const SpanReadStatus st = r.TryReadContiguousSpan(len, span);
    if (st == SpanReadStatus::kContiguous) [[likely]] {
        return DecodeMapBodyContiguous(span, span + len, r.RemainingDepth(), m);
    }
    if (st == SpanReadStatus::kError) {
        return false;
    }
    FastReader sub(FastReader::kUninitialized);
    if (!r.ReadSubReaderSlow(len, sub, /*check_depth=*/false)) {
        return false;
    }
    uint32_t count = 0;
    if (!sub.ReadVarint32(count)) {
        return false;
    }
    if (count > sub.Remaining()) [[unlikely]] {
        return false;
    }
    m.reserve(m.size() + count);
    for (uint32_t i = 0; i < count; ++i) {
        if (!DecodeMapSingleEntry(sub, m)) {
            return false;
        }
    }
    return sub.Eof();
}

// ---- Unified type-dispatched FieldCodec entry points for codegen ----

template <uint32_t FieldNumber, bool OmitProto3Default, typename T>
inline size_t SizeField(const T& v, SizeTape& tape) {
    if constexpr (IsUnorderedMap<T>::value) {
        if (v.empty()) return 0;
        using V = typename T::mapped_type;
        const size_t body_slot = tape.Reserve();
        size_t body_len = Varint32Size(static_cast<uint32_t>(v.size()));
        for (const auto& [k, val] : v) {
            const size_t entry_slot = tape.Reserve();
            const size_t entry_len =
                SizeField<1u, true>(k, tape) +
                SizeField<2u, !kIsMessageOrBox<V>>(val, tape);
            tape.Set(entry_slot, static_cast<uint32_t>(entry_len));
            body_len +=
                Varint32Size(static_cast<uint32_t>(entry_len)) + entry_len;
        }
        tape.Set(body_slot, static_cast<uint32_t>(body_len));
        return TagSize(FieldNumber, WireType::kList) +
               Varint32Size(static_cast<uint32_t>(body_len)) + body_len;
    } else if constexpr (IsVector<T>::value) {
        if (v.empty()) return 0;
        using E = typename T::value_type;
        constexpr WireType kWt = FieldWireType<T>();
        constexpr size_t tag_sz = TagSize(FieldNumber, kWt);
        if constexpr (kIsBulkCopyableElement<E>) {
            const size_t byte_len = v.size() * sizeof(E);
            return tag_sz + Varint32Size(static_cast<uint32_t>(byte_len)) +
                   byte_len;
        } else if constexpr (std::is_same_v<E, bool>) {
            const size_t n = v.size();
            return tag_sz + Varint32Size(static_cast<uint32_t>(n)) + n;
        } else {
            const size_t slot = tape.Reserve();
            size_t body_len = Varint32Size(static_cast<uint32_t>(v.size()));
            for (const auto& elem : v) {
                body_len += ValueCodec<E>::SizeValue(elem, tape);
            }
            tape.Set(slot, static_cast<uint32_t>(body_len));
            return tag_sz + Varint32Size(static_cast<uint32_t>(body_len)) +
                   body_len;
        }
    } else if constexpr (IsOptionalLike<T>::value) {
        if (!v.has_value()) return 0;
        using E = typename T::value_type;
        return TagSize(FieldNumber, ValueCodec<E>::kWireType) +
               ValueCodec<E>::SizeValue(*v, tape);
    } else {
        if constexpr (OmitProto3Default) {
            if (IsProto3Default(v)) return 0;
        }
        return TagSize(FieldNumber, ValueCodec<T>::kWireType) +
               ValueCodec<T>::SizeValue(v, tape);
    }
}

template <uint32_t FieldNumber, bool OmitProto3Default, typename T>
inline uint8_t* EncodeFieldUnchecked(uint8_t* dst, const T& v,
                                     const uint32_t*& tape) noexcept {
    if constexpr (IsUnorderedMap<T>::value) {
        if (v.empty()) return dst;
        using V = typename T::mapped_type;
        dst = WriteConstTag<FieldNumber, WireType::kList>(dst);
        const uint32_t body_len = *tape++;
        dst = WriteVarint32Raw(dst, body_len);
        dst = WriteVarint32Raw(dst, static_cast<uint32_t>(v.size()));
        for (const auto& [k, val] : v) {
            const uint32_t entry_len = *tape++;
            dst = WriteVarint32Raw(dst, entry_len);
            dst = EncodeFieldUnchecked<1u, true>(dst, k, tape);
            dst = EncodeFieldUnchecked<2u, !kIsMessageOrBox<V>>(dst, val, tape);
        }
        return dst;
    } else if constexpr (IsVector<T>::value) {
        if (v.empty()) return dst;
        using E = typename T::value_type;
        constexpr WireType kWt = FieldWireType<T>();
        dst = WriteConstTag<FieldNumber, kWt>(dst);
        if constexpr (kIsBulkCopyableElement<E>) {
            if constexpr (std::endian::native == std::endian::little) {
                return WriteLengthPrefixedBytesRaw(dst, v.data(),
                                                   v.size() * sizeof(E));
            } else {
                const uint32_t byte_len =
                    static_cast<uint32_t>(v.size() * sizeof(E));
                dst = WriteVarint32Raw(dst, byte_len);
                for (const E& elem : v) {
                    dst = WriteFixedRaw<E>(dst, elem);
                }
                return dst;
            }
        } else if constexpr (std::is_same_v<E, bool>) {
            const uint32_t n = static_cast<uint32_t>(v.size());
            dst = WriteVarint32Raw(dst, n);
            for (uint32_t i = 0; i < n; ++i) {
                *dst++ = v[i] ? 1 : 0;
            }
            return dst;
        } else {
            const uint32_t body_len = *tape++;
            dst = WriteVarint32Raw(dst, body_len);
            dst = WriteVarint32Raw(dst, static_cast<uint32_t>(v.size()));
            for (const auto& elem : v) {
                dst = ValueCodec<E>::EncodeValueUnchecked(dst, elem, tape);
            }
            return dst;
        }
    } else if constexpr (IsOptionalLike<T>::value) {
        if (!v.has_value()) return dst;
        using E = typename T::value_type;
        dst = WriteConstTag<FieldNumber, ValueCodec<E>::kWireType>(dst);
        return ValueCodec<E>::EncodeValueUnchecked(dst, *v, tape);
    } else {
        if constexpr (OmitProto3Default) {
            if (IsProto3Default(v)) return dst;
        }
        dst = WriteConstTag<FieldNumber, ValueCodec<T>::kWireType>(dst);
        return ValueCodec<T>::EncodeValueUnchecked(dst, v, tape);
    }
}

template <typename T>
inline bool DecodeFieldContiguous(const uint8_t*& cur, const uint8_t* end,
                                  int depth, T& field) {
    if constexpr (IsUnorderedMap<T>::value) {
        uint32_t body_len = 0;
        if (!ReadVarint32Contiguous(cur, end, body_len) ||
            static_cast<size_t>(end - cur) < body_len) [[unlikely]] {
            return false;
        }
        const uint8_t* sub_cur = cur;
        cur += body_len;
        ClearMap(field);
        return DecodeMapBodyContiguous(sub_cur, sub_cur + body_len, depth,
                                       field);
    } else if constexpr (IsVector<T>::value) {
        return DecodeRepeatedFieldContiguous<true>(cur, end, depth, field);
    } else if constexpr (IsOptionalLike<T>::value) {
        if (!field.has_value()) {
            field.emplace();
        }
        return ValueCodec<typename T::value_type>::DecodeValueContiguous(
            cur, end, depth, *field);
    } else {
        return ValueCodec<T>::DecodeValueContiguous(cur, end, depth, field);
    }
}

// Unified field decoder for DecodeLoop<Overwrite>.
// - Exact WireType match takes the fast path (reusing heap buffers on first
//   occurrence in Overwrite mode; merging duplicate submessages).
// - Compatible schema-evolution WireTypes convert cleanly on the slow path.
// - Genuinely incompatible WireTypes are skipped (or captured into `unknown`
//   when --preserve_unknown_fields is enabled).
template <bool Overwrite, typename T>
inline bool DecodeFieldInLoop(FastReader& r, uint32_t tag, WireType wt,
                              T& field, uint64_t& seen_mask, int bit_idx,
                              UnknownFieldBuffer* unknown) {
    auto is_seen = [&]() noexcept -> bool {
        if constexpr (Overwrite) {
            return (seen_mask & (uint64_t{1} << bit_idx)) != 0;
        } else {
            return false;
        }
    };
    auto mark_seen = [&]() noexcept {
        if constexpr (Overwrite) {
            seen_mask |= (uint64_t{1} << bit_idx);
        }
    };
    auto mark_first_seen = [&]() noexcept -> bool {
        if constexpr (Overwrite) {
            const uint64_t bit = uint64_t{1} << bit_idx;
            const bool first = (seen_mask & bit) == 0;
            seen_mask |= bit;
            return first;
        } else {
            return false;
        }
    };

    if constexpr (IsUnorderedMap<T>::value) {
        if (wt == WireType::kList) [[likely]] {
            if (mark_first_seen()) ClearMap(field);
            return DecodeMapField(r, field);
        }
        if (wt == WireType::kBytes) {
            if (mark_first_seen()) ClearMap(field);
            return DecodeMapSingleEntry(r, field);
        }
        return r.CaptureUnknownField(tag, wt, unknown);
    } else if constexpr (IsVector<T>::value) {
        if (wt == FieldWireType<T>()) [[likely]] {
            return mark_first_seen() ? DecodeRepeatedField<true>(r, field)
                                     : DecodeRepeatedField<false>(r, field);
        }
        const bool first = Overwrite && !is_seen();
        const CompatStatus st =
            DecodeRepeatedCompatSlow<Overwrite>(r, wt, field, first);
        if (st == CompatStatus::kDecoded) {
            mark_seen();
            return true;
        }
        return st == CompatStatus::kIncompatible
                   ? r.CaptureUnknownField(tag, wt, unknown)
                   : false;
    } else if constexpr (IsOptionalLike<T>::value) {
        using U = typename T::value_type;
        const bool already_present = Overwrite ? is_seen() : field.has_value();
        if (wt == ValueCodec<U>::kWireType) [[likely]] {
            mark_seen();
            if (!field.has_value()) field.emplace();
            if constexpr (FastMessageLike<U>) {
                return already_present
                           ? ValueCodec<U>::DecodeValueMerge(r, *field)
                           : ValueCodec<U>::DecodeValue(r, *field);
            } else {
                return ValueCodec<U>::DecodeValue(r, *field);
            }
        }
        if constexpr (FastMessageLike<U>) {
            if (wt != WireType::kList) {
                return r.CaptureUnknownField(tag, wt, unknown);
            }
            const CompatStatus st =
                DecodeMessageListMerge<U>(r, [&]() -> std::pair<U&, bool> {
                    mark_seen();
                    if (!field.has_value()) field.emplace();
                    return {*field, already_present};
                });
            return st == CompatStatus::kDecoded || st == CompatStatus::kEmpty;
        } else {
            U tmp{};
            const CompatStatus st =
                DecodeSingularCompatSlow<U>(r, wt, tmp, false);
            if (st == CompatStatus::kDecoded) {
                mark_seen();
                field = std::move(tmp);
                return true;
            }
            if (st == CompatStatus::kEmpty) return true;
            return st == CompatStatus::kIncompatible
                       ? r.CaptureUnknownField(tag, wt, unknown)
                       : false;
        }
    } else {
        const bool already_present = is_seen();
        if (wt == ValueCodec<T>::kWireType) [[likely]] {
            mark_seen();
            if constexpr (kIsMessageOrBox<T>) {
                return already_present
                           ? ValueCodec<T>::DecodeValueMerge(r, field)
                           : ValueCodec<T>::DecodeValue(r, field);
            } else {
                return ValueCodec<T>::DecodeValue(r, field);
            }
        }
        const CompatStatus st =
            DecodeSingularCompatSlow<T>(r, wt, field, already_present);
        if (st == CompatStatus::kDecoded) {
            mark_seen();
            return true;
        }
        if (st == CompatStatus::kEmpty) return true;
        return st == CompatStatus::kIncompatible
                   ? r.CaptureUnknownField(tag, wt, unknown)
                   : false;
    }
}

template <bool Overwrite, size_t VariantIdx, typename VariantT>
inline bool DecodeOneofBranchInLoop(FastReader& r, uint32_t tag, WireType wt,
                                    VariantT& v, uint64_t& seen_oneofs,
                                    int oneof_idx,
                                    UnknownFieldBuffer* unknown = nullptr) {
    using BranchT = std::variant_alternative_t<VariantIdx, VariantT>;
    auto mark_oneof_seen = [&]() noexcept {
        if constexpr (Overwrite) {
            seen_oneofs |= (uint64_t{1} << oneof_idx);
        }
    };
    bool same_active = false;
    if constexpr (Overwrite) {
        same_active = ((seen_oneofs & (uint64_t{1} << oneof_idx)) != 0) &&
                      (v.index() == VariantIdx);
    } else {
        same_active = (v.index() == VariantIdx);
    }

    if (wt == ValueCodec<BranchT>::kWireType) [[likely]] {
        mark_oneof_seen();
        if (v.index() != VariantIdx) v.template emplace<VariantIdx>();
        if constexpr (kIsMessageOrBox<BranchT>) {
            return same_active ? ValueCodec<BranchT>::DecodeValueMerge(
                                     r, std::get<VariantIdx>(v))
                               : ValueCodec<BranchT>::DecodeValue(
                                     r, std::get<VariantIdx>(v));
        } else {
            return ValueCodec<BranchT>::DecodeValue(r, std::get<VariantIdx>(v));
        }
    }
    if constexpr (kIsMessageOrBox<BranchT>) {
        if (wt != WireType::kList) {
            return r.CaptureUnknownField(tag, wt, unknown);
        }
        const CompatStatus st = DecodeMessageListMerge<BranchT>(
            r, [&]() -> std::pair<BranchT&, bool> {
                mark_oneof_seen();
                if (v.index() != VariantIdx) v.template emplace<VariantIdx>();
                return {std::get<VariantIdx>(v), same_active};
            });
        return st == CompatStatus::kDecoded || st == CompatStatus::kEmpty;
    } else {
        BranchT tmp{};
        const CompatStatus st =
            DecodeSingularCompatSlow<BranchT>(r, wt, tmp, false);
        if (st == CompatStatus::kDecoded) {
            mark_oneof_seen();
            v.template emplace<VariantIdx>(std::move(tmp));
            return true;
        }
        if (st == CompatStatus::kEmpty) return true;
        return st == CompatStatus::kIncompatible
                   ? r.CaptureUnknownField(tag, wt, unknown)
                   : false;
    }
}

}  // namespace brpc_fast::detail
