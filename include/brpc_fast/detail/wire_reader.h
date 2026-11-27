#pragma once

#include "brpc_fast/detail/port.h"

#include <butil/iobuf.h>

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>

namespace brpc_fast::detail {

// 3-bit wire types for the fast tagged binary format (field numbers up to
// 2^29 - 1, matching protobuf; field numbers 1..15 use 1-byte tags).
// Every unknown field (including future SeqKind container encodings) can be
// skipped in O(1) without schema knowledge.
enum class WireType : uint8_t {
    kFixed1 = 0,     // 1 byte: bool
    kSigned4 = 1,    // 4 bytes LE (signed): int32/sint32/sfixed32/enum
    kUnsigned4 = 2,  // 4 bytes LE (unsigned): uint32/fixed32
    kInt8 = 3,       // 8 bytes LE: int64/uint64/sint64/fixed64/sfixed64
    kFloat4 = 4,     // 4 bytes LE (IEEE-754): float
    kFloat8 = 5,     // 8 bytes LE (IEEE-754): double
    kBytes = 6,      // varint32 len + raw bytes: singular string/bytes/message
    kSeq = 7,        // uint8 SeqKind + varint32 body_len + payload: repeated/map
};

// 1-byte container encoding subtype carried immediately after a WireType::kSeq
// tag, followed by `varint32 body_len` and `body_len` payload bytes.
// Because `body_len` always follows the 1-byte subtype, readers can skip or
// capture unknown future SeqKinds in O(1).
enum class SeqKind : uint8_t {
    kBool = 0,       // body: 1B * count (unpacked bool bytes)
    kSigned4 = 1,    // body: 4B LE * count (repeated int32/sint32/sfixed32/enum)
    kUnsigned4 = 2,  // body: 4B LE * count (repeated uint32/fixed32)
    kInt8 = 3,       // body: 8B LE * count (repeated int64/uint64/sint64/fixed64/sfixed64)
    kFloat4 = 4,     // body: 4B LE * count (repeated float)
    kFloat8 = 5,     // body: 8B LE * count (repeated double)
    kBytesList = 6,  // body: varint32 count + count*(varint32 len + bytes)
                     //       (repeated string/bytes/message & map<K, V>)
    kBitBool = 7,    // body: varint32 count + ceil(count/8) bytes (LSB-first)
    kConstBool = 8,  // body: varint32 count + 1B (0/1)
    kConstS4 = 9,    // body: varint32 count + 4B LE (int32)
    kConstU4 = 10,   // body: varint32 count + 4B LE (uint32)
    kConstI8 = 11,   // body: varint32 count + 8B LE (int64/uint64)
    kConstF4 = 12,   // body: varint32 count + 4B LE (float)
    kConstF8 = 13,   // body: varint32 count + 8B LE (double)
    kNarrowU8 = 14,  // body: 1B * count (uint8, zero-extend)
    kNarrowI8 = 15,  // body: 1B * count (int8, sign-extend)
    kNarrowU16 = 16, // body: 2B LE * count (uint16, zero-extend)
    kNarrowI16 = 17, // body: 2B LE * count (int16, sign-extend)
    kColumnar = 18,  // body: varint32 count + [field_tag(kSeq) + column]...
};

// Maximum expanded byte budget allowed for compressed/virtual container
// payloads (kConst* and kColumnar) to guard against tiny corrupt/crafted
// packets triggering unbounded memory allocations.
inline constexpr size_t kMaxExpandedSeqBytes = 256ULL * 1024ULL * 1024ULL;

// Result of FastReader::TryReadContiguousSpan().
enum class SpanReadStatus : uint8_t {
    kContiguous,  // Payload fits in the current segment; span_cur is valid.
    kStraddling,  // Payload crosses IOBuf block boundary; use slow sub-reader.
    kError,       // Truncated or invalid varint length prefix.
};

inline constexpr int kDefaultMaxRecursionDepth = 64;

inline constexpr uint32_t MakeTag(uint32_t field_number, WireType wt) noexcept {
    return (field_number << 3) | static_cast<uint32_t>(wt);
}

inline constexpr uint32_t TagFieldNumber(uint32_t tag) noexcept {
    return tag >> 3;
}

inline constexpr WireType TagWireType(uint32_t tag) noexcept {
    return static_cast<WireType>(tag & 0x7u);
}

inline constexpr bool IsLengthDelimitedWireType(WireType wt) noexcept {
    return static_cast<uint8_t>(wt) >= static_cast<uint8_t>(WireType::kBytes);
}

inline constexpr uint32_t Varint32Size(uint32_t v) noexcept {
    if (v < (1u << 7)) return 1;
    if (v < (1u << 14)) return 2;
    if (v < (1u << 21)) return 3;
    if (v < (1u << 28)) return 4;
    return 5;
}

inline constexpr uint32_t TagSize(uint32_t field_number, WireType wt) noexcept {
    return Varint32Size(MakeTag(field_number, wt));
}

inline constexpr uint32_t SeqHeaderSize(uint32_t field_number) noexcept {
    return TagSize(field_number, WireType::kSeq) + 1u;
}

// ---- Opt-in unknown field storage (zero cost when disabled) ----

// Copyable, lazily-allocated raw byte buffer storing verbatim (tag + payload)
// entries for messages generated with --preserve_unknown_fields.
class UnknownFieldBuffer {
   public:
    UnknownFieldBuffer() noexcept = default;
    UnknownFieldBuffer(const UnknownFieldBuffer& other) {
        if (other.buf_ && !other.buf_->empty()) {
            buf_ = std::make_unique<std::string>(*other.buf_);
        }
    }
    UnknownFieldBuffer& operator=(const UnknownFieldBuffer& other) {
        if (this != &other) {
            if (other.buf_ && !other.buf_->empty()) {
                if (buf_) {
                    *buf_ = *other.buf_;
                } else {
                    buf_ = std::make_unique<std::string>(*other.buf_);
                }
            } else if (buf_) {
                buf_->clear();
            }
        }
        return *this;
    }
    UnknownFieldBuffer(UnknownFieldBuffer&&) noexcept = default;
    UnknownFieldBuffer& operator=(UnknownFieldBuffer&&) noexcept = default;

    bool operator==(const UnknownFieldBuffer& other) const noexcept {
        const size_t a = Size();
        const size_t b = other.Size();
        if (a != b) return false;
        if (a == 0) return true;
        return *buf_ == *other.buf_;
    }

    inline bool Empty() const noexcept { return !buf_ || buf_->empty(); }
    inline size_t Size() const noexcept { return buf_ ? buf_->size() : 0; }
    inline const char* Data() const noexcept {
        return buf_ ? buf_->data() : nullptr;
    }

    inline void Clear() noexcept {
        if (buf_ != nullptr) [[unlikely]] {
            buf_->clear();
            if (buf_->capacity() > 64 * 1024) {
                buf_.reset();
            }
        }
    }

    inline uint8_t* EncodeUnchecked(uint8_t* dst) const noexcept {
        if (buf_ != nullptr && !buf_->empty()) [[unlikely]] {
            std::memcpy(dst, buf_->data(), buf_->size());
            return dst + buf_->size();
        }
        return dst;
    }

    inline std::string& MutableBytes() {
        if (!buf_) {
            buf_ = std::make_unique<std::string>();
        }
        return *buf_;
    }

   private:
    std::unique_ptr<std::string> buf_;
};

// ---- Raw pointer unchecked encode primitives ----

template <uint32_t V>
inline uint8_t* WriteConstVarint32(uint8_t* dst) noexcept {
    if constexpr (V < (1u << 7)) {
        *dst = static_cast<uint8_t>(V);
        return dst + 1;
    } else if constexpr (V < (1u << 14)) {
        if constexpr (std::endian::native == std::endian::little) {
            const uint16_t packed =
                static_cast<uint16_t>(((V & 0x7Fu) | 0x80u) | ((V >> 7) << 8));
            std::memcpy(dst, &packed, 2);
        } else {
            dst[0] = static_cast<uint8_t>((V & 0x7Fu) | 0x80u);
            dst[1] = static_cast<uint8_t>(V >> 7);
        }
        return dst + 2;
    } else if constexpr (V < (1u << 21)) {
        dst[0] = static_cast<uint8_t>((V & 0x7Fu) | 0x80u);
        dst[1] = static_cast<uint8_t>(((V >> 7) & 0x7Fu) | 0x80u);
        dst[2] = static_cast<uint8_t>(V >> 14);
        return dst + 3;
    } else if constexpr (V < (1u << 28)) {
        dst[0] = static_cast<uint8_t>((V & 0x7Fu) | 0x80u);
        dst[1] = static_cast<uint8_t>(((V >> 7) & 0x7Fu) | 0x80u);
        dst[2] = static_cast<uint8_t>(((V >> 14) & 0x7Fu) | 0x80u);
        dst[3] = static_cast<uint8_t>(V >> 21);
        return dst + 4;
    } else {
        dst[0] = static_cast<uint8_t>((V & 0x7Fu) | 0x80u);
        dst[1] = static_cast<uint8_t>(((V >> 7) & 0x7Fu) | 0x80u);
        dst[2] = static_cast<uint8_t>(((V >> 14) & 0x7Fu) | 0x80u);
        dst[3] = static_cast<uint8_t>(((V >> 21) & 0x7Fu) | 0x80u);
        dst[4] = static_cast<uint8_t>(V >> 28);
        return dst + 5;
    }
}

template <uint32_t FieldNumber, WireType Wt>
inline uint8_t* WriteConstTag(uint8_t* dst) noexcept {
    return WriteConstVarint32<MakeTag(FieldNumber, Wt)>(dst);
}

template <uint32_t FieldNumber, SeqKind Kind>
inline uint8_t* WriteConstSeqHeader(uint8_t* dst) noexcept {
    constexpr uint32_t kTag = MakeTag(FieldNumber, WireType::kSeq);
    if constexpr (kTag < (1u << 7)) {
        if constexpr (std::endian::native == std::endian::little) {
            const uint16_t packed = static_cast<uint16_t>(
                kTag | (static_cast<uint16_t>(Kind) << 8));
            std::memcpy(dst, &packed, 2);
        } else {
            dst[0] = static_cast<uint8_t>(kTag);
            dst[1] = static_cast<uint8_t>(Kind);
        }
        return dst + 2;
    } else {
        dst = WriteConstVarint32<kTag>(dst);
        *dst++ = static_cast<uint8_t>(Kind);
        return dst;
    }
}

BRPC_FAST_NOINLINE inline uint8_t* WriteVarint32RawSlow(uint8_t* dst,
                                                        uint32_t v) noexcept {
    while (v >= 0x80u) {
        *dst++ = static_cast<uint8_t>((v & 0x7Fu) | 0x80u);
        v >>= 7;
    }
    *dst++ = static_cast<uint8_t>(v);
    return dst;
}

inline uint8_t* WriteVarint32Raw(uint8_t* dst, uint32_t v) noexcept {
    if (v < 0x80u) [[likely]] {
        *dst = static_cast<uint8_t>(v);
        return dst + 1;
    }
    if (v < 0x4000u) [[likely]] {
        if constexpr (std::endian::native == std::endian::little) {
            const uint16_t packed =
                static_cast<uint16_t>(((v & 0x7Fu) | 0x80u) | ((v >> 7) << 8));
            std::memcpy(dst, &packed, 2);
        } else {
            dst[0] = static_cast<uint8_t>((v & 0x7Fu) | 0x80u);
            dst[1] = static_cast<uint8_t>(v >> 7);
        }
        return dst + 2;
    }
    return WriteVarint32RawSlow(dst, v);
}

template <typename T>
inline uint8_t* WriteFixedRaw(uint8_t* dst, T v) noexcept {
    static_assert(sizeof(T) == 1 || sizeof(T) == 2 || sizeof(T) == 4 ||
                      sizeof(T) == 8,
                  "WriteFixedRaw supports 1-, 2-, 4-, or 8-byte values");
    const T le = HostToLittleEndian(v);
    std::memcpy(dst, &le, sizeof(T));
    return dst + sizeof(T);
}

inline uint8_t* WriteLengthPrefixedBytesRaw(uint8_t* dst, const void* src,
                                            size_t len) noexcept {
    dst = WriteVarint32Raw(dst, static_cast<uint32_t>(len));
    if (len > 0) {
        std::memcpy(dst, src, len);
        dst += len;
    }
    return dst;
}

// ---- Raw pointer contiguous decode primitives ----

template <uint32_t Tag>
inline bool MatchConstTag(const uint8_t*& cur, const uint8_t* end) noexcept {
    if constexpr (Tag < (1u << 7)) {
        if (cur < end && *cur == static_cast<uint8_t>(Tag)) {
            ++cur;
            return true;
        }
        return false;
    } else if constexpr (Tag < (1u << 14)) {
        if (static_cast<size_t>(end - cur) >= 2) {
            if constexpr (std::endian::native == std::endian::little) {
                constexpr uint16_t kPacked = static_cast<uint16_t>(
                    ((Tag & 0x7Fu) | 0x80u) | ((Tag >> 7) << 8));
                uint16_t v = 0;
                std::memcpy(&v, cur, 2);
                if (v == kPacked) {
                    cur += 2;
                    return true;
                }
            } else {
                if (cur[0] == static_cast<uint8_t>((Tag & 0x7Fu) | 0x80u) &&
                    cur[1] == static_cast<uint8_t>(Tag >> 7)) {
                    cur += 2;
                    return true;
                }
            }
        }
        return false;
    } else {
        return false;
    }
}

template <uint32_t FieldNumber, SeqKind Kind>
inline bool MatchConstSeqHeader(const uint8_t*& cur,
                                const uint8_t* end) noexcept {
    constexpr uint32_t kTag = MakeTag(FieldNumber, WireType::kSeq);
    if constexpr (kTag < (1u << 7)) {
        if (static_cast<size_t>(end - cur) >= 2) {
            if constexpr (std::endian::native == std::endian::little) {
                constexpr uint16_t kPacked = static_cast<uint16_t>(
                    kTag | (static_cast<uint16_t>(Kind) << 8));
                uint16_t v = 0;
                std::memcpy(&v, cur, 2);
                if (v == kPacked) {
                    cur += 2;
                    return true;
                }
            } else {
                if (cur[0] == static_cast<uint8_t>(kTag) &&
                    cur[1] == static_cast<uint8_t>(Kind)) {
                    cur += 2;
                    return true;
                }
            }
        }
        return false;
    } else if constexpr (kTag < (1u << 14)) {
        if (static_cast<size_t>(end - cur) >= 3) {
            if (cur[0] == static_cast<uint8_t>((kTag & 0x7Fu) | 0x80u) &&
                cur[1] == static_cast<uint8_t>(kTag >> 7) &&
                cur[2] == static_cast<uint8_t>(Kind)) {
                cur += 3;
                return true;
            }
        }
        return false;
    } else {
        return false;
    }
}

BRPC_FAST_NOINLINE inline bool ReadVarint32ContiguousSlow(
    const uint8_t*& cur, const uint8_t* end, uint32_t partial,
    uint32_t& out) noexcept {
    uint32_t result = partial;
    for (int shift = 14; shift <= 28; shift += 7) {
        if (cur >= end) [[unlikely]] {
            return false;
        }
        const uint32_t b = *cur++;
        if (shift == 28) {
            // 5th byte of a uint32 varint may only carry bits 28..31.
            if (b >= 0x10u) [[unlikely]] {
                return false;
            }
            out = result | (b << 28);
            return true;
        }
        result |= (b & 0x7Fu) << shift;
        if (b < 0x80u) {
            out = result;
            return true;
        }
    }
    return false;
}

inline bool ReadVarint32Contiguous(const uint8_t*& cur, const uint8_t* end,
                                   uint32_t& out) noexcept {
    if (cur >= end) [[unlikely]] {
        return false;
    }
    const uint32_t b0 = *cur++;
    if (b0 < 0x80u) {
        out = b0;
        return true;
    }
    if (cur >= end) [[unlikely]] {
        return false;
    }
    const uint32_t b1 = *cur++;
    if (b1 < 0x80u) [[likely]] {
        out = (b0 & 0x7Fu) | (b1 << 7);
        return true;
    }
    return ReadVarint32ContiguousSlow(cur, end,
                                      (b0 & 0x7Fu) | ((b1 & 0x7Fu) << 7), out);
}

template <typename T>
inline bool ReadFixedContiguous(const uint8_t*& cur, const uint8_t* end,
                                T& out) noexcept {
    static_assert(sizeof(T) == 1 || sizeof(T) == 2 || sizeof(T) == 4 ||
                      sizeof(T) == 8,
                  "ReadFixedContiguous supports 1-, 2-, 4-, or 8-byte values");
    if (static_cast<size_t>(end - cur) < sizeof(T)) [[unlikely]] {
        return false;
    }
    T raw;
    std::memcpy(&raw, cur, sizeof(T));
    out = LittleEndianToHost(raw);
    cur += sizeof(T);
    return true;
}

// Fast in-place byte copy into std::string: when `len <= out.size()`, copies
// directly and shrinks in-place without an out-of-line replace call.
inline void AssignBytesFast(std::string& out, const uint8_t* src, size_t len) {
    if (len <= out.size()) [[likely]] {
        if (len > 0) {
            std::memcpy(out.data(), src, len);
        }
        out.resize(len);
        return;
    }
    out.assign(reinterpret_cast<const char*>(src), len);
}

inline bool ReadStringContiguous(const uint8_t*& cur, const uint8_t* end,
                                 std::string& out) {
    uint32_t len = 0;
    if (!ReadVarint32Contiguous(cur, end, len)) [[unlikely]] {
        return false;
    }
    if (static_cast<size_t>(end - cur) < len) [[unlikely]] {
        return false;
    }
    AssignBytesFast(out, cur, len);
    cur += len;
    return true;
}

// Zero-copy, bounds-checked multi-block cursor over butil::IOBuf (or raw
// bytes). Never linearizes or allocates a whole-message buffer, even for
// multi-MB messages spanning hundreds of 8KB IOBuf blocks.
class FastReader {
   public:
    struct UninitializedTag {};
    static constexpr UninitializedTag kUninitialized{};

    explicit FastReader(UninitializedTag) noexcept {}

    explicit FastReader(
        const butil::IOBuf& buf,
        int remaining_depth = kDefaultMaxRecursionDepth) noexcept
        : buf_(&buf),
          cur_(nullptr),
          seg_end_(nullptr),
          block_idx_(0),
          block_count_(buf.backing_block_num()),
          rem_after_seg_(0),
          remaining_depth_(remaining_depth) {
        const size_t total = buf.size();
        if (total == 0 || block_count_ == 0) {
            return;
        }
        rem_after_seg_ = total;
        InitFirstBlock();
    }

    FastReader(const uint8_t* data, size_t size,
               int remaining_depth = kDefaultMaxRecursionDepth) noexcept
        : buf_(nullptr),
          cur_(data),
          seg_end_(data + size),
          block_idx_(0),
          block_count_(0),
          rem_after_seg_(0),
          remaining_depth_(remaining_depth) {}

    FastReader(const FastReader&) = delete;
    FastReader& operator=(const FastReader&) = delete;

    inline bool IsContiguous() const noexcept { return rem_after_seg_ == 0; }
    inline const uint8_t* Cur() const noexcept { return cur_; }
    inline const uint8_t* SegEnd() const noexcept { return seg_end_; }
    inline void ConsumeContiguous() noexcept { cur_ = seg_end_; }

    inline bool Eof() const noexcept {
        return cur_ >= seg_end_ && rem_after_seg_ == 0;
    }

    inline size_t Remaining() const noexcept {
        return static_cast<size_t>(seg_end_ - cur_) + rem_after_seg_;
    }

    inline int RemainingDepth() const noexcept { return remaining_depth_; }

    inline bool ReadU8(uint8_t& out) noexcept {
        if (cur_ < seg_end_) [[likely]] {
            out = *cur_++;
            return true;
        }
        return ReadU8Slow(out);
    }

    inline bool ReadBool(bool& out) noexcept {
        uint8_t v = 0;
        if (!ReadU8(v)) {
            return false;
        }
        out = (v != 0);
        return true;
    }

    template <typename T>
    inline bool ReadFixed(T& out) noexcept {
        static_assert(sizeof(T) == 1 || sizeof(T) == 2 || sizeof(T) == 4 ||
                          sizeof(T) == 8,
                      "ReadFixed supports 1-, 2-, 4-, or 8-byte values");
        if (static_cast<size_t>(seg_end_ - cur_) >= sizeof(T)) [[likely]] {
            T raw;
            std::memcpy(&raw, cur_, sizeof(T));
            out = LittleEndianToHost(raw);
            cur_ += sizeof(T);
            return true;
        }
        T raw;
        if (!ReadBytesRawSlow(&raw, sizeof(T))) {
            return false;
        }
        out = LittleEndianToHost(raw);
        return true;
    }

    inline bool ReadVarint32(uint32_t& out) noexcept {
        if (cur_ < seg_end_) [[likely]] {
            const uint32_t b0 = *cur_++;
            if (b0 < 0x80u) {
                out = b0;
                return true;
            }
            if (cur_ < seg_end_) [[likely]] {
                const uint32_t b1 = *cur_++;
                if (b1 < 0x80u) [[likely]] {
                    out = (b0 & 0x7Fu) | (b1 << 7);
                    return true;
                }
                return ReadVarint32Continuation(
                    (b0 & 0x7Fu) | ((b1 & 0x7Fu) << 7), 14, out);
            }
            return ReadVarint32Continuation(b0 & 0x7Fu, 7, out);
        }
        return ReadVarint32Slow(out);
    }

    inline bool ReadTag(uint32_t& tag) noexcept { return ReadVarint32(tag); }

    inline bool ReadBytesRaw(void* dst, size_t len) noexcept {
        if (len == 0) {
            return true;
        }
        if (static_cast<size_t>(seg_end_ - cur_) >= len) [[likely]] {
            std::memcpy(dst, cur_, len);
            cur_ += len;
            return true;
        }
        return ReadBytesRawSlow(dst, len);
    }

    inline bool ReadString(std::string& out) {
        uint32_t len = 0;
        if (!ReadVarint32(len)) {
            return false;
        }
        if (static_cast<size_t>(seg_end_ - cur_) >= len) [[likely]] {
            AssignBytesFast(out, cur_, len);
            cur_ += len;
            return true;
        }
        return ReadStringSlow(len, out);
    }

    template <typename Vec>
    inline bool ReadStringEmplace(Vec& vec) {
        uint32_t len = 0;
        if (!ReadVarint32(len)) {
            return false;
        }
        if (static_cast<size_t>(seg_end_ - cur_) >= len) [[likely]] {
            vec.emplace_back(reinterpret_cast<const char*>(cur_), len);
            cur_ += len;
            return true;
        }
        vec.emplace_back();
        return ReadStringSlow(len, vec.back());
    }

    inline bool Skip(size_t len) noexcept {
        if (static_cast<size_t>(seg_end_ - cur_) >= len) [[likely]] {
            cur_ += len;
            return true;
        }
        return SkipSlow(len);
    }

    // Reads a varint32 length prefix; if the payload lies completely within the
    // current contiguous segment, sets `span_cur` and advances `cur_`.
    inline SpanReadStatus TryReadContiguousSpan(
        uint32_t& len_out, const uint8_t*& span_cur) noexcept {
        if (!ReadVarint32(len_out)) [[unlikely]] {
            return SpanReadStatus::kError;
        }
        if (static_cast<size_t>(seg_end_ - cur_) >= len_out) [[likely]] {
            span_cur = cur_;
            cur_ += len_out;
            return SpanReadStatus::kContiguous;
        }
        return SpanReadStatus::kStraddling;
    }

    BRPC_FAST_NOINLINE bool ReadSubReaderSlow(uint32_t len, FastReader& sub_out,
                                              bool check_depth) noexcept {
        if (Remaining() < len) {
            return false;
        }
        const int next_depth =
            check_depth ? (remaining_depth_ - 1) : remaining_depth_;
        if (len == 0) {
            sub_out.buf_ = nullptr;
            sub_out.cur_ = nullptr;
            sub_out.seg_end_ = nullptr;
            sub_out.block_idx_ = 0;
            sub_out.block_count_ = 0;
            sub_out.rem_after_seg_ = 0;
            sub_out.remaining_depth_ = next_depth;
            return true;
        }
        if (cur_ == seg_end_ && !AdvanceBlock()) {
            return false;
        }
        const size_t avail = static_cast<size_t>(seg_end_ - cur_);
        if (avail >= len) {
            sub_out.buf_ = nullptr;
            sub_out.cur_ = cur_;
            sub_out.seg_end_ = cur_ + len;
            sub_out.block_idx_ = 0;
            sub_out.block_count_ = 0;
            sub_out.rem_after_seg_ = 0;
            sub_out.remaining_depth_ = next_depth;
            cur_ += len;
            return true;
        }
        sub_out.buf_ = buf_;
        sub_out.cur_ = cur_;
        sub_out.seg_end_ = seg_end_;
        sub_out.block_idx_ = block_idx_;
        sub_out.block_count_ = block_count_;
        sub_out.rem_after_seg_ = len - avail;
        sub_out.remaining_depth_ = next_depth;
        return SkipSlow(len);
    }

    inline bool SkipSeqBody() noexcept {
        uint32_t len = 0;
        return ReadVarint32(len) && Skip(len);
    }

    inline bool SkipUnknownField(WireType wt) noexcept {
        switch (wt) {
            case WireType::kFixed1:
                return Skip(1);
            case WireType::kSigned4:
            case WireType::kUnsigned4:
            case WireType::kFloat4:
                return Skip(4);
            case WireType::kInt8:
            case WireType::kFloat8:
                return Skip(8);
            case WireType::kBytes: {
                uint32_t len = 0;
                return ReadVarint32(len) && Skip(len);
            }
            case WireType::kSeq: {
                uint8_t kind = 0;
                uint32_t len = 0;
                return ReadU8(kind) && ReadVarint32(len) && Skip(len);
            }
        }
        return false;
    }

    // Captures raw (tag + seq_kind + body_len + payload) bytes for a kSeq field
    // whose 1-byte `kind` has already been read from this reader.
    BRPC_FAST_NOINLINE bool CaptureUnknownSeqField(uint32_t tag, uint8_t kind,
                                                   UnknownFieldBuffer* out) {
        if (out == nullptr) {
            return SkipSeqBody();
        }
        uint32_t len = 0;
        if (!ReadVarint32(len) || Remaining() < len) {
            return false;
        }
        uint8_t header[12];
        uint8_t* hp = WriteVarint32Raw(header, tag);
        *hp++ = kind;
        hp = WriteVarint32Raw(hp, len);
        const size_t header_len = static_cast<size_t>(hp - header);
        std::string& dst = out->MutableBytes();
        const size_t old_sz = dst.size();
        dst.resize(old_sz + header_len + len);
        std::memcpy(dst.data() + old_sz, header, header_len);
        if (len > 0 && !ReadBytesRaw(dst.data() + old_sz + header_len, len)) {
            dst.resize(old_sz);
            return false;
        }
        return true;
    }

    // Captures raw (tag + payload) bytes into `out` when unknown-field
    // preservation is explicitly enabled.
    BRPC_FAST_NOINLINE bool CaptureUnknownField(uint32_t tag, WireType wt,
                                                UnknownFieldBuffer* out) {
        if (wt == WireType::kSeq) {
            uint8_t kind = 0;
            if (!ReadU8(kind)) {
                return false;
            }
            return CaptureUnknownSeqField(tag, kind, out);
        }
        if (out == nullptr) {
            return SkipUnknownField(wt);
        }
        uint8_t header[10];
        uint8_t* hp = WriteVarint32Raw(header, tag);
        size_t payload_len = 0;
        switch (wt) {
            case WireType::kFixed1:
                payload_len = 1;
                break;
            case WireType::kSigned4:
            case WireType::kUnsigned4:
            case WireType::kFloat4:
                payload_len = 4;
                break;
            case WireType::kInt8:
            case WireType::kFloat8:
                payload_len = 8;
                break;
            case WireType::kBytes: {
                uint32_t len = 0;
                if (!ReadVarint32(len)) {
                    return false;
                }
                hp = WriteVarint32Raw(hp, len);
                payload_len = len;
                break;
            }
            default:
                return false;
        }
        if (Remaining() < payload_len) {
            return false;
        }
        const size_t header_len = static_cast<size_t>(hp - header);
        std::string& dst = out->MutableBytes();
        const size_t old_sz = dst.size();
        dst.resize(old_sz + header_len + payload_len);
        std::memcpy(dst.data() + old_sz, header, header_len);
        if (payload_len > 0 &&
            !ReadBytesRaw(dst.data() + old_sz + header_len, payload_len)) {
            dst.resize(old_sz);
            return false;
        }
        return true;
    }

   private:
    void InitFirstBlock() noexcept {
        for (block_idx_ = 0; block_idx_ < block_count_; ++block_idx_) {
            butil::StringPiece sp = buf_->backing_block(block_idx_);
            if (!sp.empty()) {
                const size_t take = std::min(sp.size(), rem_after_seg_);
                cur_ = reinterpret_cast<const uint8_t*>(sp.data());
                seg_end_ = cur_ + take;
                rem_after_seg_ -= take;
                return;
            }
        }
        rem_after_seg_ = 0;
    }

    bool AdvanceBlock() noexcept {
        if (rem_after_seg_ == 0 || buf_ == nullptr) {
            return false;
        }
        while (++block_idx_ < block_count_) {
            butil::StringPiece sp = buf_->backing_block(block_idx_);
            if (!sp.empty()) {
                const size_t take = std::min(sp.size(), rem_after_seg_);
                cur_ = reinterpret_cast<const uint8_t*>(sp.data());
                seg_end_ = cur_ + take;
                rem_after_seg_ -= take;
                return true;
            }
        }
        rem_after_seg_ = 0;
        return false;
    }

    BRPC_FAST_NOINLINE bool ReadU8Slow(uint8_t& out) noexcept {
        if (!AdvanceBlock()) {
            return false;
        }
        out = *cur_++;
        return true;
    }

    BRPC_FAST_NOINLINE bool ReadVarint32Slow(uint32_t& out) noexcept {
        uint8_t first = 0;
        if (!ReadU8Slow(first)) {
            return false;
        }
        if (first < 0x80u) {
            out = first;
            return true;
        }
        return ReadVarint32Continuation(first & 0x7Fu, 7, out);
    }

    BRPC_FAST_NOINLINE bool ReadVarint32Continuation(uint32_t partial,
                                                     int start_shift,
                                                     uint32_t& out) noexcept {
        uint32_t result = partial;
        for (int shift = start_shift; shift <= 28; shift += 7) {
            uint8_t b = 0;
            if (!ReadU8(b)) {
                return false;
            }
            if (shift == 28) {
                if (b >= 0x10u) [[unlikely]] {
                    return false;
                }
                out = result | (static_cast<uint32_t>(b) << 28);
                return true;
            }
            result |= static_cast<uint32_t>(b & 0x7Fu) << shift;
            if (b < 0x80u) {
                out = result;
                return true;
            }
        }
        return false;
    }

    BRPC_FAST_NOINLINE bool ReadBytesRawSlow(void* dst, size_t len) noexcept {
        if (Remaining() < len) {
            return false;
        }
        auto* out = static_cast<uint8_t*>(dst);
        while (len > 0) {
            const size_t avail = static_cast<size_t>(seg_end_ - cur_);
            if (avail == 0) {
                if (!AdvanceBlock()) {
                    return false;
                }
                continue;
            }
            const size_t take = std::min(avail, len);
            std::memcpy(out, cur_, take);
            cur_ += take;
            out += take;
            len -= take;
        }
        return true;
    }

    BRPC_FAST_NOINLINE bool ReadStringSlow(uint32_t len, std::string& out) {
        if (Remaining() < len) {
            return false;
        }
        out.resize(len);
        return ReadBytesRawSlow(out.data(), len);
    }

    BRPC_FAST_NOINLINE bool SkipSlow(size_t len) noexcept {
        if (Remaining() < len) {
            return false;
        }
        while (len > 0) {
            const size_t avail = static_cast<size_t>(seg_end_ - cur_);
            if (avail >= len) {
                cur_ += len;
                return true;
            }
            len -= avail;
            cur_ = seg_end_;
            if (len > 0 && !AdvanceBlock()) {
                return false;
            }
        }
        return true;
    }

    const butil::IOBuf* buf_;
    const uint8_t* cur_;
    const uint8_t* seg_end_;
    size_t block_idx_;
    size_t block_count_;
    size_t rem_after_seg_;
    int remaining_depth_;
};

}  // namespace brpc_fast::detail
