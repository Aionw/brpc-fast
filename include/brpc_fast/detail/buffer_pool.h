#pragma once

#include "brpc_fast/detail/port.h"

#include <butil/object_pool.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <mutex>

namespace brpc_fast::detail {

// Maximum stack buffer size used for inline encoding before acquiring a pooled
// heap slab.
inline constexpr size_t kInlineEncodeCapacity = 4096;

// Pre-order tape recording exact byte lengths of nested messages and
// length-prefixed containers during Pass 1 (ComputeSize) so Pass 2
// (EncodeUnchecked) writes compact varint32 lengths in a single unchecked pass.
// Pooled via butil::ObjectPool so steady-state large batches do zero mallocs.
class SizeTape {
   public:
    SizeTape() noexcept = default;
    ~SizeTape() { delete[] ptr_; }

    SizeTape(const SizeTape&) = delete;
    SizeTape& operator=(const SizeTape&) = delete;

    inline size_t Reserve() {
        if (count_ == cap_) [[unlikely]] {
            Grow();
        }
        return count_++;
    }

    inline void Set(size_t idx, uint32_t len) noexcept { ptr_[idx] = len; }
    inline const uint32_t* Data() const noexcept { return ptr_; }

    inline void Reset() noexcept {
        count_ = 0;
        // Release pathological outlier buffers (> 2 MiB) so pooled tapes stay
        // bounded in steady state.
        if (cap_ > (512u * 1024u)) [[unlikely]] {
            delete[] ptr_;
            ptr_ = nullptr;
            cap_ = 0;
        }
    }

   private:
    BRPC_FAST_NOINLINE void Grow() {
        const size_t new_cap = (cap_ == 0) ? 512 : (cap_ * 4);
        uint32_t* new_buf = new uint32_t[new_cap];
        if (count_ > 0) {
            std::memcpy(new_buf, ptr_, count_ * sizeof(uint32_t));
        }
        delete[] ptr_;
        ptr_ = new_buf;
        cap_ = new_cap;
    }

    uint32_t* ptr_ = nullptr;
    size_t cap_ = 0;
    size_t count_ = 0;
};

class SizeTapeGuard {
   public:
    SizeTapeGuard() noexcept : tape_(butil::get_object<SizeTape>()) {}
    ~SizeTapeGuard() {
        if (tape_ != nullptr) {
            tape_->Reset();
            butil::return_object(tape_);
        }
    }

    SizeTapeGuard(const SizeTapeGuard&) = delete;
    SizeTapeGuard& operator=(const SizeTapeGuard&) = delete;

    inline SizeTape& Tape() noexcept { return *tape_; }

   private:
    SizeTape* tape_;
};

// Bounded slab pool for large encoded buffers (> 4KB up to 8MB).
// Eliminates per-RPC mmap / minor page faults / munmap TLB shootdowns when
// encoding multi-MB batch payloads across concurrent bthreads.
class WriteSlabPool {
   public:
    static constexpr size_t kNumClasses = 7;
    static constexpr size_t kClassSizes[kNumClasses] = {
        16u * 1024u,    // 16 KiB
        64u * 1024u,    // 64 KiB
        256u * 1024u,   // 256 KiB
        1024u * 1024u,  // 1 MiB
        2048u * 1024u,  // 2 MiB
        4096u * 1024u,  // 4 MiB
        8192u * 1024u,  // 8 MiB
    };
    static constexpr size_t kClassMaxKeep[kNumClasses] = {
        48, 48, 48, 48, 48, 32, 16,
    };
    static constexpr size_t kMaxSlots = 48;
    static constexpr size_t kHeaderBytes = 16;
    static constexpr uint32_t kUncachedClass = 0xFFFFFFFFu;

    static WriteSlabPool& Instance() {
        // Intentionally leaked to avoid static destruction order issues if an
        // IOBuf outlives main().
        static auto* pool = new WriteSlabPool();
        return *pool;
    }

    uint8_t* Acquire(size_t min_bytes, size_t* actual_cap = nullptr) {
        for (uint32_t c = 0; c < kNumClasses; ++c) {
            if (min_bytes <= kClassSizes[c]) {
                if (actual_cap != nullptr) {
                    *actual_cap = kClassSizes[c];
                }
                auto& cls = classes_[c];
                {
                    std::lock_guard<std::mutex> lk(cls.mu);
                    if (cls.count > 0) {
                        return cls.ptrs[--cls.count];
                    }
                }
                uint8_t* raw = new uint8_t[kClassSizes[c] + kHeaderBytes];
                std::memcpy(raw, &c, sizeof(c));
                return raw + kHeaderBytes;
            }
        }
        if (actual_cap != nullptr) {
            *actual_cap = min_bytes;
        }
        uint8_t* raw = new uint8_t[min_bytes + kHeaderBytes];
        const uint32_t uncached = kUncachedClass;
        std::memcpy(raw, &uncached, sizeof(uncached));
        return raw + kHeaderBytes;
    }

    static void Release(void* payload_ptr) noexcept {
        if (payload_ptr == nullptr) {
            return;
        }
        uint8_t* raw = static_cast<uint8_t*>(payload_ptr) - kHeaderBytes;
        uint32_t c = 0;
        std::memcpy(&c, raw, sizeof(c));
        if (c < kNumClasses) {
            auto& cls = Instance().classes_[c];
            std::lock_guard<std::mutex> lk(cls.mu);
            if (cls.count < kClassMaxKeep[c]) {
                cls.ptrs[cls.count++] = static_cast<uint8_t*>(payload_ptr);
                return;
            }
        }
        delete[] raw;
    }

   private:
    struct alignas(64) SizeClass {
        std::mutex mu;
        uint8_t* ptrs[kMaxSlots]{};
        size_t count = 0;
    };
    SizeClass classes_[kNumClasses];
};

}  // namespace brpc_fast::detail
