#pragma once

#include <memory>
#include <optional>
#include <type_traits>
#include <utility>

namespace brpc_fast {

// Value-semantic optional box used by codegen for cyclic (self- or mutually-
// recursive) singular message fields so incomplete types compile cleanly while
// retaining std::optional-like ergonomics. Non-cyclic fields use std::optional.
template <typename T>
class IndirectOptional {
   public:
    using value_type = T;

    IndirectOptional() noexcept = default;
    IndirectOptional(std::nullopt_t) noexcept {}
    IndirectOptional(const T& v) : ptr_(std::make_unique<T>(v)) {}
    IndirectOptional(T&& v) : ptr_(std::make_unique<T>(std::move(v))) {}
    IndirectOptional(const IndirectOptional& other) {
        if (other.ptr_) {
            ptr_ = std::make_unique<T>(*other.ptr_);
        }
    }
    IndirectOptional& operator=(const IndirectOptional& other) {
        if (this != &other) {
            if (other.ptr_) {
                if (ptr_) {
                    *ptr_ = *other.ptr_;
                } else {
                    ptr_ = std::make_unique<T>(*other.ptr_);
                }
            } else {
                ptr_.reset();
            }
        }
        return *this;
    }
    IndirectOptional(IndirectOptional&&) noexcept = default;
    IndirectOptional& operator=(IndirectOptional&&) noexcept = default;

    IndirectOptional& operator=(std::nullopt_t) noexcept {
        ptr_.reset();
        return *this;
    }
    IndirectOptional& operator=(const T& v) {
        if (ptr_) {
            *ptr_ = v;
        } else {
            ptr_ = std::make_unique<T>(v);
        }
        return *this;
    }
    IndirectOptional& operator=(T&& v) {
        if (ptr_) {
            *ptr_ = std::move(v);
        } else {
            ptr_ = std::make_unique<T>(std::move(v));
        }
        return *this;
    }

    [[nodiscard]] bool has_value() const noexcept { return ptr_ != nullptr; }
    explicit operator bool() const noexcept { return has_value(); }

    T& value() & {
        if (!ptr_) throw std::bad_optional_access();
        return *ptr_;
    }
    const T& value() const& {
        if (!ptr_) throw std::bad_optional_access();
        return *ptr_;
    }
    T&& value() && {
        if (!ptr_) throw std::bad_optional_access();
        return std::move(*ptr_);
    }
    T& operator*() & noexcept { return *ptr_; }
    const T& operator*() const& noexcept { return *ptr_; }
    T&& operator*() && noexcept { return std::move(*ptr_); }
    T* operator->() noexcept { return ptr_.get(); }
    const T* operator->() const noexcept { return ptr_.get(); }

    template <typename... Args>
    T& emplace(Args&&... args) {
        if constexpr (sizeof...(Args) == 0) {
            if (ptr_) {
                ptr_->Clear();
                return *ptr_;
            }
        }
        ptr_ = std::make_unique<T>(std::forward<Args>(args)...);
        return *ptr_;
    }

    void reset() noexcept { ptr_.reset(); }

    bool operator==(const IndirectOptional& other) const {
        if (!ptr_ && !other.ptr_) return true;
        if (!ptr_ || !other.ptr_) return false;
        return *ptr_ == *other.ptr_;
    }

   private:
    std::unique_ptr<T> ptr_;
};

// Always-initialized value-semantic heap box used for cyclic oneof message
// branches inside std::variant and cyclic map values.
template <typename T>
class IndirectBox {
   public:
    using value_type = T;

    IndirectBox() : ptr_(std::make_unique<T>()) {}
    IndirectBox(const T& v) : ptr_(std::make_unique<T>(v)) {}
    IndirectBox(T&& v) : ptr_(std::make_unique<T>(std::move(v))) {}
    template <typename... Args>
        requires(sizeof...(Args) >= 1 &&
                 !(sizeof...(Args) == 1 &&
                   (std::is_same_v<std::remove_cvref_t<Args>, IndirectBox> ||
                    ...)))
    explicit IndirectBox(Args&&... args)
        : ptr_(std::make_unique<T>(std::forward<Args>(args)...)) {}

    IndirectBox(const IndirectBox& other)
        : ptr_(other.ptr_ ? std::make_unique<T>(*other.ptr_)
                          : std::make_unique<T>()) {}
    IndirectBox& operator=(const IndirectBox& other) {
        if (this != &other) {
            if (other.ptr_) {
                if (ptr_) {
                    *ptr_ = *other.ptr_;
                } else {
                    ptr_ = std::make_unique<T>(*other.ptr_);
                }
            } else {
                ptr_ = std::make_unique<T>();
            }
        }
        return *this;
    }
    IndirectBox(IndirectBox&&) noexcept = default;
    IndirectBox& operator=(IndirectBox&&) noexcept = default;

    [[nodiscard]] bool has_value() const noexcept { return ptr_ != nullptr; }

    T& get() & {
        if (!ptr_) [[unlikely]] {
            ptr_ = std::make_unique<T>();
        }
        return *ptr_;
    }
    const T& get() const& {
        if (ptr_) [[likely]] {
            return *ptr_;
        }
        return DefaultEmpty();
    }
    T& operator*() & { return get(); }
    const T& operator*() const& { return get(); }
    T* operator->() { return &get(); }
    const T* operator->() const { return &get(); }

    bool operator==(const IndirectBox& other) const {
        if (ptr_ == other.ptr_) return true;
        return get() == other.get();
    }

   private:
    static const T& DefaultEmpty() {
        static const T kEmpty{};
        return kEmpty;
    }

    std::unique_ptr<T> ptr_;
};

}  // namespace brpc_fast
