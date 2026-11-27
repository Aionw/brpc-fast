#pragma once

#include "brpc_fast/codec.h"

#include <brpc/baidu_master_service.h>
#include <brpc/channel_base.h>
#include <brpc/controller.h>
#include <brpc/errno.pb.h>
#include <brpc/server.h>
#include <butil/endpoint.h>

#include <cerrno>
#include <coroutine>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <variant>

namespace brpc_fast {

namespace detail {
class FastMethodRegistry;
template <FastMessage Resp>
class AsyncCallState;
}  // namespace detail

// ---- Public RPC status, result, options, and context ----

struct [[nodiscard]] RpcStatus {
    int code = 0;
    std::string message;

    static RpcStatus Ok() noexcept { return {}; }
    static RpcStatus Error(int err_code, std::string msg = {}) {
        return RpcStatus{err_code == 0 ? ::brpc::EINTERNAL : err_code,
                         std::move(msg)};
    }

    [[nodiscard]] bool ok() const noexcept { return code == 0; }
    explicit operator bool() const noexcept { return ok(); }
};

template <typename T>
class [[nodiscard]] RpcResult {
   public:
    RpcResult(T value) : storage_(std::in_place_index<0>, std::move(value)) {}
    RpcResult(RpcStatus error)
        : storage_(std::in_place_index<1>,
                   error.ok() ? RpcStatus::Error(::brpc::EINTERNAL,
                                                 "unspecified error")
                              : std::move(error)) {}

    [[nodiscard]] bool ok() const noexcept { return storage_.index() == 0; }
    explicit operator bool() const noexcept { return ok(); }

    T& value() & { return std::get<0>(storage_); }
    const T& value() const& { return std::get<0>(storage_); }
    T&& value() && { return std::get<0>(std::move(storage_)); }

    T& operator*() & { return value(); }
    const T& operator*() const& { return value(); }
    T&& operator*() && { return std::move(value()); }
    T* operator->() { return &value(); }
    const T* operator->() const { return &value(); }

    const RpcStatus& error() const& { return std::get<1>(storage_); }
    RpcStatus& error() & { return std::get<1>(storage_); }

   private:
    std::variant<T, RpcStatus> storage_;
};

struct CallOptions {
    int64_t timeout_ms = -1;  // -1 uses channel default
    int max_retry = -1;       // -1 uses channel default
    uint64_t log_id = 0;
    bool has_log_id = false;
};

// Safe read-only server-side RPC metadata passed to service handlers.
class RpcContext {
   public:
    explicit RpcContext(::brpc::Controller* cntl) noexcept : cntl_(cntl) {}

    [[nodiscard]] butil::EndPoint remote_side() const noexcept {
        return cntl_->remote_side();
    }
    [[nodiscard]] butil::EndPoint local_side() const noexcept {
        return cntl_->local_side();
    }
    [[nodiscard]] uint64_t log_id() const noexcept { return cntl_->log_id(); }
    [[nodiscard]] bool has_log_id() const noexcept {
        return cntl_->has_log_id();
    }
    [[nodiscard]] bool is_canceled() const noexcept {
        return cntl_->IsCanceled();
    }
    [[nodiscard]] const butil::IOBuf& request_attachment() const noexcept {
        return cntl_->request_attachment();
    }
    [[nodiscard]] butil::IOBuf& response_attachment() const noexcept {
        return cntl_->response_attachment();
    }

   private:
    ::brpc::Controller* cntl_;
};

// Move-only, non-thread-safe handle for an in-flight asynchronous RPC.
// Supports both blocking `.Get()` (bthread-friendly) and C++20 `co_await`.
// Dropping an unconsumed, non-awaited RpcFuture cancels the RPC and releases
// state safely.
//
// Coroutine lifetime & cancellation contract:
// - Once a coroutine suspends on `co_await future`, destroying the suspended
//   coroutine frame before `co_await` resumes is forbidden (undefined behavior,
//   guarded by debug assert).
// - `Cancel()` is not thread-safe and must not be called concurrently with
//   `co_await` resumption or `Get()`. To cancel an in-flight `co_await` from
//   another thread, save `call_id()` before `co_await`, call
//   `::brpc::StartCancel(saved_call_id)`, and let the coroutine resume normally
//   with an error result.
template <FastMessage Resp>
class [[nodiscard]] RpcFuture {
   public:
    RpcFuture() = default;
    RpcFuture(detail::AsyncCallState<Resp>* state,
              ::brpc::CallId call_id) noexcept
        : state_(state), call_id_(call_id) {}

    RpcFuture(const RpcFuture&) = delete;
    RpcFuture& operator=(const RpcFuture&) = delete;

    RpcFuture(RpcFuture&& other) noexcept
        : state_(std::exchange(other.state_, nullptr)),
          call_id_(other.call_id_),
          awaited_(std::exchange(other.awaited_, false)) {}

    RpcFuture& operator=(RpcFuture&& other) noexcept {
        if (this != &other) {
            CancelAndRelease();
            state_ = std::exchange(other.state_, nullptr);
            call_id_ = other.call_id_;
            awaited_ = std::exchange(other.awaited_, false);
        }
        return *this;
    }

    ~RpcFuture() { CancelAndRelease(); }

    [[nodiscard]] bool valid() const noexcept { return state_ != nullptr; }
    [[nodiscard]] bool ready() const noexcept;
    [[nodiscard]] ::brpc::CallId call_id() const noexcept { return call_id_; }

    void Cancel() noexcept;

    // Blocks the current bthread/thread until completion and consumes the
    // result.
    RpcResult<Resp> Get();

    // C++20 coroutine awaiter interface (`co_await stub.MethodAsync(req)`).
    bool await_ready() const noexcept;
    bool await_suspend(std::coroutine_handle<> h) noexcept;
    RpcResult<Resp> await_resume();

   private:
    RpcResult<Resp> ConsumeReady();
    void CancelAndRelease() noexcept;

    detail::AsyncCallState<Resp>* state_ = nullptr;
    ::brpc::CallId call_id_{INVALID_BTHREAD_ID};
    bool awaited_ = false;
};

// ---- Service hosting & lifecycle ----

// Base class for all generated fast bRPC services.
class FastServiceBase {
   public:
    virtual ~FastServiceBase() = default;
    virtual std::string_view ServiceFullName() const = 0;
    virtual std::string_view ServiceShortName() const = 0;
    virtual void RegisterMethods(detail::FastMethodRegistry& registry) = 0;
};

// Multi-service router implementing brpc::BaiduMasterService.
// Prefer FastServer for RAII server/service lifecycle management; use
// FastMasterService directly only when integrating with an existing
// brpc::Server.
class FastMasterService : public ::brpc::BaiduMasterService {
   public:
    FastMasterService();
    ~FastMasterService() override;

    // Register and take ownership of a FastServiceBase instance.
    // Not thread-safe with ProcessRpcRequest; must only be called before
    // brpc::Server::Start().
    int AddService(std::unique_ptr<FastServiceBase> service);

    // Non-owning overload; caller must guarantee `service` outlives the server
    // and only register services before brpc::Server::Start().
    int AddService(FastServiceBase* service);

    void ProcessRpcRequest(::brpc::Controller* cntl,
                           const ::brpc::SerializedRequest* request,
                           ::brpc::SerializedResponse* response,
                           ::google::protobuf::Closure* done) override;

    void Describe(std::ostream& os,
                  const ::brpc::DescribeOptions& options) const override;

   private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

// RAII server wrapper enforcing AddService -> Start -> Stop/Join -> Service
// destruction ordering by construction.
class FastServer {
   public:
    FastServer() = default;
    ~FastServer();

    FastServer(const FastServer&) = delete;
    FastServer& operator=(const FastServer&) = delete;

    RpcStatus AddService(std::unique_ptr<FastServiceBase> service);

    RpcStatus Start(const std::string& listen_addr,
                    ::brpc::ServerOptions options = {});

    void Stop(int closewait_ms = 0);

    [[nodiscard]] butil::EndPoint listen_address() const {
        return server_.listen_address();
    }
    [[nodiscard]] std::string listen_address_str() const;
    [[nodiscard]] bool is_running() const noexcept { return started_; }
    [[nodiscard]] const FastMasterService& master_service() const noexcept {
        return master_;
    }

   private:
    // Declared BEFORE server_ so server_ stops and joins all workers before
    // master_ (and its owned FastServiceBase instances) are destroyed.
    FastMasterService master_;
    ::brpc::Server server_;
    bool started_ = false;
};

}  // namespace brpc_fast

#include "brpc_fast/detail/rpc_runtime.h"
