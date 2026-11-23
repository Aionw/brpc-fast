#pragma once

#include <brpc/closure_guard.h>
#include <brpc/serialized_request.h>
#include <brpc/serialized_response.h>
#include <bthread/countdown_event.h>
#include <butil/object_pool.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/stubs/callback.h>

#include <atomic>
#include <cassert>
#include <functional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace brpc_fast::detail {

using RawMethodHandler = std::function<void(
    ::brpc::Controller*, const ::brpc::SerializedRequest*,
    ::brpc::SerializedResponse*, ::google::protobuf::Closure*)>;

class FastMethodRegistry {
   public:
    virtual ~FastMethodRegistry() = default;
    virtual void RegisterMethod(std::string_view method_name,
                                RawMethodHandler handler) = 0;
};

// Builds (once per process, cached in an internal DescriptorPool) synthetic
// google::protobuf::MethodDescriptors carrying the exact protobuf package,
// service, and method names so baidu_std RpcRequestMeta populates real
// service_name / method_name without needing protoc --cpp_out codegen.
std::vector<const ::google::protobuf::MethodDescriptor*>
RegisterSyntheticServiceDescriptors(
    const std::string& proto_file_name, const std::string& package_name,
    const std::string& service_name,
    const std::vector<std::string>& method_names);

inline void ApplyCallOptions(::brpc::Controller& cntl,
                             const CallOptions& opts) noexcept {
    if (opts.timeout_ms >= 0) {
        cntl.set_timeout_ms(opts.timeout_ms);
    }
    if (opts.max_retry >= 0) {
        cntl.set_max_retry(opts.max_retry);
    }
    if (opts.has_log_id) {
        cntl.set_log_id(opts.log_id);
    }
}

// ---- Pooled server-side dispatch ----

template <FastMessage Req, FastMessage Resp>
struct ServerCallSlot {
    Req req{};
    Resp resp{};
};

template <FastMessage Req, FastMessage Resp, typename Fn>
void DispatchServerMethod(::brpc::Controller* cntl,
                          const ::brpc::SerializedRequest* raw_req,
                          ::brpc::SerializedResponse* raw_resp,
                          ::google::protobuf::Closure* brpc_done, Fn&& fn) {
    constexpr size_t kMaxRetainedReqBytes = 8u * 1024u * 1024u;
    ::brpc::ClosureGuard done_guard(brpc_done);
    auto* slot = ::butil::get_object<ServerCallSlot<Req, Resp>>();
    const bool clear_req_on_return =
        raw_req->serialized_data().size() > kMaxRetainedReqBytes;

    struct SlotReturnGuard {
        ServerCallSlot<Req, Resp>* s;
        bool clear_req;
        ~SlotReturnGuard() {
            if (clear_req) [[unlikely]] {
                s->req.Clear();
            }
            s->resp.Clear();
            ::butil::return_object(s);
        }
    } slot_guard{slot, clear_req_on_return};

    if (!DecodeFromIOBuf(raw_req->serialized_data(), slot->req)) {
        cntl->SetFailed(::brpc::EREQUEST,
                        "brpc_fast: failed to decode request");
        return;
    }

    RpcContext ctx(cntl);
    RpcStatus status = fn(ctx, slot->req, slot->resp);
    if (!status.ok()) {
        cntl->SetFailed(status.code, "%s", status.message.c_str());
        return;
    }
    if (!cntl->Failed()) {
        if (!EncodeToIOBuf(slot->resp, &raw_resp->serialized_data())) {
            cntl->SetFailed(::brpc::ERESPONSE,
                            "brpc_fast: encoded response exceeds size limit");
        }
    }
}

// ---- Pooled client-side async state & invocation helpers ----

template <FastMessage Resp>
class AsyncCallState : public ::google::protobuf::Closure {
   public:
    static constexpr uintptr_t kPending = 0;
    static constexpr uintptr_t kCompleted = 1;

    AsyncCallState() : done_event_(1) {}

    void Prepare(const ::google::protobuf::MethodDescriptor* method,
                 const CallOptions& opts) noexcept {
        cntl_.Reset();
        ApplyCallOptions(cntl_, opts);
        method_ = method;
        status_ = RpcStatus::Ok();
        waiter_.store(kPending, std::memory_order_relaxed);
        ref_count_.store(2, std::memory_order_relaxed);
        done_event_.reset(1);
    }

    ::brpc::Controller& cntl() noexcept { return cntl_; }
    ::brpc::SerializedRequest& raw_req() noexcept { return raw_req_; }
    ::brpc::SerializedResponse& raw_resp() noexcept { return raw_resp_; }

    void Run() override {
        if (cntl_.Failed()) {
            status_ = RpcStatus::Error(cntl_.ErrorCode(), cntl_.ErrorText());
        } else if (!DecodeFromIOBuf(raw_resp_.serialized_data(), resp_)) {
            std::string_view name(method_->full_name());
            status_ = RpcStatus::Error(
                ::brpc::ERESPONSE, "brpc_fast: failed to decode response for " +
                                       std::string(name));
        }

        const uintptr_t prev =
            waiter_.exchange(kCompleted, std::memory_order_acq_rel);
        done_event_.signal();

        std::coroutine_handle<> coro =
            (prev != kPending && prev != kCompleted)
                ? std::coroutine_handle<>::from_address(
                      reinterpret_cast<void*>(prev))
                : std::coroutine_handle<>{};

        Release();
        if (coro) {
            coro.resume();
        }
    }

    bool IsReady() const noexcept {
        return waiter_.load(std::memory_order_acquire) == kCompleted;
    }

    void Wait() {
        if (!IsReady()) {
            done_event_.wait();
        }
    }

    bool TrySuspendCoroutine(std::coroutine_handle<> h) noexcept {
        uintptr_t expected = kPending;
        return waiter_.compare_exchange_strong(
            expected, reinterpret_cast<uintptr_t>(h.address()),
            std::memory_order_acq_rel, std::memory_order_acquire);
    }

    RpcResult<Resp> TakeResult() {
        if (!status_.ok()) {
            return std::move(status_);
        }
        return std::move(resp_);
    }

    void Release() noexcept {
        if (ref_count_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            raw_req_.Clear();
            raw_resp_.Clear();
            resp_.Clear();
            status_ = RpcStatus::Ok();
            method_ = nullptr;
            cntl_.Reset();
            ::butil::return_object(this);
        }
    }

   private:
    ::brpc::Controller cntl_;
    ::brpc::SerializedRequest raw_req_;
    ::brpc::SerializedResponse raw_resp_;
    const ::google::protobuf::MethodDescriptor* method_ = nullptr;
    Resp resp_{};
    RpcStatus status_{};
    std::atomic<uintptr_t> waiter_{kPending};
    std::atomic<int> ref_count_{0};
    ::bthread::CountdownEvent done_event_;
};

template <FastMessage Req, FastMessage Resp>
RpcStatus CallClientMethod(::brpc::ChannelBase* channel,
                           const ::google::protobuf::MethodDescriptor* method,
                           const Req& request, Resp& response,
                           const CallOptions& opts = {}) {
    ::brpc::Controller cntl;
    ApplyCallOptions(cntl, opts);
    ::brpc::SerializedRequest raw_req;
    ::brpc::SerializedResponse raw_resp;
    if (!EncodeToIOBuf(request, &raw_req.serialized_data())) {
        return RpcStatus::Error(
            ::brpc::EREQUEST, "brpc_fast: encoded request exceeds size limit");
    }
    channel->CallMethod(method, &cntl, &raw_req, &raw_resp, nullptr);
    if (cntl.Failed()) {
        return RpcStatus::Error(cntl.ErrorCode(), cntl.ErrorText());
    }
    if (!DecodeFromIOBuf(raw_resp.serialized_data(), response)) {
        std::string_view name(method->full_name());
        return RpcStatus::Error(
            ::brpc::ERESPONSE,
            "brpc_fast: failed to decode response for " + std::string(name));
    }
    return RpcStatus::Ok();
}

template <FastMessage Req, FastMessage Resp>
RpcFuture<Resp> CallClientMethodAsync(
    ::brpc::ChannelBase* channel,
    const ::google::protobuf::MethodDescriptor* method, const Req& request,
    const CallOptions& opts = {}) {
    auto* state = ::butil::get_object<AsyncCallState<Resp>>();
    state->Prepare(method, opts);
    struct AsyncInitGuard {
        AsyncCallState<Resp>* s;
        ~AsyncInitGuard() {
            if (s != nullptr) {
                s->Release();
                s->Release();
            }
        }
    } init_guard{state};

    const ::brpc::CallId call_id = state->cntl().call_id();
    if (!EncodeToIOBuf(request, &state->raw_req().serialized_data())) {
        state->cntl().SetFailed(
            ::brpc::EREQUEST, "brpc_fast: encoded request exceeds size limit");
        init_guard.s = nullptr;
        state->Run();
        return RpcFuture<Resp>(state, call_id);
    }
    init_guard.s = nullptr;
    channel->CallMethod(method, &state->cntl(), &state->raw_req(),
                        &state->raw_resp(), state);
    return RpcFuture<Resp>(state, call_id);
}

}  // namespace brpc_fast::detail

// ---- Out-of-line RpcFuture<Resp> method definitions ----

namespace brpc_fast {

template <FastMessage Resp>
bool RpcFuture<Resp>::ready() const noexcept {
    return state_ != nullptr && state_->IsReady();
}

template <FastMessage Resp>
void RpcFuture<Resp>::Cancel() noexcept {
    if (state_ != nullptr && !state_->IsReady()) {
        ::brpc::StartCancel(call_id_);
    }
}

template <FastMessage Resp>
RpcResult<Resp> RpcFuture<Resp>::Get() {
    if (state_ == nullptr) {
        return RpcStatus::Error(EINVAL,
                                "brpc_fast: RpcFuture already consumed");
    }
    state_->Wait();
    return ConsumeReady();
}

template <FastMessage Resp>
bool RpcFuture<Resp>::await_ready() const noexcept {
    return state_ == nullptr || state_->IsReady();
}

template <FastMessage Resp>
bool RpcFuture<Resp>::await_suspend(std::coroutine_handle<> h) noexcept {
    if (state_ == nullptr) {
        return false;
    }
    // Mark awaited before publishing `h`: once TrySuspendCoroutine succeeds,
    // Run() may resume `h`, execute await_resume(), and destroy this RpcFuture
    // on another thread immediately, so do not touch `this` afterwards.
    awaited_ = true;
    if (state_->TrySuspendCoroutine(h)) {
        return true;
    }
    awaited_ = false;
    return false;
}

template <FastMessage Resp>
RpcResult<Resp> RpcFuture<Resp>::await_resume() {
    if (state_ == nullptr) {
        return RpcStatus::Error(EINVAL,
                                "brpc_fast: RpcFuture already consumed");
    }
    return ConsumeReady();
}

template <FastMessage Resp>
RpcResult<Resp> RpcFuture<Resp>::ConsumeReady() {
    awaited_ = false;
    auto* s = std::exchange(state_, nullptr);
    RpcResult<Resp> result = s->TakeResult();
    s->Release();
    return result;
}

template <FastMessage Resp>
void RpcFuture<Resp>::CancelAndRelease() noexcept {
    if (state_ != nullptr) {
        assert(!awaited_ &&
               "brpc_fast: destroying a coroutine suspended on co_await "
               "RpcFuture is forbidden; save call_id() before co_await, "
               "call ::brpc::StartCancel(saved_call_id), and wait for the "
               "coroutine to resume");
        if (!state_->IsReady()) {
            ::brpc::StartCancel(call_id_);
        }
        state_->Release();
        state_ = nullptr;
        awaited_ = false;
    }
}

}  // namespace brpc_fast
