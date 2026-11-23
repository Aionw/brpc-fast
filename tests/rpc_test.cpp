#include <brpc/channel.h>
#include <brpc/interceptor.h>
#include <bthread/bthread.h>
#include <bthread/countdown_event.h>
#include <gtest/gtest.h>

#include "test_schema.fast.h"

#include <atomic>
#include <coroutine>
#include <memory>
#include <optional>
#include <sstream>
#include <string>

namespace brpc_fast::test {
namespace {

using namespace ::brpc_fast_pb::brpc_fast::test;

class EchoServiceImpl final : public EchoService {
   public:
    RpcStatus Echo(const RpcContext& ctx, const EchoRequest& request,
                   EchoResponse& response) override {
        response.message = "echo:" + request.message;
        response.seq = request.seq + 1;
        response.payload = request.payload;
        if (ctx.has_log_id()) {
            last_log_id.store(ctx.log_id(), std::memory_order_relaxed);
        }
        if (!ctx.request_attachment().empty()) {
            ctx.response_attachment().append("att:");
            ctx.response_attachment().append(ctx.request_attachment());
        }
        return RpcStatus::Ok();
    }

    RpcStatus SlowEcho(const RpcContext& /*ctx*/, const EchoRequest& request,
                       EchoResponse& response) override {
        slow_entered.signal();
        slow_release.wait();
        response.message = "slow:" + request.message;
        response.seq = request.seq;
        return RpcStatus::Ok();
    }

    RpcStatus FailCall(const RpcContext& /*ctx*/, const EchoRequest& request,
                       EchoResponse& /*response*/) override {
        return RpcStatus::Error(::brpc::EINTERNAL,
                                "custom-error:" + request.message);
    }

    std::atomic<uint64_t> last_log_id{0};
    bthread::CountdownEvent slow_entered{1};
    bthread::CountdownEvent slow_release{1};

    RpcStatus BatchGetReplica(const RpcContext& /*ctx*/,
                              const BatchGetReplicaRequest& request,
                              BatchGetReplicaResponse& response) override {
        response.results.reserve(request.keys.size());
        for (size_t i = 0; i < request.keys.size(); ++i) {
            KeyReplicaResult res;
            res.error_code = 0;
            res.lease_ttl_ms = 3000;
            res.object_checksum = static_cast<uint64_t>(request.keys[i].size());

            ReplicaDesc desc;
            desc.replica_id = i + 1;
            desc.status = ReplicaStatus::REPLICA_COMPLETE;
            auto& mem = desc.mutable_memory();
            mem.buffer_address = 0x100000ULL + i * 4096;
            mem.size = 65536;
            mem.protocol = "rdma";
            mem.transport_endpoint = "127.0.0.1:9000";
            mem.hca_names = {"mlx5_0"};
            res.replicas.push_back(std::move(desc));
            response.results.push_back(std::move(res));
        }
        return RpcStatus::Ok();
    }
};

class HealthServiceImpl final : public HealthService {
   public:
    RpcStatus Ping(const RpcContext& /*ctx*/, const PingRequest& request,
                   PingResponse& response) override {
        response.view_version = request.client_hi ^ request.client_lo;
        response.ok = true;
        return RpcStatus::Ok();
    }
};

// Minimal C++20 eager coroutine task used to verify `co_await` on RpcFuture.
template <typename T>
struct SyncTask {
    struct promise_type {
        std::optional<T> value;
        bthread::CountdownEvent done{1};

        SyncTask get_return_object() {
            return SyncTask{
                std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_never initial_suspend() noexcept { return {}; }
        struct FinalAwaiter {
            bool await_ready() const noexcept { return false; }
            void await_suspend(
                std::coroutine_handle<promise_type> h) const noexcept {
                // Coroutine is now suspended; signaling `done` may unblock
                // ~SyncTask() to call handle.destroy(), so do not touch `h`
                // afterwards.
                h.promise().done.signal();
            }
            void await_resume() const noexcept {}
        };
        FinalAwaiter final_suspend() noexcept { return {}; }
        void return_value(T v) { value.emplace(std::move(v)); }
        void unhandled_exception() { std::terminate(); }
    };

    std::coroutine_handle<promise_type> handle;

    ~SyncTask() {
        if (handle) {
            handle.promise().done.wait();
            handle.destroy();
        }
    }

    T Wait() {
        handle.promise().done.wait();
        return std::move(*handle.promise().value);
    }
};

class BrpcFastRpcTest : public ::testing::Test {
   protected:
    void SetUp() override {
        auto echo = std::make_unique<EchoServiceImpl>();
        echo_svc_ = echo.get();
        ASSERT_TRUE(server_.AddService(std::move(echo)).ok());
        ASSERT_TRUE(
            server_.AddService(std::make_unique<HealthServiceImpl>()).ok());

        brpc::ServerOptions options;
        options.has_builtin_services = true;
        ASSERT_TRUE(server_.Start("127.0.0.1:0", options).ok());

        brpc::ChannelOptions ch_opts;
        ch_opts.protocol = brpc::PROTOCOL_BAIDU_STD;
        ch_opts.timeout_ms = 5000;
        ASSERT_EQ(channel_.Init(server_.listen_address_str().c_str(), &ch_opts),
                  0);
    }

    EchoServiceImpl* echo_svc_ = nullptr;
    FastServer server_;
    brpc::Channel channel_;
};

TEST_F(BrpcFastRpcTest, SyncEchoAndBatchGetReplica) {
    EchoService_Stub stub(&channel_);

    {
        EchoResponse resp;
        auto st = stub.Echo(
            {
                .message = "hello",
                .seq = 41,
                .payload = std::string("\x01\x02\x03\x04", 4),
            },
            resp);
        ASSERT_TRUE(st.ok()) << st.message;
        EXPECT_EQ(resp.message, "echo:hello");
        EXPECT_EQ(resp.seq, 42);
        ASSERT_TRUE(resp.payload.has_value());
        EXPECT_EQ(*resp.payload, std::string("\x01\x02\x03\x04", 4));
    }

    {
        BatchGetReplicaResponse resp;
        auto st = stub.BatchGetReplica(
            {
                .tenant_id = "tenant-a",
                .keys = {"k1", "key_two", "key_three"},
            },
            resp);
        ASSERT_TRUE(st.ok()) << st.message;
        ASSERT_EQ(resp.results.size(), 3u);
        EXPECT_EQ(resp.results[1].object_checksum, 7u);
        ASSERT_EQ(resp.results[1].replicas.size(), 1u);
        EXPECT_TRUE(resp.results[1].replicas[0].has_memory());
        EXPECT_EQ(resp.results[1].replicas[0].memory().protocol, "rdma");
    }
}

TEST_F(BrpcFastRpcTest, MultiServiceRoutingDescribeAndStartGuard) {
    HealthService_Stub health_stub(&channel_);
    PingResponse resp;
    auto st =
        health_stub.Ping({.client_hi = 0xAA00, .client_lo = 0x0055}, resp);
    ASSERT_TRUE(st.ok()) << st.message;
    EXPECT_TRUE(resp.ok);
    EXPECT_EQ(resp.view_version, 0xAA55u);

    // Adding a service after Start() must be rejected cleanly.
    EXPECT_FALSE(
        server_.AddService(std::make_unique<HealthServiceImpl>()).ok());

    std::ostringstream oss;
    brpc::DescribeOptions desc_opts;
    server_.master_service().Describe(oss, desc_opts);
    std::string text = oss.str();
    EXPECT_NE(text.find("brpc_fast.test.EchoService"), std::string::npos);
    EXPECT_NE(text.find("BatchGetReplica"), std::string::npos);
    EXPECT_NE(text.find("brpc_fast.test.HealthService"), std::string::npos);
    EXPECT_NE(text.find("Ping"), std::string::npos);
}

TEST_F(BrpcFastRpcTest, AsyncFutureGetCoAwaitAndDropSafety) {
    EchoService_Stub stub(&channel_);

    // 1. Concurrent fan-out with RpcFuture::Get()
    auto f1 = stub.EchoAsync({.message = "async-1", .seq = 99});
    auto f2 = stub.EchoAsync({.message = "async-2", .seq = 199});
    auto r1 = f1.Get();
    auto r2 = f2.Get();
    ASSERT_TRUE(r1.ok()) << r1.error().message;
    EXPECT_EQ(r1->message, "echo:async-1");
    EXPECT_EQ(r1->seq, 100);
    ASSERT_TRUE(r2.ok()) << r2.error().message;
    EXPECT_EQ(r2->message, "echo:async-2");
    EXPECT_EQ(r2->seq, 200);

    // 2. C++20 co_await on RpcFuture
    auto coro = [&]() -> SyncTask<RpcResult<EchoResponse>> {
        co_return co_await stub.EchoAsync({.message = "coro-hello", .seq = 7});
    };
    auto r3 = coro().Wait();
    ASSERT_TRUE(r3.ok()) << r3.error().message;
    EXPECT_EQ(r3->message, "echo:coro-hello");
    EXPECT_EQ(r3->seq, 8);

    // 3. Dropping unconsumed RpcFutures cancels and cleans up without UB.
    for (int i = 0; i < 8; ++i) {
        auto dropped = stub.EchoAsync({.message = "drop-me", .seq = i});
        (void)dropped;
    }
}

TEST_F(BrpcFastRpcTest, UnimplementedMethodReturnsEnoMethod) {
    EchoService_Stub stub(&channel_);
    EchoResponse resp;
    auto st = stub.UnimplementedCall({}, resp);
    EXPECT_FALSE(st.ok());
    EXPECT_EQ(st.code, brpc::ENOMETHOD);
}

TEST_F(BrpcFastRpcTest, ReusedResponseClearsPreviousStateAndConcurrentCalls) {
    EchoService_Stub stub(&channel_);
    BatchGetReplicaResponse reused_resp;

    // First call with 3 keys, second call with 1 key reusing the same resp.
    {
        auto st = stub.BatchGetReplica(
            {.tenant_id = "t1", .keys = {"a", "bb", "ccc"}}, reused_resp);
        ASSERT_TRUE(st.ok()) << st.message;
        ASSERT_EQ(reused_resp.results.size(), 3u);
    }
    {
        auto st = stub.BatchGetReplica(
            {.tenant_id = "t1", .keys = {"only_one"}}, reused_resp);
        ASSERT_TRUE(st.ok()) << st.message;
        ASSERT_EQ(reused_resp.results.size(), 1u);
        EXPECT_EQ(reused_resp.results[0].object_checksum, 8u);
    }

    // Concurrent multi-bthread RPCs mixing sync reuse and async future calls
    // must complete without errors or cross-talk.
    constexpr int kWorkers = 8;
    constexpr int kCallsPerWorker = 50;
    std::atomic<int> failures{0};

    struct WorkerArg {
        brpc::Channel* channel;
        int worker_id;
        std::atomic<int>* failures;
    };
    auto worker_fn = [](void* raw) -> void* {
        auto* arg = static_cast<WorkerArg*>(raw);
        EchoService_Stub local_stub(arg->channel);
        EchoResponse reused;
        for (int i = 0; i < kCallsPerWorker; ++i) {
            EchoRequest req;
            req.message =
                "w" + std::to_string(arg->worker_id) + "_" + std::to_string(i);
            req.seq = arg->worker_id * 1000 + i;
            if ((i & 1) == 0) {
                req.payload = std::string(16, 'p');
            }
            if ((i & 3) == 0) {
                auto res = local_stub.EchoAsync(req).Get();
                if (!res.ok() || res->message != ("echo:" + req.message) ||
                    res->seq != req.seq + 1 ||
                    res->payload.has_value() != req.payload.has_value()) {
                    arg->failures->fetch_add(1);
                }
            } else {
                auto st = local_stub.Echo(req, reused);
                if (!st.ok() || reused.message != ("echo:" + req.message) ||
                    reused.seq != req.seq + 1 ||
                    reused.payload.has_value() != req.payload.has_value()) {
                    arg->failures->fetch_add(1);
                }
            }
        }
        return nullptr;
    };

    bthread_t tids[kWorkers];
    WorkerArg args[kWorkers];
    for (int i = 0; i < kWorkers; ++i) {
        args[i] = WorkerArg{&channel_, i, &failures};
        ASSERT_EQ(
            bthread_start_background(&tids[i], nullptr, worker_fn, &args[i]),
            0);
    }
    for (int i = 0; i < kWorkers; ++i) {
        bthread_join(tids[i], nullptr);
    }
    EXPECT_EQ(failures.load(), 0);
}

TEST_F(BrpcFastRpcTest, CallOptionsContextAndCustomErrorPropagation) {
    EchoService_Stub stub(&channel_);

    CallOptions opts;
    opts.timeout_ms = 3000;
    opts.max_retry = 0;
    opts.log_id = 0xCAFE1234ULL;
    opts.has_log_id = true;

    EchoResponse resp;
    auto st =
        stub.Echo({.message = "ctx-check", .seq = 5, .payload = std::nullopt},
                  resp, opts);
    ASSERT_TRUE(st.ok()) << st.message;
    EXPECT_EQ(echo_svc_->last_log_id.load(std::memory_order_relaxed),
              0xCAFE1234ULL);

    auto err = stub.FailCall(
        {.message = "boom", .seq = 0, .payload = std::nullopt}, resp, opts);
    EXPECT_FALSE(err.ok());
    EXPECT_EQ(err.code, ::brpc::EINTERNAL);
    EXPECT_NE(err.message.find("custom-error:boom"), std::string::npos);
}

TEST_F(BrpcFastRpcTest, CancelSuspendedCoroutineResumesWithError) {
    EchoService_Stub stub(&channel_);
    auto fut = stub.SlowEchoAsync(
        {.message = "cancel-coro", .seq = 0, .payload = std::nullopt});
    const brpc::CallId call_id = fut.call_id();

    auto start =
        [&](RpcFuture<EchoResponse> f) -> SyncTask<RpcResult<EchoResponse>> {
        co_return co_await std::move(f);
    };
    auto task = start(std::move(fut));

    // Wait until the server is actively executing SlowEcho while the coroutine
    // is suspended on co_await RpcFuture.
    echo_svc_->slow_entered.wait();

    // Cooperative cancellation via StartCancel(call_id) resumes the suspended
    // coroutine cleanly with an error instead of destroying the frame early.
    brpc::StartCancel(call_id);
    auto cancelled = task.Wait();
    EXPECT_FALSE(cancelled.ok());
    EXPECT_EQ(cancelled.error().code, ECANCELED);

    // Release the server handler so SlowEcho exits cleanly.
    echo_svc_->slow_release.signal();

    // Follow-up RPC confirms the channel & server remain healthy.
    EchoResponse resp;
    ASSERT_TRUE(
        stub.Echo(
                {.message = "after-cancel", .seq = 1, .payload = std::nullopt},
                resp)
            .ok());
    EXPECT_EQ(resp.message, "echo:after-cancel");
}

TEST(BrpcFastServiceRegistryTest,
     PackagelessServiceAllowedAfterPackagedSameShortName) {
    class DummyService final : public FastServiceBase {
       public:
        DummyService(std::string full, std::string short_n)
            : full_(std::move(full)), short_(std::move(short_n)) {}
        std::string_view ServiceFullName() const override { return full_; }
        std::string_view ServiceShortName() const override { return short_; }
        void RegisterMethods(detail::FastMethodRegistry&) override {}

       private:
        std::string full_;
        std::string short_;
    };

    FastMasterService master;
    EXPECT_EQ(master.AddService(
                  std::make_unique<DummyService>("pkg1.Shared", "Shared")),
              0);
    // Packageless service with the same short name must NOT be rejected.
    EXPECT_EQ(
        master.AddService(std::make_unique<DummyService>("Shared", "Shared")),
        0);
    // Duplicate full name must still be rejected.
    EXPECT_EQ(master.AddService(
                  std::make_unique<DummyService>("pkg1.Shared", "Shared")),
              -1);
    // Second packaged service with the same short name succeeds (short alias
    // becomes ambiguous, full names remain routable).
    EXPECT_EQ(master.AddService(
                  std::make_unique<DummyService>("pkg2.Shared", "Shared")),
              0);

    // FastServer rejects caller-provided baidu_master_service and interceptor.
    FastServer srv;
    brpc::ServerOptions opts;
    opts.baidu_master_service = &master;
    EXPECT_FALSE(srv.Start("127.0.0.1:0", opts).ok());

    class DummyInterceptor final : public brpc::Interceptor {
       public:
        bool Accept(const brpc::Controller*, int&,
                    std::string&) const override {
            return true;
        }
    };
    DummyInterceptor interceptor;
    brpc::ServerOptions interceptor_opts;
    interceptor_opts.interceptor = &interceptor;
    EXPECT_FALSE(srv.Start("127.0.0.1:0", interceptor_opts).ok());
}

}  // namespace
}  // namespace brpc_fast::test
