#include "bench_schema.fast.h"

#include <brpc/channel.h>
#include <brpc/protocol.h>
#include <bthread/bthread.h>
#include <butil/iobuf.h>
#include <gflags/gflags.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <vector>

DEFINE_string(mode, "all", "Benchmark mode: all, codec, or rpc");
DEFINE_uint64(batch_size, 10000, "Batch size for large batch scenarios");
DEFINE_uint64(key_len, 128, "Key length in bytes for batch requests");
DEFINE_uint64(replicas_per_key, 2, "Replica descriptors per key");
DEFINE_uint64(concurrency, 16, "Concurrent bthread workers for RPC benchmarks");
DEFINE_uint64(warmup_ms, 200, "Warmup duration in ms for RPC benchmarks");
DEFINE_uint64(duration_ms, 1500, "Measurement duration in ms per RPC scenario");
DEFINE_string(label, "run", "Label column written to CSV output");
DEFINE_string(csv_path, "", "Optional path to export CSV results");

namespace fast_msg = ::brpc_fast_pb::brpc_fast::bench_proto;

namespace {

using SteadyClock = std::chrono::steady_clock;

struct LatencySummary {
    size_t count = 0;
    double avg_ns = 0.0;
    double p50_ns = 0.0;
    double p99_ns = 0.0;
    double p999_ns = 0.0;
};

struct CodecRow {
    std::string scenario;
    std::string variant;
    size_t wire_bytes = 0;
    LatencySummary enc;
    LatencySummary dec;
    LatencySummary rt;
    double mb_s = 0.0;
};

struct RpcRow {
    std::string scenario;
    std::string traffic;
    size_t ops = 0;
    size_t errors = 0;
    double qps = 0.0;
    double mb_s = 0.0;
    LatencySummary lat;
};

double PercentileSorted(const std::vector<double>& sorted, double p) {
    if (sorted.empty()) return 0.0;
    double idx = p * static_cast<double>(sorted.size() - 1);
    size_t lo = static_cast<size_t>(std::floor(idx));
    size_t hi = static_cast<size_t>(std::ceil(idx));
    if (lo == hi) return sorted[lo];
    double frac = idx - static_cast<double>(lo);
    return sorted[lo] * (1.0 - frac) + sorted[hi] * frac;
}

LatencySummary Summarize(std::vector<double> samples_ns) {
    LatencySummary s;
    s.count = samples_ns.size();
    if (samples_ns.empty()) return s;
    s.avg_ns = std::accumulate(samples_ns.begin(), samples_ns.end(), 0.0) /
               static_cast<double>(s.count);
    std::sort(samples_ns.begin(), samples_ns.end());
    s.p50_ns = PercentileSorted(samples_ns, 0.50);
    s.p99_ns = PercentileSorted(samples_ns, 0.99);
    s.p999_ns = PercentileSorted(samples_ns, 0.999);
    return s;
}

std::string FormatNs(double ns) {
    std::ostringstream oss;
    oss << std::fixed;
    if (ns >= 1e6) {
        oss << std::setprecision(2) << (ns / 1e6) << " ms";
    } else if (ns >= 1e3) {
        oss << std::setprecision(2) << (ns / 1e3) << " us";
    } else {
        oss << std::setprecision(1) << ns << " ns";
    }
    return oss.str();
}

std::string FormatBytes(size_t bytes) {
    std::ostringstream oss;
    oss << std::fixed;
    if (bytes >= 1024 * 1024) {
        oss << std::setprecision(2)
            << (static_cast<double>(bytes) / (1024.0 * 1024.0)) << " MiB";
    } else if (bytes >= 1024) {
        oss << std::setprecision(1) << (static_cast<double>(bytes) / 1024.0)
            << " KiB";
    } else {
        oss << bytes << " B";
    }
    return oss.str();
}

double CalcMbPerSec(size_t bytes, double avg_ns) {
    if (avg_ns <= 0.0) return 0.0;
    return (static_cast<double>(bytes) / (1024.0 * 1024.0)) / (avg_ns / 1e9);
}

std::string MakeKey(size_t idx, size_t key_len) {
    std::string suffix = std::to_string(idx);
    if (key_len <= suffix.size() + 4) return "key_" + suffix;
    std::string k(key_len, 'k');
    k[0] = 'm';
    k[1] = 'c';
    k[2] = '_';
    k.replace(key_len - suffix.size(), suffix.size(), suffix);
    return k;
}

fast_msg::KeyReplicaResult MakeKeyResult(size_t i, size_t replicas_per_key) {
    fast_msg::KeyReplicaResult r;
    r.error_code = 0;
    r.lease_ttl_ms = 5000;
    r.object_checksum = 0xABCDEF000000ULL + i;
    r.replicas.resize(replicas_per_key);
    for (size_t j = 0; j < replicas_per_key; ++j) {
        auto& d = r.replicas[j];
        d.replica_id = i * replicas_per_key + j;
        d.status = fast_msg::ReplicaStatus::REPLICA_COMPLETE;
        auto& mem = d.mutable_memory();
        mem.buffer_address =
            0x7f0000000000ULL + (i * replicas_per_key + j) * 4096;
        mem.size = 1048576;
        mem.protocol = "rdma";
        mem.transport_endpoint = "10.0.0.10:15000";
        mem.numa_node = static_cast<int32_t>(j & 1);
        mem.hca_names = {"mlx5_0", "mlx5_1"};
    }
    return r;
}

fast_msg::BatchGetReplicaResponse MakeBatchGetResp(size_t batch_size,
                                                   size_t replicas_per_key) {
    fast_msg::BatchGetReplicaResponse resp;
    resp.results.reserve(batch_size);
    for (size_t i = 0; i < batch_size; ++i) {
        resp.results.push_back(MakeKeyResult(i, replicas_per_key));
    }
    return resp;
}

fast_msg::BatchPutStartRequest MakeBatchPutReq(size_t batch_size,
                                               size_t key_len,
                                               size_t replicas_per_key) {
    fast_msg::BatchPutStartRequest req;
    req.client_hi = 0x1122334455667788ULL;
    req.client_lo = 0x99AABBCCDDEEFF00ULL;
    req.tenant_id = "tenant_prod_01";
    req.replica_num = static_cast<uint32_t>(replicas_per_key);
    req.keys.reserve(batch_size);
    req.slice_lengths.reserve(batch_size);
    for (size_t i = 0; i < batch_size; ++i) {
        req.keys.push_back(MakeKey(i, key_len));
        req.slice_lengths.push_back(4096ULL * (1 + (i & 15)));
    }
    return req;
}

fast_msg::BatchCompactReplicaResponse MakeBatchCompactResp(
    size_t batch_size, size_t replicas_per_key) {
    fast_msg::BatchCompactReplicaResponse resp;
    resp.key_count = static_cast<uint32_t>(batch_size);
    resp.lease_ttl_ms = 5000;
    resp.endpoints = {"10.0.0.10:15000", "10.0.0.11:15000", "10.0.0.12:15000",
                      "10.0.0.13:15000"};
    resp.protocols = {"rdma"};
    const size_t total = batch_size * replicas_per_key;
    resp.memory_replicas.resize(total);
    for (size_t i = 0; i < total; ++i) {
        auto& r = resp.memory_replicas[i];
        r.replica_id = i + 1;
        r.status = fast_msg::ReplicaStatus::REPLICA_COMPLETE;
        r.buffer_address = 0x7f0000000000ULL + i * 4096ULL;
        r.size = 1048576ULL;
        r.endpoint_idx = static_cast<uint32_t>(i & 3u);
        r.protocol_idx = 0u;
    }
    return resp;
}

fast_msg::BatchExistKeyResponse MakeBatchExistResp(size_t batch_size) {
    fast_msg::BatchExistKeyResponse resp;
    resp.error_codes.assign(batch_size, 0);
    resp.exists.resize(batch_size);
    for (size_t i = 0; i < batch_size; ++i) {
        resp.exists[i] = ((i * 37 + 1) % 5) != 0;
    }
    return resp;
}

template <typename MsgT>
void RunCodecBench(const std::string& scenario, const MsgT& src, size_t iters,
                   std::vector<CodecRow>* out_rows) {
    butil::IOBuf sample;
    src.EncodeTo(&sample);
    const size_t wire_bytes = sample.size();
    const size_t warmup = std::max<size_t>(2, iters / 10);

    for (size_t i = 0; i < warmup; ++i) {
        butil::IOBuf out;
        src.EncodeTo(&out);
        MsgT dst;
        (void)dst.DecodeFrom(out);
    }

    auto measure = [&](const char* variant, bool reuse) {
        std::vector<double> enc_ns, dec_ns, rt_ns;
        enc_ns.reserve(iters);
        dec_ns.reserve(iters);
        rt_ns.reserve(iters);

        MsgT reused;
        if (reuse) (void)reused.DecodeFrom(sample);

        for (size_t i = 0; i < iters; ++i) {
            auto t0 = SteadyClock::now();
            butil::IOBuf out;
            src.EncodeTo(&out);
            auto t1 = SteadyClock::now();
            if (reuse) {
                (void)reused.DecodeFrom(out);
            } else {
                MsgT dst;
                (void)dst.DecodeFrom(out);
            }
            auto t2 = SteadyClock::now();
            double e =
                std::chrono::duration<double, std::nano>(t1 - t0).count();
            double d =
                std::chrono::duration<double, std::nano>(t2 - t1).count();
            enc_ns.push_back(e);
            dec_ns.push_back(d);
            rt_ns.push_back(e + d);
        }

        CodecRow row;
        row.scenario = scenario;
        row.variant = variant;
        row.wire_bytes = wire_bytes;
        row.enc = Summarize(std::move(enc_ns));
        row.dec = Summarize(std::move(dec_ns));
        row.rt = Summarize(std::move(rt_ns));
        row.mb_s = CalcMbPerSec(wire_bytes, row.rt.avg_ns);
        out_rows->push_back(std::move(row));
    };

    measure("fresh", false);
    measure("reuse", true);
}

class BenchMasterServiceImpl final : public fast_msg::BenchMasterService {
   public:
    explicit BenchMasterServiceImpl(size_t replicas_per_key)
        : template_result_(MakeKeyResult(0, replicas_per_key)) {}

    brpc_fast::RpcStatus Ping(const brpc_fast::RpcContext& /*ctx*/,
                              const fast_msg::PingRequest& req,
                              fast_msg::PingResponse& resp) override {
        resp.view_version = req.client_hi ^ req.client_lo;
        resp.client_status = 1;
        return brpc_fast::RpcStatus::Ok();
    }

    brpc_fast::RpcStatus GetReplica(
        const brpc_fast::RpcContext& /*ctx*/,
        const fast_msg::GetReplicaRequest& req,
        fast_msg::GetReplicaResponse& resp) override {
        resp.result = template_result_;
        resp.result->object_checksum = static_cast<uint64_t>(req.key.size());
        return brpc_fast::RpcStatus::Ok();
    }

    brpc_fast::RpcStatus BatchGetReplica(
        const brpc_fast::RpcContext& /*ctx*/,
        const fast_msg::BatchGetReplicaRequest& req,
        fast_msg::BatchGetReplicaResponse& resp) override {
        const size_t n = req.keys.size();
        resp.results.resize(n);
        for (size_t i = 0; i < n; ++i) {
            resp.results[i] = template_result_;
            resp.results[i].object_checksum =
                static_cast<uint64_t>(req.keys[i].size()) + i;
        }
        return brpc_fast::RpcStatus::Ok();
    }

    brpc_fast::RpcStatus BatchPutStart(
        const brpc_fast::RpcContext& /*ctx*/,
        const fast_msg::BatchPutStartRequest& req,
        fast_msg::BatchPutStartResponse& resp) override {
        const size_t n = req.keys.size();
        resp.results.resize(n);
        for (size_t i = 0; i < n; ++i) {
            resp.results[i] = template_result_;
            resp.results[i].object_checksum = req.slice_lengths[i];
        }
        return brpc_fast::RpcStatus::Ok();
    }

   private:
    fast_msg::KeyReplicaResult template_result_;
};

void RunRpcScenario(const std::string& name, bool is_mixed, double small_ratio,
                    size_t batch_size, std::vector<RpcRow>* out_rows) {
    brpc_fast::FastServer server;
    if (!server
             .AddService(std::make_unique<BenchMasterServiceImpl>(
                 FLAGS_replicas_per_key))
             .ok()) {
        return;
    }

    brpc::ServerOptions srv_opts;
    srv_opts.has_builtin_services = false;
    if (!server.Start("127.0.0.1:0", srv_opts).ok()) return;

    brpc::Channel channel;
    brpc::ChannelOptions ch_opts;
    ch_opts.protocol = brpc::PROTOCOL_BAIDU_STD;
    ch_opts.connection_type = "pooled";
    ch_opts.timeout_ms = 30000;
    if (channel.Init(server.listen_address_str().c_str(), &ch_opts) != 0) {
        return;
    }

    fast_msg::BatchGetReplicaRequest large_req;
    large_req.tenant_id = "tenant_prod_01";
    large_req.keys.reserve(batch_size);
    for (size_t i = 0; i < batch_size; ++i) {
        large_req.keys.push_back(MakeKey(i, FLAGS_key_len));
    }
    fast_msg::GetReplicaRequest small_req;
    small_req.tenant_id = "tenant_prod_01";
    small_req.key = MakeKey(0, FLAGS_key_len);
    fast_msg::PingRequest ping_req;
    ping_req.client_hi = 1;
    ping_req.client_lo = 2;

    auto wire_size = [](const auto& m) -> size_t {
        butil::IOBuf b;
        m.EncodeTo(&b);
        return b.size();
    };
    fast_msg::PingResponse sample_ping{3, 1};
    fast_msg::GetReplicaResponse sample_small;
    sample_small.result = MakeKeyResult(0, FLAGS_replicas_per_key);
    auto sample_large = MakeBatchGetResp(batch_size, FLAGS_replicas_per_key);

    const size_t ping_bytes = wire_size(ping_req) + wire_size(sample_ping);
    const size_t small_bytes_per_op =
        wire_size(small_req) + wire_size(sample_small);
    const size_t large_bytes_per_op =
        wire_size(large_req) + wire_size(sample_large);

    struct WorkerState {
        brpc::Channel* channel;
        bool is_mixed;
        double small_ratio;
        const fast_msg::BatchGetReplicaRequest* large_req;
        const fast_msg::GetReplicaRequest* small_req;
        const fast_msg::PingRequest* ping_req;
        size_t ping_bytes;
        size_t small_bytes_per_op;
        size_t large_bytes_per_op;
        uint64_t seed;
        std::atomic<bool>* warmup_done;
        std::atomic<bool>* stop;
        std::vector<double> small_lat;
        std::vector<double> large_lat;
        size_t small_bytes = 0;
        size_t large_bytes = 0;
        size_t errors = 0;
    };

    auto worker_fn = [](void* raw) -> void* {
        auto* st = static_cast<WorkerState*>(raw);
        fast_msg::BenchMasterService_Stub stub(st->channel);
        std::mt19937_64 rng(st->seed);
        std::uniform_real_distribution<double> dist(0.0, 1.0);

        fast_msg::PingResponse ping_resp;
        fast_msg::GetReplicaResponse small_resp;
        fast_msg::BatchGetReplicaResponse large_resp;

        while (!st->stop->load(std::memory_order_relaxed)) {
            const bool measure =
                st->warmup_done->load(std::memory_order_relaxed);
            bool do_small = false;
            bool do_ping = false;
            if (st->is_mixed) {
                double r = dist(rng);
                if (r < st->small_ratio) {
                    do_small = true;
                    do_ping = (r < st->small_ratio * 0.5);
                }
            }

            auto t0 = SteadyClock::now();
            size_t bytes = 0;
            brpc_fast::RpcStatus rpc_st = brpc_fast::RpcStatus::Ok();
            if (do_small && do_ping) {
                rpc_st = stub.Ping(*st->ping_req, ping_resp);
                bytes = st->ping_bytes;
            } else if (do_small) {
                rpc_st = stub.GetReplica(*st->small_req, small_resp);
                bytes = st->small_bytes_per_op;
            } else {
                rpc_st = stub.BatchGetReplica(*st->large_req, large_resp);
                bytes = st->large_bytes_per_op;
            }
            auto t1 = SteadyClock::now();

            if (!measure) continue;
            if (!rpc_st.ok()) {
                ++st->errors;
                continue;
            }
            double ns =
                std::chrono::duration<double, std::nano>(t1 - t0).count();
            if (do_small) {
                st->small_lat.push_back(ns);
                st->small_bytes += bytes;
            } else {
                st->large_lat.push_back(ns);
                st->large_bytes += bytes;
            }
        }
        return nullptr;
    };

    std::atomic<bool> warmup_done{false};
    std::atomic<bool> stop{false};
    std::vector<WorkerState> states(FLAGS_concurrency);
    std::vector<bthread_t> tids(FLAGS_concurrency);

    for (size_t i = 0; i < FLAGS_concurrency; ++i) {
        states[i] = WorkerState{&channel,
                                is_mixed,
                                small_ratio,
                                &large_req,
                                &small_req,
                                &ping_req,
                                ping_bytes,
                                small_bytes_per_op,
                                large_bytes_per_op,
                                42ULL + i,
                                &warmup_done,
                                &stop,
                                {},
                                {},
                                0,
                                0,
                                0};
        bthread_start_background(&tids[i], nullptr, worker_fn, &states[i]);
    }

    bthread_usleep(FLAGS_warmup_ms * 1000ULL);
    auto m_start = SteadyClock::now();
    warmup_done.store(true, std::memory_order_release);
    bthread_usleep(FLAGS_duration_ms * 1000ULL);
    stop.store(true, std::memory_order_release);
    auto m_end = SteadyClock::now();

    for (size_t i = 0; i < FLAGS_concurrency; ++i)
        bthread_join(tids[i], nullptr);
    server.Stop(0);

    const double elapsed =
        std::chrono::duration<double>(m_end - m_start).count();
    std::vector<double> small_lat, large_lat, all_lat;
    size_t small_bytes = 0, large_bytes = 0, errors = 0;
    for (const auto& s : states) {
        small_lat.insert(small_lat.end(), s.small_lat.begin(),
                         s.small_lat.end());
        large_lat.insert(large_lat.end(), s.large_lat.begin(),
                         s.large_lat.end());
        small_bytes += s.small_bytes;
        large_bytes += s.large_bytes;
        errors += s.errors;
    }

    auto emit = [&](const char* traffic, std::vector<double> lats, size_t bytes,
                    size_t errs) {
        RpcRow r;
        r.scenario = name;
        r.traffic = traffic;
        r.ops = lats.size();
        r.errors = errs;
        r.qps = elapsed > 0.0 ? (static_cast<double>(r.ops) / elapsed) : 0.0;
        r.mb_s =
            elapsed > 0.0
                ? (static_cast<double>(bytes) / (1024.0 * 1024.0) / elapsed)
                : 0.0;
        r.lat = Summarize(std::move(lats));
        out_rows->push_back(std::move(r));
    };

    if (is_mixed) {
        all_lat.reserve(small_lat.size() + large_lat.size());
        all_lat.insert(all_lat.end(), small_lat.begin(), small_lat.end());
        all_lat.insert(all_lat.end(), large_lat.begin(), large_lat.end());
        emit("all", std::move(all_lat), small_bytes + large_bytes, errors);
        emit("small", std::move(small_lat), small_bytes, 0);
        emit("large_batch", std::move(large_lat), large_bytes, 0);
    } else {
        emit("large_batch", std::move(large_lat), large_bytes, errors);
    }
}

void PrintCodecTable(const std::vector<CodecRow>& rows) {
    std::cout
        << "\n==================== brpc_fast Codec ====================\n";
    std::cout << std::left << std::setw(24) << "Scenario" << std::setw(8)
              << "Mode" << std::right << std::setw(11) << "Wire"
              << std::setw(12) << "Enc(p50)" << std::setw(12) << "Dec(p50)"
              << std::setw(12) << "RT(avg)" << std::setw(12) << "RT(p99)"
              << std::setw(10) << "MB/s" << "\n";
    std::cout << std::string(101, '-') << "\n";
    for (const auto& r : rows) {
        std::cout << std::left << std::setw(24) << r.scenario << std::setw(8)
                  << r.variant << std::right << std::setw(11)
                  << FormatBytes(r.wire_bytes) << std::setw(12)
                  << FormatNs(r.enc.p50_ns) << std::setw(12)
                  << FormatNs(r.dec.p50_ns) << std::setw(12)
                  << FormatNs(r.rt.avg_ns) << std::setw(12)
                  << FormatNs(r.rt.p99_ns) << std::setw(10) << std::fixed
                  << std::setprecision(1) << r.mb_s << "\n";
    }
}

void PrintRpcTable(const std::vector<RpcRow>& rows) {
    std::cout
        << "\n==================== brpc_fast E2E RPC ====================\n";
    std::cout << std::left << std::setw(24) << "Scenario" << std::setw(12)
              << "Class" << std::right << std::setw(10) << "QPS"
              << std::setw(10) << "MB/s" << std::setw(12) << "Avg"
              << std::setw(12) << "P50" << std::setw(12) << "P99"
              << std::setw(12) << "P999" << std::setw(6) << "Err" << "\n";
    std::cout << std::string(110, '-') << "\n";
    for (const auto& r : rows) {
        std::cout << std::left << std::setw(24) << r.scenario << std::setw(12)
                  << r.traffic << std::right << std::setw(10) << std::fixed
                  << std::setprecision(1) << r.qps << std::setw(10)
                  << std::setprecision(1) << r.mb_s << std::setw(12)
                  << FormatNs(r.lat.avg_ns) << std::setw(12)
                  << FormatNs(r.lat.p50_ns) << std::setw(12)
                  << FormatNs(r.lat.p99_ns) << std::setw(12)
                  << FormatNs(r.lat.p999_ns) << std::setw(6) << r.errors
                  << "\n";
    }
}

bool WriteCsv(const std::string& path, const std::string& label,
              const std::vector<CodecRow>& codec_rows,
              const std::vector<RpcRow>& rpc_rows) {
    std::ofstream ofs(path);
    if (!ofs) return false;
    ofs << "label,kind,scenario,variant,wire_bytes,ops,errors,qps,mb_s,"
           "enc_p50_ns,dec_p50_ns,avg_ns,p50_ns,p99_ns,p999_ns\n";
    ofs << std::fixed << std::setprecision(2);
    for (const auto& r : codec_rows) {
        ofs << label << ",codec," << r.scenario << "," << r.variant << ","
            << r.wire_bytes << "," << r.rt.count << ",0,0," << r.mb_s << ","
            << r.enc.p50_ns << "," << r.dec.p50_ns << "," << r.rt.avg_ns << ","
            << r.rt.p50_ns << "," << r.rt.p99_ns << "," << r.rt.p999_ns << "\n";
    }
    for (const auto& r : rpc_rows) {
        ofs << label << ",rpc," << r.scenario << "," << r.traffic << ",0,"
            << r.ops << "," << r.errors << "," << r.qps << "," << r.mb_s
            << ",0,0," << r.lat.avg_ns << "," << r.lat.p50_ns << ","
            << r.lat.p99_ns << "," << r.lat.p999_ns << "\n";
    }
    return true;
}

}  // namespace

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (brpc::FLAGS_max_body_size < 128ULL * 1024 * 1024) {
        brpc::FLAGS_max_body_size = 128ULL * 1024 * 1024;
    }

    const size_t bs = FLAGS_batch_size;
    std::cout << "brpc_fast Core Performance Benchmark"
              << " (mode=" << FLAGS_mode << ", batch_size=" << bs
              << ", concurrency=" << FLAGS_concurrency
              << ", duration_ms=" << FLAGS_duration_ms << ")\n";

    std::vector<CodecRow> codec_rows;
    std::vector<RpcRow> rpc_rows;

    if (FLAGS_mode == "all" || FLAGS_mode == "codec") {
        fast_msg::PingResponse ping;
        ping.view_version = 123456789ULL;
        ping.client_status = 1;
        RunCodecBench("Ping(small)", ping, 10000, &codec_rows);

        const size_t batch_iters =
            std::clamp<size_t>(200000 / std::max<size_t>(bs, 1), 15, 200);
        RunCodecBench("BatchGet(" + std::to_string(bs) + ")",
                      MakeBatchGetResp(bs, FLAGS_replicas_per_key), batch_iters,
                      &codec_rows);
        RunCodecBench(
            "BatchPutStart(" + std::to_string(bs) + ")",
            MakeBatchPutReq(bs, FLAGS_key_len, FLAGS_replicas_per_key),
            batch_iters, &codec_rows);
        RunCodecBench("BatchCompact(" + std::to_string(bs) + ")",
                      MakeBatchCompactResp(bs, FLAGS_replicas_per_key),
                      batch_iters, &codec_rows);
        RunCodecBench("BatchExistKey(" + std::to_string(bs) + ")",
                      MakeBatchExistResp(bs), batch_iters * 2, &codec_rows);
        PrintCodecTable(codec_rows);
    }

    if (FLAGS_mode == "all" || FLAGS_mode == "rpc") {
        RunRpcScenario("BatchGet(" + std::to_string(bs) + ")", false, 0.0, bs,
                       &rpc_rows);
        RunRpcScenario("Mixed(80%sm+" + std::to_string(bs) + ")", true, 0.8, bs,
                       &rpc_rows);
        PrintRpcTable(rpc_rows);
    }

    if (!FLAGS_csv_path.empty()) {
        if (!WriteCsv(FLAGS_csv_path, FLAGS_label, codec_rows, rpc_rows)) {
            std::cerr << "Failed to write CSV to " << FLAGS_csv_path << "\n";
            return 1;
        }
        std::cout << "\nCSV written to: " << FLAGS_csv_path << "\n";
    }

    return 0;
}
