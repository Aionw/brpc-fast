#include "brpc_fast/service.h"

#include <brpc/errno.pb.h>
#include <brpc/rpc_dump.h>
#include <butil/logging.h>
#include <google/protobuf/descriptor.pb.h>

#include <mutex>
#include <unordered_map>
#include <vector>

namespace brpc_fast {
namespace {

struct TransparentStringHash {
    using is_transparent = void;
    size_t operator()(std::string_view sv) const noexcept {
        return std::hash<std::string_view>{}(sv);
    }
    size_t operator()(const std::string& s) const noexcept {
        return std::hash<std::string_view>{}(s);
    }
};

struct TransparentStringEq {
    using is_transparent = void;
    bool operator()(std::string_view a, std::string_view b) const noexcept {
        return a == b;
    }
};

using MethodMap =
    std::unordered_map<std::string, detail::RawMethodHandler,
                       TransparentStringHash, TransparentStringEq>;

class ServiceRegistryAdapter final : public detail::FastMethodRegistry {
   public:
    ServiceRegistryAdapter(MethodMap& methods, std::vector<std::string>& order)
        : methods_(methods), order_(order) {}

    void RegisterMethod(std::string_view method_name,
                        detail::RawMethodHandler handler) override {
        std::string name(method_name);
        if (methods_.find(name) == methods_.end()) {
            order_.push_back(name);
        }
        methods_[std::move(name)] = std::move(handler);
    }

   private:
    MethodMap& methods_;
    std::vector<std::string>& order_;
};

struct SyntheticPoolState {
    std::mutex mu;
    ::google::protobuf::DescriptorPool pool;
    uint64_t next_id = 0;
};

SyntheticPoolState& GetSyntheticPool() {
    // Intentionally leaked to avoid static destruction order issues if a
    // channel or server references synthetic descriptors during shutdown.
    static auto* state = new SyntheticPoolState();
    return *state;
}

// Thin proxy owned by brpc::Server that forwards to FastServer::master_,
// guaranteeing master_ (and its owned services) are always destroyed AFTER
// brpc::Server has stopped and joined all workers.
class ForwardingMasterService final : public ::brpc::BaiduMasterService {
   public:
    explicit ForwardingMasterService(FastMasterService* target) noexcept
        : target_(target) {}

    void ProcessRpcRequest(::brpc::Controller* cntl,
                           const ::brpc::SerializedRequest* request,
                           ::brpc::SerializedResponse* response,
                           ::google::protobuf::Closure* done) override {
        target_->ProcessRpcRequest(cntl, request, response, done);
    }

    void Describe(std::ostream& os,
                  const ::brpc::DescribeOptions& options) const override {
        target_->Describe(os, options);
    }

   private:
    FastMasterService* target_;
};

}  // namespace

namespace detail {

std::vector<const ::google::protobuf::MethodDescriptor*>
RegisterSyntheticServiceDescriptors(
    const std::string& proto_file_name, const std::string& package_name,
    const std::string& service_name,
    const std::vector<std::string>& method_names) {
    auto& state = GetSyntheticPool();
    std::lock_guard<std::mutex> lock(state.mu);

    const std::string full_service_name =
        package_name.empty() ? service_name
                             : (package_name + "." + service_name);
    const ::google::protobuf::ServiceDescriptor* existing =
        state.pool.FindServiceByName(full_service_name);
    if (existing != nullptr) {
        std::vector<const ::google::protobuf::MethodDescriptor*> result;
        result.reserve(method_names.size());
        for (const auto& name : method_names) {
            const auto* md = existing->FindMethodByName(name);
            CHECK(md != nullptr)
                << "brpc_fast: cached synthetic ServiceDescriptor "
                << full_service_name << " is missing method " << name;
            result.push_back(md);
        }
        return result;
    }

    ::google::protobuf::FileDescriptorProto file_proto;
    file_proto.set_name("brpc_fast_synthetic_" +
                        std::to_string(state.next_id++) + "_" +
                        proto_file_name);
    file_proto.set_syntax("proto3");
    if (!package_name.empty()) {
        file_proto.set_package(package_name);
    }

    const std::string placeholder_name = "_BrpcFastEmpty_" + service_name;
    auto* empty_msg = file_proto.add_message_type();
    empty_msg->set_name(placeholder_name);

    const std::string full_placeholder =
        package_name.empty() ? ("." + placeholder_name)
                             : ("." + package_name + "." + placeholder_name);

    auto* svc_proto = file_proto.add_service();
    svc_proto->set_name(service_name);
    for (const auto& method_name : method_names) {
        auto* m = svc_proto->add_method();
        m->set_name(method_name);
        m->set_input_type(full_placeholder);
        m->set_output_type(full_placeholder);
    }

    const ::google::protobuf::FileDescriptor* file_desc =
        state.pool.BuildFile(file_proto);
    CHECK(file_desc != nullptr)
        << "brpc_fast: failed to build synthetic FileDescriptor for service "
        << full_service_name;

    const ::google::protobuf::ServiceDescriptor* svc_desc =
        file_desc->FindServiceByName(service_name);
    CHECK(svc_desc != nullptr)
        << "brpc_fast: failed to find synthetic ServiceDescriptor "
        << full_service_name;

    std::vector<const ::google::protobuf::MethodDescriptor*> result;
    result.reserve(method_names.size());
    for (const auto& method_name : method_names) {
        const auto* md = svc_desc->FindMethodByName(method_name);
        CHECK(md != nullptr)
            << "brpc_fast: failed to find synthetic MethodDescriptor "
            << full_service_name << "." << method_name;
        result.push_back(md);
    }
    return result;
}

}  // namespace detail

struct FastMasterService::Impl {
    struct ServiceEntry {
        std::string full_name;
        std::string short_name;
        MethodMap methods;
        std::vector<std::string> method_order;
    };

    std::vector<std::unique_ptr<FastServiceBase>> owned_services;
    std::vector<std::unique_ptr<ServiceEntry>> owned_entries;
    std::unordered_map<std::string, ServiceEntry*, TransparentStringHash,
                       TransparentStringEq>
        services;  // exact full_name -> entry
    std::unordered_map<std::string, ServiceEntry*, TransparentStringHash,
                       TransparentStringEq>
        short_name_aliases;  // short_name -> entry (nullptr if ambiguous)
};

FastMasterService::FastMasterService() : impl_(std::make_unique<Impl>()) {}
FastMasterService::~FastMasterService() = default;

int FastMasterService::AddService(std::unique_ptr<FastServiceBase> service) {
    if (!service) {
        return -1;
    }
    if (AddService(service.get()) != 0) {
        return -1;
    }
    impl_->owned_services.push_back(std::move(service));
    return 0;
}

int FastMasterService::AddService(FastServiceBase* service) {
    if (service == nullptr) {
        return -1;
    }
    std::string full_name(service->ServiceFullName());
    std::string short_name(service->ServiceShortName());
    if (full_name.empty()) {
        return -1;
    }
    if (impl_->services.find(full_name) != impl_->services.end()) {
        LOG(ERROR) << "brpc_fast: duplicate service registration: "
                   << full_name;
        return -1;
    }

    auto entry = std::make_unique<Impl::ServiceEntry>();
    entry->full_name = full_name;
    entry->short_name = short_name;

    ServiceRegistryAdapter adapter(entry->methods, entry->method_order);
    service->RegisterMethods(adapter);

    Impl::ServiceEntry* raw = entry.get();
    impl_->owned_entries.push_back(std::move(entry));
    impl_->services[full_name] = raw;
    if (!short_name.empty() && short_name != full_name) {
        auto [it, inserted] =
            impl_->short_name_aliases.emplace(short_name, raw);
        if (!inserted) {
            it->second = nullptr;  // Mark ambiguous short name
        }
    }
    return 0;
}

void FastMasterService::ProcessRpcRequest(
    ::brpc::Controller* cntl, const ::brpc::SerializedRequest* request,
    ::brpc::SerializedResponse* response, ::google::protobuf::Closure* done) {
    const auto* sampled = cntl->sampled_request();
    if (sampled == nullptr) {
        ::brpc::ClosureGuard done_guard(done);
        cntl->SetFailed(::brpc::EINTERNAL,
                        "brpc_fast: missing sampled request metadata");
        return;
    }
    const std::string& service_name = sampled->meta.service_name();
    const std::string& method_name = sampled->meta.method_name();

    Impl::ServiceEntry* entry = nullptr;
    auto svc_it = impl_->services.find(service_name);
    if (svc_it != impl_->services.end()) {
        entry = svc_it->second;
    } else {
        auto alias_it = impl_->short_name_aliases.find(service_name);
        if (alias_it != impl_->short_name_aliases.end()) {
            if (alias_it->second == nullptr) {
                ::brpc::ClosureGuard done_guard(done);
                cntl->SetFailed(
                    ::brpc::ENOSERVICE,
                    "Ambiguous fast short service name=%s; use full name",
                    service_name.c_str());
                return;
            }
            entry = alias_it->second;
        }
    }
    if (entry == nullptr) {
        ::brpc::ClosureGuard done_guard(done);
        cntl->SetFailed(::brpc::ENOSERVICE, "Fail to find fast service=%s",
                        service_name.c_str());
        return;
    }
    const auto& method_map = entry->methods;
    auto method_it = method_map.find(method_name);
    if (method_it == method_map.end()) {
        ::brpc::ClosureGuard done_guard(done);
        cntl->SetFailed(::brpc::ENOMETHOD, "Fail to find fast method=%s/%s",
                        service_name.c_str(), method_name.c_str());
        return;
    }
    method_it->second(cntl, request, response, done);
}

void FastMasterService::Describe(std::ostream& os,
                                 const ::brpc::DescribeOptions& options) const {
    os << "brpc_fast services (" << impl_->owned_entries.size() << "):";
    const char* nl = options.use_html ? "<br>" : "\n";
    os << nl;
    for (const auto& entry : impl_->owned_entries) {
        os << "  " << entry->full_name << " {";
        for (size_t i = 0; i < entry->method_order.size(); ++i) {
            if (i > 0) {
                os << ", ";
            }
            os << entry->method_order[i];
        }
        os << "}" << nl;
    }
}

FastServer::~FastServer() { Stop(0); }

RpcStatus FastServer::AddService(std::unique_ptr<FastServiceBase> service) {
    if (started_) {
        return RpcStatus::Error(
            EINVAL,
            "brpc_fast: cannot AddService after FastServer has started");
    }
    if (!service) {
        return RpcStatus::Error(EINVAL, "brpc_fast: null service pointer");
    }
    if (master_.AddService(std::move(service)) != 0) {
        return RpcStatus::Error(EINVAL,
                                "brpc_fast: failed to register service");
    }
    return RpcStatus::Ok();
}

RpcStatus FastServer::Start(const std::string& listen_addr,
                            ::brpc::ServerOptions options) {
    if (started_) {
        return RpcStatus::Error(EINVAL,
                                "brpc_fast: FastServer already started");
    }
    if (options.baidu_master_service != nullptr) {
        return RpcStatus::Error(
            EINVAL,
            "brpc_fast: options.baidu_master_service must not be set on "
            "FastServer");
    }
    if (options.interceptor != nullptr) {
        return RpcStatus::Error(
            EINVAL,
            "brpc_fast: options.interceptor is not supported on FastServer "
            "(bypassed by baidu_master_service)");
    }
    butil::EndPoint ep;
    if (butil::str2endpoint(listen_addr.c_str(), &ep) != 0) {
        return RpcStatus::Error(
            EINVAL, "brpc_fast: invalid listen address: " + listen_addr);
    }
    auto* forwarder = new ForwardingMasterService(&master_);
    options.baidu_master_service = forwarder;
    if (server_.Start(ep, &options) != 0) {
        if (server_.options().baidu_master_service != forwarder) {
            delete forwarder;
        }
        return RpcStatus::Error(
            ::brpc::EINTERNAL,
            "brpc_fast: failed to start server on " + listen_addr);
    }
    started_ = true;
    return RpcStatus::Ok();
}

void FastServer::Stop(int closewait_ms) {
    if (!started_) {
        return;
    }
    server_.Stop(closewait_ms);
    server_.Join();
    started_ = false;
}

std::string FastServer::listen_address_str() const {
    return butil::endpoint2str(server_.listen_address()).c_str();
}

}  // namespace brpc_fast
