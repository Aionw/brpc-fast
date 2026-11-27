#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace {

using ::google::protobuf::Descriptor;
using ::google::protobuf::DescriptorPool;
using ::google::protobuf::EnumDescriptor;
using ::google::protobuf::FieldDescriptor;
using ::google::protobuf::FileDescriptor;
using ::google::protobuf::FileDescriptorSet;
using ::google::protobuf::MethodDescriptor;
using ::google::protobuf::OneofDescriptor;
using ::google::protobuf::ServiceDescriptor;

constexpr const char* kDetailNs = "::brpc_fast::detail::";
constexpr const char* kGeneratedRootNs = "brpc_fast_pb";

struct CodegenOptions {
    bool preserve_unknown_fields = false;
};

// ---- Naming & descriptor utilities ----

const std::unordered_set<std::string>& CppKeywords() {
    static const std::unordered_set<std::string> kKeywords = {
        "alignas",       "alignof",     "and",
        "and_eq",        "asm",         "auto",
        "bitand",        "bitor",       "bool",
        "break",         "case",        "catch",
        "char",          "char8_t",     "char16_t",
        "char32_t",      "class",       "compl",
        "concept",       "const",       "consteval",
        "constexpr",     "constinit",   "const_cast",
        "continue",      "co_await",    "co_return",
        "co_yield",      "decltype",    "default",
        "delete",        "do",          "double",
        "dynamic_cast",  "else",        "enum",
        "explicit",      "export",      "extern",
        "false",         "float",       "for",
        "friend",        "goto",        "if",
        "inline",        "int",         "long",
        "mutable",       "namespace",   "new",
        "noexcept",      "not",         "not_eq",
        "nullptr",       "operator",    "or",
        "or_eq",         "private",     "protected",
        "public",        "register",    "reinterpret_cast",
        "requires",      "return",      "short",
        "signed",        "sizeof",      "static",
        "static_assert", "static_cast", "struct",
        "switch",        "template",    "this",
        "thread_local",  "throw",       "true",
        "try",           "typedef",     "typeid",
        "typename",      "union",       "unsigned",
        "using",         "virtual",     "void",
        "volatile",      "wchar_t",     "while",
        "xor",           "xor_eq",
    };
    return kKeywords;
}

std::string SafeIdent(const std::string& name) {
    return CppKeywords().count(name) > 0 ? (name + "_") : name;
}

std::string ToPascalCase(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    bool upper_next = true;
    for (char c : s) {
        if (c == '_') {
            upper_next = true;
        } else {
            out.push_back(upper_next ? static_cast<char>(std::toupper(
                                           static_cast<unsigned char>(c)))
                                     : c);
            upper_next = false;
        }
    }
    return out;
}

std::string ReplaceDotsWithColons(const std::string& full_name) {
    std::string out;
    size_t start = 0;
    while (start <= full_name.size()) {
        const size_t dot = full_name.find('.', start);
        const std::string part = (dot == std::string::npos)
                                     ? full_name.substr(start)
                                     : full_name.substr(start, dot - start);
        out += "::";
        out += SafeIdent(part);
        if (dot == std::string::npos) break;
        start = dot + 1;
    }
    return out;
}

std::string GeneratedNamespace(const std::string& pkg) {
    if (pkg.empty()) {
        return kGeneratedRootNs;
    }
    return std::string(kGeneratedRootNs) + ReplaceDotsWithColons(pkg);
}

std::string GeneratedCppType(const std::string& full_name) {
    return std::string("::") + kGeneratedRootNs +
           ReplaceDotsWithColons(full_name);
}

std::string ProtoStem(const std::string& proto_name) {
    if (proto_name.size() > 6 &&
        proto_name.compare(proto_name.size() - 6, 6, ".proto") == 0) {
        return proto_name.substr(0, proto_name.size() - 6);
    }
    return proto_name;
}

bool IsFieldOptionalWrapped(const FieldDescriptor* f) {
    if (f->is_repeated() || f->is_map() ||
        f->real_containing_oneof() != nullptr) {
        return false;
    }
    return f->type() == FieldDescriptor::TYPE_MESSAGE || f->has_presence();
}

bool IsProto3ImplicitField(const FieldDescriptor* f) {
    return !f->is_map() && !f->is_repeated() && !IsFieldOptionalWrapped(f) &&
           f->real_containing_oneof() == nullptr;
}

// 3-bit WireType: field numbers 1..15 are 1-byte tags; 16..2047 are 2-byte
// tags. Both are handled on the speculative fast path via MatchConstTag.
bool HasFastTag(const FieldDescriptor* f) {
    return f->number() > 0 && f->number() < 2048;
}

bool MessageUsesSizeTape(const Descriptor* msg) {
    for (int i = 0; i < msg->field_count(); ++i) {
        const FieldDescriptor* f = msg->field(i);
        if (f->is_map() || f->type() == FieldDescriptor::TYPE_MESSAGE ||
            f->type() == FieldDescriptor::TYPE_GROUP) {
            return true;
        }
        if (f->is_repeated() && (f->type() == FieldDescriptor::TYPE_STRING ||
                                 f->type() == FieldDescriptor::TYPE_BYTES)) {
            return true;
        }
    }
    return false;
}

bool IsColumnarEligible(const Descriptor* msg, const CodegenOptions& opts) {
    if (opts.preserve_unknown_fields) return false;
    if (msg->real_oneof_decl_count() > 0) return false;
    if (msg->field_count() < 2) return false;
    for (int i = 0; i < msg->field_count(); ++i) {
        const FieldDescriptor* f = msg->field(i);
        if (f->is_repeated() || f->is_map() || IsFieldOptionalWrapped(f) ||
            f->real_containing_oneof() != nullptr) {
            return false;
        }
        switch (f->type()) {
            case FieldDescriptor::TYPE_BOOL:
            case FieldDescriptor::TYPE_INT32:
            case FieldDescriptor::TYPE_UINT32:
            case FieldDescriptor::TYPE_SINT32:
            case FieldDescriptor::TYPE_FIXED32:
            case FieldDescriptor::TYPE_SFIXED32:
            case FieldDescriptor::TYPE_INT64:
            case FieldDescriptor::TYPE_UINT64:
            case FieldDescriptor::TYPE_SINT64:
            case FieldDescriptor::TYPE_FIXED64:
            case FieldDescriptor::TYPE_SFIXED64:
            case FieldDescriptor::TYPE_FLOAT:
            case FieldDescriptor::TYPE_DOUBLE:
            case FieldDescriptor::TYPE_ENUM:
                break;
            default:
                return false;
        }
    }
    return true;
}

std::string FieldResetCall(const FieldDescriptor* f) {
    return std::string(kDetailNs) + "ResetField(this->" +
           SafeIdent(std::string(f->name())) + ")";
}

bool IsAncestorOrSelf(const Descriptor* maybe_ancestor, const Descriptor* d) {
    while (d != nullptr) {
        if (d == maybe_ancestor) return true;
        d = d->containing_type();
    }
    return false;
}

std::vector<const Descriptor*> TopoSortMessages(
    const std::vector<const Descriptor*>& msgs) {
    std::unordered_set<const Descriptor*> scope(msgs.begin(), msgs.end());
    std::unordered_map<const Descriptor*, int> state;
    std::vector<const Descriptor*> ordered;
    ordered.reserve(msgs.size());

    auto find_enclosing = [&](const Descriptor* d) -> const Descriptor* {
        while (d != nullptr) {
            if (scope.count(d) > 0) return d;
            d = d->containing_type();
        }
        return nullptr;
    };

    std::function<void(const Descriptor*)> dfs = [&](const Descriptor* m) {
        if (state[m] != 0) return;
        state[m] = 1;
        std::function<void(const Descriptor*)> scan_deps =
            [&](const Descriptor* cur) {
                for (int i = 0; i < cur->field_count(); ++i) {
                    const FieldDescriptor* f = cur->field(i);
                    const Descriptor* target = nullptr;
                    if (f->is_map()) {
                        const FieldDescriptor* val =
                            f->message_type()->map_value();
                        if (val->type() == FieldDescriptor::TYPE_MESSAGE) {
                            target = val->message_type();
                        } else if (val->type() == FieldDescriptor::TYPE_ENUM) {
                            target = val->enum_type()->containing_type();
                        }
                    } else if (f->type() == FieldDescriptor::TYPE_MESSAGE) {
                        target = f->message_type();
                    } else if (f->type() == FieldDescriptor::TYPE_ENUM) {
                        target = f->enum_type()->containing_type();
                    }
                    if (target != nullptr) {
                        const Descriptor* dep = find_enclosing(target);
                        if (dep != nullptr && dep != m) dfs(dep);
                    }
                }
                for (int i = 0; i < cur->nested_type_count(); ++i) {
                    const Descriptor* nested = cur->nested_type(i);
                    if (!nested->options().map_entry()) scan_deps(nested);
                }
            };
        scan_deps(m);
        state[m] = 2;
        ordered.push_back(m);
    };

    for (const Descriptor* m : msgs) dfs(m);
    return ordered;
}

// Collects all FileDescriptors (other than `root`) referenced by fields, RPC
// methods, or transitive `import public` chains (and transitively within
// google/protobuf/* WKTs).
void CollectPublicDependencies(
    const FileDescriptor* file,
    std::unordered_set<const FileDescriptor*>& visited_pub,
    std::unordered_set<const FileDescriptor*>& visited_ref,
    std::unordered_set<const FileDescriptor*>& out);

void CollectReferencedFiles(const FileDescriptor* file,
                            std::unordered_set<const FileDescriptor*>& visited,
                            std::unordered_set<const FileDescriptor*>& out) {
    if (!visited.insert(file).second) return;

    auto record_file = [&](const FileDescriptor* dep) {
        if (dep == nullptr || dep == file) return;
        out.insert(dep);
        if (std::string(dep->name()).rfind("google/protobuf/", 0) == 0) {
            CollectReferencedFiles(dep, visited, out);
        }
    };

    std::function<void(const Descriptor*)> scan_msg = [&](const Descriptor* m) {
        for (int i = 0; i < m->field_count(); ++i) {
            const FieldDescriptor* f = m->field(i);
            if (f->is_map()) {
                const FieldDescriptor* val = f->message_type()->map_value();
                if (val->type() == FieldDescriptor::TYPE_MESSAGE) {
                    record_file(val->message_type()->file());
                } else if (val->type() == FieldDescriptor::TYPE_ENUM) {
                    record_file(val->enum_type()->file());
                }
            } else if (f->type() == FieldDescriptor::TYPE_MESSAGE) {
                record_file(f->message_type()->file());
            } else if (f->type() == FieldDescriptor::TYPE_ENUM) {
                record_file(f->enum_type()->file());
            }
        }
        for (int i = 0; i < m->nested_type_count(); ++i) {
            const Descriptor* n = m->nested_type(i);
            if (!n->options().map_entry()) scan_msg(n);
        }
    };

    for (int i = 0; i < file->message_type_count(); ++i) {
        scan_msg(file->message_type(i));
    }
    for (int i = 0; i < file->service_count(); ++i) {
        const ServiceDescriptor* svc = file->service(i);
        for (int j = 0; j < svc->method_count(); ++j) {
            record_file(svc->method(j)->input_type()->file());
            record_file(svc->method(j)->output_type()->file());
        }
    }

    std::unordered_set<const FileDescriptor*> visited_pub;
    CollectPublicDependencies(file, visited_pub, visited, out);
}

void CollectPublicDependencies(
    const FileDescriptor* file,
    std::unordered_set<const FileDescriptor*>& visited_pub,
    std::unordered_set<const FileDescriptor*>& visited_ref,
    std::unordered_set<const FileDescriptor*>& out) {
    if (!visited_pub.insert(file).second) return;
    for (int i = 0; i < file->public_dependency_count(); ++i) {
        const FileDescriptor* pub = file->public_dependency(i);
        if (pub == nullptr || pub == file) continue;
        out.insert(pub);
        if (std::string(pub->name()).rfind("google/protobuf/", 0) == 0) {
            CollectReferencedFiles(pub, visited_ref, out);
        }
        CollectPublicDependencies(pub, visited_pub, visited_ref, out);
    }
}

bool WriteFileAtomic(const std::filesystem::path& path,
                     const std::string& content) {
    std::filesystem::create_directories(path.parent_path());
    // Skip rewriting identical files so build timestamps stay stable.
    {
        std::ifstream ifs(path, std::ios::binary);
        if (ifs) {
            std::string existing((std::istreambuf_iterator<char>(ifs)),
                                 std::istreambuf_iterator<char>());
            if (existing == content) {
                return true;
            }
        }
    }
    const auto stamp =
        std::chrono::steady_clock::now().time_since_epoch().count();
    std::mt19937_64 rng(std::random_device{}() ^ static_cast<uint64_t>(stamp));
    std::filesystem::path tmp = path;
    tmp += ".tmp." + std::to_string(rng());
    {
        std::ofstream ofs(tmp, std::ios::binary | std::ios::trunc);
        if (!ofs) return false;
        ofs << content;
        if (!ofs.good()) return false;
    }
    std::error_code ec;
    std::filesystem::rename(tmp, path, ec);
    if (ec) {
        std::filesystem::remove(tmp, ec);
        return false;
    }
    return true;
}

// ---- C++20 code generator ----

class CodeGenerator {
   public:
    CodeGenerator(const FileDescriptor* file, const CodegenOptions& opts,
                  const std::unordered_set<const FileDescriptor*>& ref_files)
        : file_(file), opts_(opts), ref_files_(ref_files) {
        // Compute top-level and nested emission order in post-order (children
        // are emitted before their enclosing message's fields) so cyclic and
        // forward references—both top-level and nested—are identified before
        // emitting any struct.
        std::vector<const Descriptor*> top_msgs;
        top_msgs.reserve(file_->message_type_count());
        for (int i = 0; i < file_->message_type_count(); ++i) {
            top_msgs.push_back(file_->message_type(i));
        }
        ordered_top_ = TopoSortMessages(top_msgs);

        size_t next_order = 0;
        std::function<void(const Descriptor*)> index_msg =
            [&](const Descriptor* m) {
                std::vector<const Descriptor*> nested;
                for (int i = 0; i < m->nested_type_count(); ++i) {
                    const Descriptor* n = m->nested_type(i);
                    if (!n->options().map_entry()) nested.push_back(n);
                }
                auto sorted = TopoSortMessages(nested);
                for (const Descriptor* n : sorted) {
                    index_msg(n);
                }
                ordered_nested_[m] = std::move(sorted);
                emit_order_[m] = next_order++;
            };
        for (const Descriptor* msg : ordered_top_) {
            index_msg(msg);
        }
    }

    std::string GenerateHeader() {
        std::ostringstream out;
        out << "// Generated by brpc_fast_codegen from " << file_->name()
            << ". DO NOT EDIT.\n"
            << "#pragma once\n\n"
            << "#include <brpc_fast/codec.h>\n";
        if (file_->service_count() > 0) {
            out << "#include <brpc_fast/service.h>\n";
        }
        out << "\n#include <cstdint>\n#include <optional>\n#include <string>\n"
               "#include <unordered_map>\n#include <utility>\n"
               "#include <variant>\n#include <vector>\n";

        std::unordered_set<const FileDescriptor*> public_deps;
        for (int i = 0; i < file_->public_dependency_count(); ++i) {
            public_deps.insert(file_->public_dependency(i));
        }
        std::unordered_set<std::string> emitted_includes;
        for (int i = 0; i < file_->dependency_count(); ++i) {
            const FileDescriptor* dep = file_->dependency(i);
            std::string dep_name(dep->name());
            if (dep_name.rfind("google/protobuf/", 0) == 0 &&
                public_deps.count(dep) == 0 && ref_files_.count(dep) == 0) {
                continue;
            }
            if (emitted_includes.insert(dep_name).second) {
                out << "#include \"" << ProtoStem(dep_name) << ".fast.h\"\n";
            }
        }
        std::vector<std::string> extra_includes;
        for (const FileDescriptor* dep : ref_files_) {
            if (dep == file_) continue;
            std::string dep_name(dep->name());
            if (emitted_includes.count(dep_name) == 0) {
                extra_includes.push_back(std::move(dep_name));
            }
        }
        std::sort(extra_includes.begin(), extra_includes.end());
        for (const std::string& dep_name : extra_includes) {
            out << "#include \"" << ProtoStem(dep_name) << ".fast.h\"\n";
        }
        out << "\n";

        EmitNamespaceOpen(out);

        for (int i = 0; i < file_->enum_type_count(); ++i) {
            EmitEnum(out, file_->enum_type(i), "");
            out << "\n";
        }

        // Hoist nested enums into an internal namespace before any struct
        // definitions so cyclic references between messages and nested enums
        // always see complete enum types, while structs expose `using` aliases.
        bool has_nested_enums = false;
        for (const Descriptor* msg : ordered_top_) {
            if (MessageHasNestedEnums(msg)) {
                has_nested_enums = true;
                break;
            }
        }
        if (has_nested_enums) {
            out << "namespace _brpc_fast_enums {\n\n";
            for (const Descriptor* msg : ordered_top_) {
                EmitHoistedNestedEnums(out, msg, "");
            }
            out << "}  // namespace _brpc_fast_enums\n\n";
        }

        // Forward-declare all top-level messages so mutually-recursive structs
        // can reference each other via IndirectOptional / IndirectBox / vector.
        for (const Descriptor* msg : ordered_top_) {
            out << "struct " << SafeIdent(std::string(msg->name())) << ";\n";
        }
        if (!ordered_top_.empty()) {
            out << "\n";
        }

        std::vector<const Descriptor*> deferred_method_msgs;
        for (const Descriptor* msg : ordered_top_) {
            EmitMessage(out, msg, "", deferred_method_msgs);
            out << "\n";
        }

        // Emit out-of-line inline method definitions for any message that had
        // forward references to a later-defined sibling/mutual-cycle message.
        for (const Descriptor* msg : deferred_method_msgs) {
            EmitDeferredMessageMethods(out, msg);
        }

        for (int i = 0; i < file_->service_count(); ++i) {
            EmitServiceHeader(out, file_->service(i));
            out << "\n";
        }
        EmitNamespaceClose(out);
        return out.str();
    }

    std::string GenerateSource() {
        std::ostringstream out;
        out << "// Generated by brpc_fast_codegen from " << file_->name()
            << ". DO NOT EDIT.\n"
            << "#include \"" << ProtoStem(std::string(file_->name()))
            << ".fast.h\"\n\n";
        if (file_->service_count() == 0) return out.str();

        EmitNamespaceOpen(out);
        for (int i = 0; i < file_->service_count(); ++i) {
            EmitServiceSource(out, file_->service(i));
            out << "\n";
        }
        EmitNamespaceClose(out);
        return out.str();
    }

   private:
    // True if `target` is incomplete at the moment `enclosing_msg` is being
    // laid out (self-recursion, enclosing-ancestor recursion, or a mutual cycle
    // back-edge to a top-level or nested message emitted later in this file).
    bool IsTargetIncomplete(const Descriptor* enclosing_msg,
                            const Descriptor* target) const {
        if (target->file() != file_) return false;
        if (IsAncestorOrSelf(target, enclosing_msg)) return true;
        auto it_e = emit_order_.find(enclosing_msg);
        auto it_t = emit_order_.find(target);
        if (it_e != emit_order_.end() && it_t != emit_order_.end() &&
            it_t->second > it_e->second) {
            return true;
        }
        return false;
    }

    // True if `msg` directly references a message emitted later in the same
    // file (making inline method bodies inside `struct msg` see an incomplete
    // type; self-recursion alone is already complete at the closing brace).
    bool NeedsDeferredMethods(const Descriptor* msg) const {
        auto it_e = emit_order_.find(msg);
        if (it_e == emit_order_.end()) return false;
        const size_t my_ord = it_e->second;

        for (int i = 0; i < msg->field_count(); ++i) {
            const FieldDescriptor* f = msg->field(i);
            const Descriptor* target = nullptr;
            if (f->is_map()) {
                const FieldDescriptor* val = f->message_type()->map_value();
                if (val->type() == FieldDescriptor::TYPE_MESSAGE) {
                    target = val->message_type();
                }
            } else if (f->type() == FieldDescriptor::TYPE_MESSAGE) {
                target = f->message_type();
            }
            if (target != nullptr && target->file() == file_ && target != msg) {
                if (IsAncestorOrSelf(target, msg)) {
                    return true;
                }
                auto it_t = emit_order_.find(target);
                if (it_t != emit_order_.end() && it_t->second > my_ord) {
                    return true;
                }
            }
        }
        return false;
    }

    std::string QualifiedInNamespace(const Descriptor* msg) const {
        std::vector<std::string> parts;
        for (const Descriptor* d = msg; d != nullptr;
             d = d->containing_type()) {
            parts.push_back(SafeIdent(std::string(d->name())));
        }
        std::reverse(parts.begin(), parts.end());
        std::string res;
        for (size_t i = 0; i < parts.size(); ++i) {
            if (i > 0) res += "::";
            res += parts[i];
        }
        return res;
    }

    void EmitNamespaceOpen(std::ostringstream& out) {
        const std::string ns =
            GeneratedNamespace(std::string(file_->package()));
        out << "namespace " << ns << " {\n\n";
    }

    void EmitNamespaceClose(std::ostringstream& out) {
        const std::string ns =
            GeneratedNamespace(std::string(file_->package()));
        out << "}  // namespace " << ns << "\n";
    }

    void EmitEnum(std::ostringstream& out, const EnumDescriptor* e,
                  const std::string& indent) {
        out << indent << "enum class " << SafeIdent(std::string(e->name()))
            << " : int32_t {\n";
        for (int i = 0; i < e->value_count(); ++i) {
            const auto* v = e->value(i);
            out << indent << "    " << SafeIdent(std::string(v->name()))
                << " = " << v->number() << ",\n";
        }
        out << indent << "};\n";
    }

    bool MessageHasNestedEnums(const Descriptor* msg) const {
        if (msg->enum_type_count() > 0) return true;
        for (int i = 0; i < msg->nested_type_count(); ++i) {
            const Descriptor* n = msg->nested_type(i);
            if (!n->options().map_entry() && MessageHasNestedEnums(n)) {
                return true;
            }
        }
        return false;
    }

    void EmitHoistedNestedEnums(std::ostringstream& out, const Descriptor* msg,
                                const std::string& indent) {
        if (!MessageHasNestedEnums(msg)) return;
        const std::string cls = SafeIdent(std::string(msg->name()));
        out << indent << "namespace " << cls << " {\n";
        for (int i = 0; i < msg->enum_type_count(); ++i) {
            EmitEnum(out, msg->enum_type(i), indent);
        }
        for (int i = 0; i < msg->nested_type_count(); ++i) {
            const Descriptor* n = msg->nested_type(i);
            if (!n->options().map_entry()) {
                EmitHoistedNestedEnums(out, n, indent);
            }
        }
        out << indent << "}  // namespace " << cls << "\n\n";
    }

    std::string HoistedEnumCppType(const EnumDescriptor* e) const {
        return "::" + GeneratedNamespace(std::string(e->file()->package())) +
               "::_brpc_fast_enums::" +
               QualifiedInNamespace(e->containing_type()) +
               "::" + SafeIdent(std::string(e->name()));
    }

    std::string EnumCppType(const Descriptor* enclosing_msg,
                            const EnumDescriptor* e) const {
        if (e->file() == file_ && e->containing_type() != nullptr &&
            IsTargetIncomplete(enclosing_msg, e->containing_type())) {
            return HoistedEnumCppType(e);
        }
        return GeneratedCppType(std::string(e->full_name()));
    }

    std::string FieldCppType(const Descriptor* enclosing_msg,
                             const FieldDescriptor* f) const {
        switch (f->type()) {
            case FieldDescriptor::TYPE_BOOL:
                return "bool";
            case FieldDescriptor::TYPE_INT32:
            case FieldDescriptor::TYPE_SINT32:
            case FieldDescriptor::TYPE_SFIXED32:
                return "int32_t";
            case FieldDescriptor::TYPE_UINT32:
            case FieldDescriptor::TYPE_FIXED32:
                return "uint32_t";
            case FieldDescriptor::TYPE_INT64:
            case FieldDescriptor::TYPE_SINT64:
            case FieldDescriptor::TYPE_SFIXED64:
                return "int64_t";
            case FieldDescriptor::TYPE_UINT64:
            case FieldDescriptor::TYPE_FIXED64:
                return "uint64_t";
            case FieldDescriptor::TYPE_FLOAT:
                return "float";
            case FieldDescriptor::TYPE_DOUBLE:
                return "double";
            case FieldDescriptor::TYPE_STRING:
            case FieldDescriptor::TYPE_BYTES:
                return "std::string";
            case FieldDescriptor::TYPE_ENUM:
                return EnumCppType(enclosing_msg, f->enum_type());
            case FieldDescriptor::TYPE_MESSAGE:
            case FieldDescriptor::TYPE_GROUP:
                return GeneratedCppType(
                    std::string(f->message_type()->full_name()));
        }
        return "/*unsupported*/";
    }

    std::string FieldDefaultExpr(const Descriptor* enclosing_msg,
                                 const FieldDescriptor* f) const {
        switch (f->type()) {
            case FieldDescriptor::TYPE_BOOL:
                return "false";
            case FieldDescriptor::TYPE_INT32:
            case FieldDescriptor::TYPE_SINT32:
            case FieldDescriptor::TYPE_SFIXED32:
                return "0";
            case FieldDescriptor::TYPE_UINT32:
            case FieldDescriptor::TYPE_FIXED32:
                return "0u";
            case FieldDescriptor::TYPE_INT64:
            case FieldDescriptor::TYPE_SINT64:
            case FieldDescriptor::TYPE_SFIXED64:
                return "0";
            case FieldDescriptor::TYPE_UINT64:
            case FieldDescriptor::TYPE_FIXED64:
                return "0u";
            case FieldDescriptor::TYPE_FLOAT:
                return "0.0f";
            case FieldDescriptor::TYPE_DOUBLE:
                return "0.0";
            case FieldDescriptor::TYPE_ENUM:
                return EnumCppType(enclosing_msg, f->enum_type()) + "::" +
                       SafeIdent(std::string(f->enum_type()->value(0)->name()));
            default:
                return "";
        }
    }

    void EmitMessage(std::ostringstream& out, const Descriptor* msg,
                     const std::string& indent,
                     std::vector<const Descriptor*>& deferred_method_msgs) {
        const std::string cls = SafeIdent(std::string(msg->name()));
        const std::string inner = indent + "    ";
        const bool defer = NeedsDeferredMethods(msg);
        if (defer) {
            deferred_method_msgs.push_back(msg);
        }

        out << indent << "struct " << cls << " {\n";

        for (int i = 0; i < msg->enum_type_count(); ++i) {
            const EnumDescriptor* e = msg->enum_type(i);
            out << inner << "using " << SafeIdent(std::string(e->name()))
                << " = " << HoistedEnumCppType(e) << ";\n";
        }
        if (msg->enum_type_count() > 0) {
            out << "\n";
        }
        const auto& nested_msgs = ordered_nested_.at(msg);
        for (const Descriptor* n : nested_msgs) {
            out << inner << "struct " << SafeIdent(std::string(n->name()))
                << ";\n";
        }
        if (!nested_msgs.empty()) {
            out << "\n";
        }
        for (const Descriptor* n : nested_msgs) {
            EmitMessage(out, n, inner, deferred_method_msgs);
            out << "\n";
        }

        EmitOneofs(out, msg, inner, defer);
        EmitFields(out, msg, inner);
        if (opts_.preserve_unknown_fields) {
            out << inner << kDetailNs
                << "UnknownFieldBuffer _unknown_fields;\n";
        }
        out << "\n";
        if (defer) {
            out << inner << "bool operator==(const " << cls << "&) const;\n\n";
            out << inner << "static constexpr bool kUsesSizeTape = "
                << (MessageUsesSizeTape(msg) ? "true" : "false") << ";\n"
                << inner << "static constexpr bool kIsColumnarEligible = false;\n\n"
                << inner << "void Clear();\n"
                << inner << "size_t ComputeSize(" << kDetailNs
                << "SizeTape& tape) const;\n"
                << inner
                << "uint8_t* EncodeUnchecked(uint8_t* dst, const uint32_t*& "
                   "tape) const noexcept;\n"
                << inner
                << "bool DecodeContiguous(const uint8_t* cur, const uint8_t* "
                   "end, int remaining_depth);\n"
                << inner << "template <bool Overwrite>\n"
                << inner << "BRPC_FAST_NOINLINE bool DecodeLoop(" << kDetailNs
                << "FastReader& r);\n"
                << inner << "bool Decode(" << kDetailNs << "FastReader& r);\n"
                << inner << "template <bool Overwrite>\n"
                << inner
                << "static BRPC_FAST_NOINLINE bool DecodeColumnarVec("
                << kDetailNs << "FastReader& sub, std::vector<" << cls
                << ">& vec);\n"
                << inner << "static inline " << kDetailNs
                << "CompatStatus DecodeColumnarSingular(" << kDetailNs
                << "FastReader& sub, uint32_t count, " << cls
                << "& target, bool already_present);\n"
                << inner << "bool EncodeTo(::butil::IOBuf* out) const;\n"
                << inner << "bool DecodeFrom(const ::butil::IOBuf& in);\n";
        } else {
            out << inner << "bool operator==(const " << cls
                << "&) const = default;\n\n";
            EmitClearBody(out, msg, inner, "");
            EmitEncodeBodies(out, msg, inner, "");
            EmitDecodeBodies(out, msg, inner, "");
            EmitColumnarBodies(out, msg, inner, "");
            out << inner
                << "bool EncodeTo(::butil::IOBuf* out) const { return "
                   "::brpc_fast::EncodeToIOBuf(*this, out); }\n"
                << inner
                << "bool DecodeFrom(const ::butil::IOBuf& in) { return "
                   "::brpc_fast::DecodeFromIOBuf(in, *this); }\n";
        }
        out << indent << "};\n";
    }

    void EmitDeferredMessageMethods(std::ostringstream& out,
                                    const Descriptor* msg) {
        const std::string qcls = QualifiedInNamespace(msg);
        for (int i = 0; i < msg->real_oneof_decl_count(); ++i) {
            const OneofDescriptor* oneof = msg->oneof_decl(i);
            const std::string oname = SafeIdent(std::string(oneof->name()));
            for (int j = 0; j < oneof->field_count(); ++j) {
                const FieldDescriptor* f = oneof->field(j);
                if (!IsOneofBranchIndirect(msg, f)) continue;
                const std::string ftype = FieldCppType(msg, f);
                const int idx = j + 1;
                out << "inline " << ftype << "& " << qcls << "::mutable_"
                    << f->name() << "() {\n"
                    << "    if (this->" << oname << ".index() != " << idx
                    << ") { this->" << oname << ".emplace<" << idx << ">(); }\n"
                    << "    return std::get<" << idx << ">(this->" << oname
                    << ").get();\n"
                    << "}\n\n";
            }
        }
        out << "inline bool " << qcls << "::operator==(const " << qcls
            << "&) const = default;\n\n";
        EmitClearBody(out, msg, "", qcls + "::");
        EmitEncodeBodies(out, msg, "", qcls + "::");
        EmitDecodeBodies(out, msg, "", qcls + "::");
        EmitColumnarBodies(out, msg, "", qcls + "::");
        out << "inline bool " << qcls
            << "::EncodeTo(::butil::IOBuf* out) const { return "
               "::brpc_fast::EncodeToIOBuf(*this, out); }\n"
            << "inline bool " << qcls
            << "::DecodeFrom(const ::butil::IOBuf& in) { return "
               "::brpc_fast::DecodeFromIOBuf(in, *this); }\n\n";
    }

    bool IsOneofBranchIndirect(const Descriptor* msg,
                               const FieldDescriptor* f) const {
        return f->type() == FieldDescriptor::TYPE_MESSAGE &&
               IsTargetIncomplete(msg, f->message_type());
    }

    std::string OneofVariantType(const Descriptor* msg,
                                 const FieldDescriptor* f) const {
        const std::string ftype = FieldCppType(msg, f);
        if (IsOneofBranchIndirect(msg, f)) {
            return "::brpc_fast::IndirectBox<" + ftype + ">";
        }
        return ftype;
    }

    void EmitOneofs(std::ostringstream& out, const Descriptor* msg,
                    const std::string& inner, bool defer) {
        for (int i = 0; i < msg->real_oneof_decl_count(); ++i) {
            const OneofDescriptor* oneof = msg->oneof_decl(i);
            const std::string oname = SafeIdent(std::string(oneof->name()));
            const std::string cenum =
                ToPascalCase(std::string(oneof->name())) + "Case";

            out << inner << "enum class " << cenum << " : uint32_t {\n"
                << inner << "    kNotSet = 0,\n";
            for (int j = 0; j < oneof->field_count(); ++j) {
                const FieldDescriptor* f = oneof->field(j);
                out << inner << "    k" << ToPascalCase(std::string(f->name()))
                    << " = " << f->number() << ",\n";
            }
            out << inner << "};\n" << inner << "std::variant<std::monostate";
            for (int j = 0; j < oneof->field_count(); ++j) {
                out << ", " << OneofVariantType(msg, oneof->field(j));
            }
            out << "> " << oname << ";\n";

            out << inner << cenum << " " << oname
                << "_case() const noexcept {\n"
                << inner << "    switch (this->" << oname << ".index()) {\n";
            for (int j = 0; j < oneof->field_count(); ++j) {
                const FieldDescriptor* f = oneof->field(j);
                out << inner << "        case " << (j + 1) << ": return "
                    << cenum << "::k" << ToPascalCase(std::string(f->name()))
                    << ";\n";
            }
            out << inner << "        default: return " << cenum
                << "::kNotSet;\n"
                << inner << "    }\n"
                << inner << "}\n";

            for (int j = 0; j < oneof->field_count(); ++j) {
                const FieldDescriptor* f = oneof->field(j);
                const std::string fname = SafeIdent(std::string(f->name()));
                const std::string ftype = FieldCppType(msg, f);
                const bool indirect = IsOneofBranchIndirect(msg, f);
                const std::string deref = indirect ? ".get()" : "";
                const int idx = j + 1;
                out << inner << "bool has_" << f->name()
                    << "() const noexcept { return this->" << oname
                    << ".index() == " << idx << "; }\n"
                    << inner << "const " << ftype << "& " << fname
                    << "() const { return std::get<" << idx << ">(this->"
                    << oname << ")" << deref << "; }\n";
                if (defer && indirect) {
                    out << inner << ftype << "& mutable_" << f->name()
                        << "();\n";
                } else {
                    out << inner << ftype << "& mutable_" << f->name()
                        << "() {\n"
                        << inner << "    if (this->" << oname
                        << ".index() != " << idx << ") { this->" << oname
                        << ".emplace<" << idx << ">(); }\n"
                        << inner << "    return std::get<" << idx << ">(this->"
                        << oname << ")" << deref << ";\n"
                        << inner << "}\n";
                }
                out << inner << "template <typename... Args>\n"
                    << inner << ftype << "& emplace_" << f->name()
                    << "(Args&&... args) {\n"
                    << inner << "    return this->" << oname << ".emplace<"
                    << idx << ">(std::forward<Args>(args)...)" << deref << ";\n"
                    << inner << "}\n";
            }
            out << "\n";
        }
    }

    void EmitFields(std::ostringstream& out, const Descriptor* msg,
                    const std::string& inner) {
        for (int i = 0; i < msg->field_count(); ++i) {
            const FieldDescriptor* f = msg->field(i);
            if (f->real_containing_oneof() != nullptr) continue;
            const std::string fname = SafeIdent(std::string(f->name()));
            if (f->is_map()) {
                const Descriptor* entry = f->message_type();
                const FieldDescriptor* val = entry->map_value();
                std::string vtype = FieldCppType(msg, val);
                if (val->type() == FieldDescriptor::TYPE_MESSAGE &&
                    IsTargetIncomplete(msg, val->message_type())) {
                    vtype = "::brpc_fast::IndirectBox<" + vtype + ">";
                }
                out << inner << "std::unordered_map<"
                    << FieldCppType(msg, entry->map_key()) << ", " << vtype
                    << "> " << fname << ";\n";
            } else if (f->is_repeated()) {
                out << inner << "std::vector<" << FieldCppType(msg, f) << "> "
                    << fname << ";\n";
            } else if (IsFieldOptionalWrapped(f)) {
                const bool indirect =
                    f->type() == FieldDescriptor::TYPE_MESSAGE &&
                    IsTargetIncomplete(msg, f->message_type());
                out << inner
                    << (indirect ? "::brpc_fast::IndirectOptional<"
                                 : "std::optional<")
                    << FieldCppType(msg, f) << "> " << fname << ";\n";
            } else {
                const std::string ftype = FieldCppType(msg, f);
                const std::string def = FieldDefaultExpr(msg, f);
                out << inner << ftype << " " << fname
                    << (def.empty() ? "" : (" = " + def)) << ";\n";
            }
        }
    }

    void EmitClearBody(std::ostringstream& out, const Descriptor* msg,
                       const std::string& inner, const std::string& qual) {
        const std::string prefix = qual.empty() ? "" : "inline ";
        out << inner << prefix << "void " << qual << "Clear() {\n";
        for (int i = 0; i < msg->field_count(); ++i) {
            const FieldDescriptor* f = msg->field(i);
            if (f->real_containing_oneof() == nullptr) {
                out << inner << "    " << FieldResetCall(f) << ";\n";
            }
        }
        for (int i = 0; i < msg->real_oneof_decl_count(); ++i) {
            out << inner << "    this->"
                << SafeIdent(std::string(msg->oneof_decl(i)->name()))
                << ".emplace<0>();\n";
        }
        if (opts_.preserve_unknown_fields) {
            out << inner << "    this->_unknown_fields.Clear();\n";
        }
        out << inner << "}\n\n";
    }

    void EmitEncodeBodies(std::ostringstream& out, const Descriptor* msg,
                          const std::string& inner, const std::string& qual) {
        const std::string prefix = qual.empty() ? "" : "inline ";
        if (qual.empty()) {
            out << inner << "static constexpr bool kUsesSizeTape = "
                << (MessageUsesSizeTape(msg) ? "true" : "false") << ";\n"
                << inner << "static constexpr bool kIsColumnarEligible = "
                << (IsColumnarEligible(msg, opts_) ? "true" : "false")
                << ";\n\n";
        }

        // Pass 1: ComputeSize
        out << inner << prefix << "size_t " << qual << "ComputeSize("
            << kDetailNs << "SizeTape& tape) const {\n"
            << inner << "    (void)tape;\n"
            << inner << "    size_t sz = 0;\n";
        for (int i = 0; i < msg->field_count(); ++i) {
            const FieldDescriptor* f = msg->field(i);
            if (f->real_containing_oneof() != nullptr) continue;
            out << inner << "    sz += " << kDetailNs << "SizeField<"
                << f->number() << "u, "
                << (IsProto3ImplicitField(f) ? "true" : "false") << ">(this->"
                << SafeIdent(std::string(f->name())) << ", tape);\n";
        }
        for (int i = 0; i < msg->real_oneof_decl_count(); ++i) {
            const OneofDescriptor* oneof = msg->oneof_decl(i);
            const std::string oname = SafeIdent(std::string(oneof->name()));
            out << inner << "    switch (this->" << oname << ".index()) {\n";
            for (int j = 0; j < oneof->field_count(); ++j) {
                const FieldDescriptor* f = oneof->field(j);
                out << inner << "        case " << (j + 1)
                    << ": sz += " << kDetailNs << "SizeField<" << f->number()
                    << "u>(std::get<" << (j + 1) << ">(this->" << oname
                    << "), tape); break;\n";
            }
            out << inner << "        default: break;\n" << inner << "    }\n";
        }
        if (opts_.preserve_unknown_fields) {
            out << inner << "    sz += this->_unknown_fields.Size();\n";
        }
        out << inner << "    return sz;\n" << inner << "}\n\n";

        // Pass 2: EncodeUnchecked
        out << inner << prefix << "uint8_t* " << qual
            << "EncodeUnchecked(uint8_t* dst, const uint32_t*& tape) "
               "const noexcept {\n"
            << inner << "    (void)tape;\n";
        for (int i = 0; i < msg->field_count(); ++i) {
            const FieldDescriptor* f = msg->field(i);
            if (f->real_containing_oneof() != nullptr) continue;
            out << inner << "    dst = " << kDetailNs << "EncodeFieldUnchecked<"
                << f->number() << "u, "
                << (IsProto3ImplicitField(f) ? "true" : "false")
                << ">(dst, this->" << SafeIdent(std::string(f->name()))
                << ", tape);\n";
        }
        for (int i = 0; i < msg->real_oneof_decl_count(); ++i) {
            const OneofDescriptor* oneof = msg->oneof_decl(i);
            const std::string oname = SafeIdent(std::string(oneof->name()));
            out << inner << "    switch (this->" << oname << ".index()) {\n";
            for (int j = 0; j < oneof->field_count(); ++j) {
                const FieldDescriptor* f = oneof->field(j);
                out << inner << "        case " << (j + 1)
                    << ": dst = " << kDetailNs << "EncodeFieldUnchecked<"
                    << f->number() << "u>(dst, std::get<" << (j + 1)
                    << ">(this->" << oname << "), tape); break;\n";
            }
            out << inner << "        default: break;\n" << inner << "    }\n";
        }
        if (opts_.preserve_unknown_fields) {
            out << inner
                << "    dst = this->_unknown_fields.EncodeUnchecked(dst);\n";
        }
        out << inner << "    return dst;\n" << inner << "}\n\n";
    }

    void EmitDecodeBodies(std::ostringstream& out, const Descriptor* msg,
                          const std::string& inner, const std::string& qual) {
        const std::string prefix = qual.empty() ? "" : "inline ";
        const std::string unknown_arg = opts_.preserve_unknown_fields
                                            ? "&this->_unknown_fields"
                                            : "nullptr";

        // Fast path: DecodeContiguous (speculative canonical in-order 1- or
        // 2-byte tags, overwriting existing fields in-place)
        out << inner << prefix << "bool " << qual
            << "DecodeContiguous(const uint8_t* cur, const uint8_t* end, "
               "int remaining_depth) {\n";
        if (opts_.preserve_unknown_fields) {
            out << inner << "    this->_unknown_fields.Clear();\n";
        }
        for (int i = 0; i < msg->field_count(); ++i) {
            const FieldDescriptor* f = msg->field(i);
            if (f->real_containing_oneof() != nullptr) continue;
            if (!HasFastTag(f)) {
                out << inner << "    " << FieldResetCall(f) << ";\n";
                continue;
            }
            const std::string fname = SafeIdent(std::string(f->name()));
            out << inner << "    if (!" << kDetailNs
                << "TryDecodeFieldContiguous<" << f->number()
                << "u>(cur, end, remaining_depth, this->" << fname
                << ")) [[unlikely]] { return false; }\n";
        }
        for (int i = 0; i < msg->real_oneof_decl_count(); ++i) {
            const OneofDescriptor* oneof = msg->oneof_decl(i);
            const std::string oname = SafeIdent(std::string(oneof->name()));
            std::vector<const FieldDescriptor*> fast_branches;
            for (int j = 0; j < oneof->field_count(); ++j) {
                if (HasFastTag(oneof->field(j))) {
                    fast_branches.push_back(oneof->field(j));
                }
            }
            if (fast_branches.empty()) {
                out << inner << "    this->" << oname << ".emplace<0>();\n";
                continue;
            }
            for (size_t k = 0; k < fast_branches.size(); ++k) {
                const FieldDescriptor* f = fast_branches[k];
                const int idx = f->index_in_oneof() + 1;
                const std::string vtype = OneofVariantType(msg, f);
                out << inner << "    " << (k == 0 ? "if" : "} else if") << " ("
                    << kDetailNs << "MatchConstTag<" << kDetailNs
                    << "kFieldTag<" << f->number() << "u, " << vtype
                    << ">>(cur, end)) {\n"
                    << inner << "        if (this->" << oname
                    << ".index() != " << idx << ") { this->" << oname
                    << ".emplace<" << idx << ">(); }\n"
                    << inner << "        if (!" << kDetailNs << "ValueCodec<"
                    << vtype
                    << ">::DecodeValueContiguous(cur, end, remaining_depth, "
                       "std::get<"
                    << idx << ">(this->" << oname
                    << "))) [[unlikely]] { return false; }\n";
            }
            out << inner << "    } else {\n"
                << inner << "        this->" << oname << ".emplace<0>();\n"
                << inner << "    }\n";
        }
        out << inner << "    if (cur == end) [[likely]] {\n"
            << inner << "        return true;\n"
            << inner << "    }\n"
            << inner << "    " << kDetailNs
            << "FastReader rem(cur, static_cast<size_t>(end - cur), "
               "remaining_depth);\n"
            << inner << "    return DecodeLoop<false>(rem);\n"
            << inner << "}\n\n";

        // General multi-block & any-order decoder: Overwrite=true reuses
        // existing slots in-place and resets unseen fields; Overwrite=false
        // appends/merges trailing fields after DecodeContiguous or recursive
        // submessage merges.
        out << inner << "template <bool Overwrite>\n"
            << inner << "BRPC_FAST_NOINLINE bool " << qual << "DecodeLoop("
            << kDetailNs << "FastReader& r) {\n";
        int regular_field_count = 0;
        for (int i = 0; i < msg->field_count(); ++i) {
            if (msg->field(i)->real_containing_oneof() == nullptr) {
                ++regular_field_count;
            }
        }
        const bool can_track_seen =
            regular_field_count <= 64 && msg->real_oneof_decl_count() <= 64;
        if (!can_track_seen) {
            out << inner
                << "    if constexpr (Overwrite) { Clear(); return "
                   "DecodeLoop<false>(r); }\n";
        } else if (opts_.preserve_unknown_fields) {
            out << inner
                << "    if constexpr (Overwrite) { "
                   "this->_unknown_fields.Clear(); }\n";
        }
        out << inner << "    uint64_t seen_fields = 0;\n"
            << inner << "    uint64_t seen_oneofs = 0;\n"
            << inner << "    (void)seen_fields;\n"
            << inner << "    (void)seen_oneofs;\n"
            << inner << "    while (!r.Eof()) {\n"
            << inner << "        uint32_t tag = 0;\n"
            << inner << "        if (!r.ReadTag(tag)) { return false; }\n"
            << inner << "        const uint32_t field_number = " << kDetailNs
            << "TagFieldNumber(tag);\n"
            << inner << "        const " << kDetailNs
            << "WireType wt = " << kDetailNs << "TagWireType(tag);\n"
            << inner << "        switch (field_number) {\n"
            << inner << "            case 0u:\n"
            << inner << "                return false;\n";

        int regular_bit = 0;
        for (int i = 0; i < msg->field_count(); ++i) {
            const FieldDescriptor* f = msg->field(i);
            out << inner << "            case " << f->number() << "u:\n";
            if (const OneofDescriptor* oneof = f->real_containing_oneof()) {
                const int oneof_idx = can_track_seen ? oneof->index() : 0;
                out << inner << "                if (!" << kDetailNs
                    << "DecodeOneofBranchInLoop<Overwrite, "
                    << (f->index_in_oneof() + 1) << ">(r, tag, wt, this->"
                    << SafeIdent(std::string(oneof->name()))
                    << ", seen_oneofs, " << oneof_idx << ", " << unknown_arg
                    << ")) { return false; }\n";
            } else {
                const int bit_idx = can_track_seen ? regular_bit++ : 0;
                out << inner << "                if (!" << kDetailNs
                    << "DecodeFieldInLoop<Overwrite>(r, tag, wt, this->"
                    << SafeIdent(std::string(f->name())) << ", seen_fields, "
                    << bit_idx << ", " << unknown_arg
                    << ")) { return false; }\n";
            }
            out << inner << "                break;\n";
        }
        out << inner << "            default:\n";
        if (opts_.preserve_unknown_fields) {
            out << inner
                << "                if (!r.CaptureUnknownField(tag, wt, "
                   "&this->_unknown_fields)) { return false; }\n";
        } else {
            out << inner
                << "                if (!r.SkipUnknownField(wt)) { return "
                   "false; }\n";
        }
        out << inner << "                break;\n"
            << inner << "        }\n"
            << inner << "    }\n";

        if (can_track_seen) {
            out << inner << "    if constexpr (Overwrite) {\n";
            regular_bit = 0;
            for (int i = 0; i < msg->field_count(); ++i) {
                const FieldDescriptor* f = msg->field(i);
                if (f->real_containing_oneof() != nullptr) continue;
                const int bit_idx = regular_bit++;
                out << inner << "        if ((seen_fields & (uint64_t{1} << "
                    << bit_idx << ")) == 0) { " << FieldResetCall(f) << "; }\n";
            }
            for (int i = 0; i < msg->real_oneof_decl_count(); ++i) {
                const OneofDescriptor* oneof = msg->oneof_decl(i);
                out << inner << "        if ((seen_oneofs & (uint64_t{1} << "
                    << oneof->index() << ")) == 0) { this->"
                    << SafeIdent(std::string(oneof->name()))
                    << ".emplace<0>(); }\n";
            }
            out << inner << "    }\n";
        }
        out << inner << "    return true;\n" << inner << "}\n\n";

        // Top-level Decode dispatch
        out << inner << prefix << "bool " << qual << "Decode(" << kDetailNs
            << "FastReader& r) {\n"
            << inner << "    if (r.IsContiguous()) [[likely]] {\n"
            << inner << "        const uint8_t* cur = r.Cur();\n"
            << inner << "        const uint8_t* end = r.SegEnd();\n"
            << inner << "        r.ConsumeContiguous();\n"
            << inner
            << "        return DecodeContiguous(cur, end, "
               "r.RemainingDepth());\n"
            << inner << "    }\n"
            << inner << "    return DecodeLoop<true>(r);\n"
            << inner << "}\n\n";
    }

    void EmitColumnarBodies(std::ostringstream& out, const Descriptor* msg,
                            const std::string& inner, const std::string& qual) {
        const std::string cls_type = qual.empty()
                                         ? SafeIdent(std::string(msg->name()))
                                         : QualifiedInNamespace(msg);
        const std::string vec_prefix = qual.empty() ? "static " : "";
        const std::string sing_prefix =
            qual.empty() ? "static BRPC_FAST_NOINLINE " : "inline ";

        if (qual.empty() && IsColumnarEligible(msg, opts_)) {
            out << inner << "static size_t ComputeColumnarSize(const "
                << cls_type << "* data, uint32_t count, " << kDetailNs
                << "SizeTape& tape) {\n"
                << inner << "    const size_t body_slot = tape.Reserve();\n"
                << inner << "    size_t body_len = " << kDetailNs
                << "Varint32Size(count);\n";
            for (int i = 0; i < msg->field_count(); ++i) {
                const FieldDescriptor* f = msg->field(i);
                const std::string fname = SafeIdent(std::string(f->name()));
                out << inner << "    body_len += " << kDetailNs
                    << "SizeScalarColumn<" << f->number()
                    << "u>(data, count, &" << cls_type << "::" << fname
                    << ", tape);\n";
            }
            out << inner
                << "    tape.Set(body_slot, static_cast<uint32_t>(body_len));\n"
                << inner << "    return body_len;\n"
                << inner << "}\n\n";

            out << inner
                << "static uint8_t* EncodeColumnarUnchecked(uint8_t* dst, "
                   "const "
                << cls_type
                << "* data, uint32_t count, const uint32_t*& tape) "
                   "noexcept {\n"
                << inner << "    const uint32_t body_len = *tape++;\n"
                << inner << "    dst = " << kDetailNs
                << "WriteVarint32Raw(dst, body_len);\n"
                << inner << "    dst = " << kDetailNs
                << "WriteVarint32Raw(dst, count);\n";
            for (int i = 0; i < msg->field_count(); ++i) {
                const FieldDescriptor* f = msg->field(i);
                const std::string fname = SafeIdent(std::string(f->name()));
                out << inner << "    dst = " << kDetailNs
                    << "EncodeScalarColumnUnchecked<" << f->number()
                    << "u>(dst, data, count, &" << cls_type << "::" << fname
                    << ", tape);\n";
            }
            out << inner << "    return dst;\n" << inner << "}\n\n";
        }

        int regular_field_count = 0;
        for (int i = 0; i < msg->field_count(); ++i) {
            if (msg->field(i)->real_containing_oneof() == nullptr) {
                ++regular_field_count;
            }
        }
        const bool can_track_seen = regular_field_count <= 64;

        out << inner << "template <bool Overwrite>\n"
            << inner << vec_prefix << "BRPC_FAST_NOINLINE bool " << qual
            << "DecodeColumnarVec(" << kDetailNs
            << "FastReader& sub, std::vector<" << cls_type << ">& vec) {\n"
            << inner << "    uint32_t count = 0;\n"
            << inner << "    if (!sub.ReadVarint32(count)) { return false; }\n"
            << inner << "    if (static_cast<uint64_t>(count) * sizeof("
            << cls_type << ") > " << kDetailNs
            << "kMaxExpandedSeqBytes) { return false; }\n"
            << inner << "    const size_t existing = vec.size();\n"
            << inner << "    const size_t base = Overwrite ? 0u : existing;\n"
            << inner
            << "    const size_t reuse_end = Overwrite ? (count < existing ? "
               "count : existing) : base;\n"
            << inner << "    vec.resize(base + count);\n";

        if (!can_track_seen || msg->real_oneof_decl_count() > 0 ||
            opts_.preserve_unknown_fields) {
            out << inner << "    if constexpr (Overwrite) {\n"
                << inner << "        if (reuse_end > base) {\n"
                << inner
                << "            for (size_t i = base; i < reuse_end; ++i) {\n";
            if (!can_track_seen) {
                out << inner << "                vec[i].Clear();\n";
            } else {
                for (int i = 0; i < msg->real_oneof_decl_count(); ++i) {
                    out << inner << "                vec[i]."
                        << SafeIdent(std::string(msg->oneof_decl(i)->name()))
                        << ".template emplace<0>();\n";
                }
                if (opts_.preserve_unknown_fields) {
                    out << inner
                        << "                vec[i]._unknown_fields.Clear();\n";
                }
            }
            out << inner << "            }\n"
                << inner << "        }\n"
                << inner << "    }\n";
        }

        out << inner << "    uint64_t seen_cols = 0;\n"
            << inner << "    (void)seen_cols;\n"
            << inner << "    while (!sub.Eof()) {\n"
            << inner << "        uint32_t tag = 0;\n"
            << inner << "        if (!sub.ReadTag(tag)) { return false; }\n"
            << inner << "        const uint32_t field_number = " << kDetailNs
            << "TagFieldNumber(tag);\n"
            << inner << "        const " << kDetailNs
            << "WireType wt = " << kDetailNs << "TagWireType(tag);\n"
            << inner << "        switch (field_number) {\n"
            << inner << "            case 0u:\n"
            << inner << "                return false;\n";
        int regular_bit = 0;
        for (int i = 0; i < msg->field_count(); ++i) {
            const FieldDescriptor* f = msg->field(i);
            out << inner << "            case " << f->number() << "u:\n";
            if (const OneofDescriptor* oneof = f->real_containing_oneof()) {
                out << inner << "                if (!" << kDetailNs
                    << "DecodeColumnIntoOneof<" << (f->index_in_oneof() + 1)
                    << ">(sub, wt, vec, base, count, &" << cls_type << "::"
                    << SafeIdent(std::string(oneof->name()))
                    << ")) { return false; }\n";
            } else {
                const int bit_idx = can_track_seen ? regular_bit++ : -1;
                out << inner << "                if (!" << kDetailNs
                    << "DecodeColumnIntoField(sub, wt, vec, base, count, &"
                    << cls_type << "::" << SafeIdent(std::string(f->name()))
                    << ", seen_cols, " << bit_idx << ")) { return false; }\n";
            }
            out << inner << "                break;\n";
        }
        out << inner << "            default:\n"
            << inner
            << "                if (!sub.SkipUnknownField(wt)) { return "
               "false; }\n"
            << inner << "                break;\n"
            << inner << "        }\n"
            << inner << "    }\n";

        if (can_track_seen && regular_field_count > 0) {
            out << inner << "    if constexpr (Overwrite) {\n"
                << inner << "        if (reuse_end > base) {\n";
            regular_bit = 0;
            for (int i = 0; i < msg->field_count(); ++i) {
                const FieldDescriptor* f = msg->field(i);
                if (f->real_containing_oneof() != nullptr) continue;
                const int bit_idx = regular_bit++;
                const std::string fname = SafeIdent(std::string(f->name()));
                out << inner << "            if ((seen_cols & (uint64_t{1} << "
                    << bit_idx << ")) == 0) {\n"
                    << inner
                    << "                for (size_t i = base; i < reuse_end; "
                       "++i) { "
                    << kDetailNs << "ResetField(vec[i]." << fname << "); }\n"
                    << inner << "            }\n";
            }
            out << inner << "        }\n" << inner << "    }\n";
        }
        out << inner << "    return true;\n" << inner << "}\n\n";

        out << inner << sing_prefix << kDetailNs
            << "CompatStatus " << qual << "DecodeColumnarSingular(" << kDetailNs
            << "FastReader& sub, uint32_t count, " << cls_type
            << "& target, bool already_present) {\n"
            << inner << "    if (!already_present) { target.Clear(); }\n"
            << inner << "    while (!sub.Eof()) {\n"
            << inner << "        uint32_t tag = 0;\n"
            << inner << "        if (!sub.ReadTag(tag)) { return " << kDetailNs
            << "CompatStatus::kError; }\n"
            << inner << "        const uint32_t field_number = " << kDetailNs
            << "TagFieldNumber(tag);\n"
            << inner << "        const " << kDetailNs
            << "WireType wt = " << kDetailNs << "TagWireType(tag);\n"
            << inner << "        switch (field_number) {\n"
            << inner << "            case 0u:\n"
            << inner << "                return " << kDetailNs
            << "CompatStatus::kError;\n";
        for (int i = 0; i < msg->field_count(); ++i) {
            const FieldDescriptor* f = msg->field(i);
            out << inner << "            case " << f->number() << "u:\n";
            if (const OneofDescriptor* oneof = f->real_containing_oneof()) {
                out << inner << "                if (!" << kDetailNs
                    << "DecodeColumnIntoSingularOneof<"
                    << (f->index_in_oneof() + 1) << ">(sub, wt, count, target."
                    << SafeIdent(std::string(oneof->name())) << ")) { return "
                    << kDetailNs << "CompatStatus::kError; }\n";
            } else {
                out << inner << "                if (!" << kDetailNs
                    << "DecodeColumnIntoSingularField(sub, wt, count, target."
                    << SafeIdent(std::string(f->name())) << ")) { return "
                    << kDetailNs << "CompatStatus::kError; }\n";
            }
            out << inner << "                break;\n";
        }
        out << inner << "            default:\n"
            << inner << "                if (!sub.SkipUnknownField(wt)) "
            << "{ return " << kDetailNs << "CompatStatus::kError; }\n"
            << inner << "                break;\n"
            << inner << "        }\n"
            << inner << "    }\n"
            << inner << "    return " << kDetailNs
            << "CompatStatus::kDecoded;\n"
            << inner << "}\n\n";
    }

    void EmitServiceHeader(std::ostringstream& out,
                           const ServiceDescriptor* svc) {
        const std::string cls = SafeIdent(std::string(svc->name()));
        out << "class " << cls << " : public ::brpc_fast::FastServiceBase {\n"
            << "   public:\n"
            << "    ~" << cls << "() override = default;\n\n"
            << "    std::string_view ServiceFullName() const override {\n"
            << "        return \"" << svc->full_name() << "\";\n"
            << "    }\n"
            << "    std::string_view ServiceShortName() const override {\n"
            << "        return \"" << svc->name() << "\";\n"
            << "    }\n\n"
            << "    static const std::vector<const "
               "::google::protobuf::MethodDescriptor*>&\n"
            << "    MethodDescriptors();\n\n";

        for (int i = 0; i < svc->method_count(); ++i) {
            const MethodDescriptor* m = svc->method(i);
            const std::string req_t =
                GeneratedCppType(std::string(m->input_type()->full_name()));
            const std::string resp_t =
                GeneratedCppType(std::string(m->output_type()->full_name()));
            out << "    virtual ::brpc_fast::RpcStatus "
                << SafeIdent(std::string(m->name()))
                << "(const ::brpc_fast::RpcContext& ctx, const " << req_t
                << "& request, " << resp_t << "& response);\n";
        }
        out << "\n    void RegisterMethods(" << kDetailNs
            << "FastMethodRegistry& registry) override;\n"
            << "};\n\n";

        out << "class " << cls << "_Stub {\n"
            << "   public:\n"
            << "    explicit " << cls
            << "_Stub(::brpc::ChannelBase* channel) : channel_(channel) {}\n\n";
        for (int i = 0; i < svc->method_count(); ++i) {
            const MethodDescriptor* m = svc->method(i);
            const std::string mname = SafeIdent(std::string(m->name()));
            const std::string req_t =
                GeneratedCppType(std::string(m->input_type()->full_name()));
            const std::string resp_t =
                GeneratedCppType(std::string(m->output_type()->full_name()));
            out << "    [[nodiscard]] ::brpc_fast::RpcStatus " << mname
                << "(const " << req_t << "& request, " << resp_t
                << "& response, const ::brpc_fast::CallOptions& "
                   "options = {});\n"
                << "    [[nodiscard]] ::brpc_fast::RpcFuture<" << resp_t << "> "
                << mname << "Async(const " << req_t
                << "& request, const ::brpc_fast::CallOptions& "
                   "options = {});\n";
        }
        out << "\n   private:\n"
            << "    ::brpc::ChannelBase* channel_;\n"
            << "};\n";
    }

    void EmitServiceSource(std::ostringstream& out,
                           const ServiceDescriptor* svc) {
        const std::string cls = SafeIdent(std::string(svc->name()));
        out << "const std::vector<const "
               "::google::protobuf::MethodDescriptor*>&\n"
            << cls << "::MethodDescriptors() {\n"
            << "    static const auto descriptors =\n"
            << "        " << kDetailNs
            << "RegisterSyntheticServiceDescriptors(\n"
            << "            \"" << file_->name() << "\", \"" << file_->package()
            << "\", \"" << svc->name() << "\", {";
        for (int i = 0; i < svc->method_count(); ++i) {
            if (i > 0) out << ", ";
            out << "\"" << svc->method(i)->name() << "\"";
        }
        out << "});\n    return descriptors;\n}\n\n";

        for (int i = 0; i < svc->method_count(); ++i) {
            const MethodDescriptor* m = svc->method(i);
            const std::string req_t =
                GeneratedCppType(std::string(m->input_type()->full_name()));
            const std::string resp_t =
                GeneratedCppType(std::string(m->output_type()->full_name()));
            out << "::brpc_fast::RpcStatus " << cls
                << "::" << SafeIdent(std::string(m->name()))
                << "(const ::brpc_fast::RpcContext& /*ctx*/, const " << req_t
                << "& /*request*/, " << resp_t << "& /*response*/) {\n"
                << "    return ::brpc_fast::RpcStatus::Error(\n"
                << "        ::brpc::ENOMETHOD, \"Method " << m->full_name()
                << " is not implemented\");\n"
                << "}\n\n";
        }

        out << "void " << cls << "::RegisterMethods(" << kDetailNs
            << "FastMethodRegistry& registry) {\n";
        for (int i = 0; i < svc->method_count(); ++i) {
            const MethodDescriptor* m = svc->method(i);
            const std::string req_t =
                GeneratedCppType(std::string(m->input_type()->full_name()));
            const std::string resp_t =
                GeneratedCppType(std::string(m->output_type()->full_name()));
            out << "    registry.RegisterMethod(\"" << m->name()
                << "\", [this](::brpc::Controller* cntl,\n"
                   "                               const "
                   "::brpc::SerializedRequest* raw_req,\n"
                   "                               ::brpc::SerializedResponse* "
                   "raw_resp,\n"
                   "                               "
                   "::google::protobuf::Closure* done) {\n"
                << "        " << kDetailNs << "DispatchServerMethod<" << req_t
                << ", " << resp_t << ">(cntl, raw_req, raw_resp, done,\n"
                << "            [this](const "
                   "::brpc_fast::RpcContext& ctx, const "
                << req_t << "& req, " << resp_t
                << "& resp) -> ::brpc_fast::RpcStatus {\n"
                << "                return this->"
                << SafeIdent(std::string(m->name())) << "(ctx, req, resp);\n"
                << "            });\n"
                << "    });\n";
        }
        out << "}\n\n";

        for (int i = 0; i < svc->method_count(); ++i) {
            const MethodDescriptor* m = svc->method(i);
            const std::string mname = SafeIdent(std::string(m->name()));
            const std::string req_t =
                GeneratedCppType(std::string(m->input_type()->full_name()));
            const std::string resp_t =
                GeneratedCppType(std::string(m->output_type()->full_name()));
            out << "::brpc_fast::RpcStatus " << cls << "_Stub::" << mname
                << "(const " << req_t << "& request, " << resp_t
                << "& response, const ::brpc_fast::CallOptions& "
                   "options) {\n"
                << "    return " << kDetailNs << "CallClientMethod<" << req_t
                << ", " << resp_t << ">(channel_, " << cls
                << "::MethodDescriptors()[" << i
                << "], request, response, options);\n"
                << "}\n\n"
                << "::brpc_fast::RpcFuture<" << resp_t << "> " << cls
                << "_Stub::" << mname << "Async(const " << req_t
                << "& request, const ::brpc_fast::CallOptions& "
                   "options) {\n"
                << "    return " << kDetailNs << "CallClientMethodAsync<"
                << req_t << ", " << resp_t << ">(channel_, " << cls
                << "::MethodDescriptors()[" << i << "], request, options);\n"
                << "}\n\n";
        }
    }

    const FileDescriptor* file_;
    CodegenOptions opts_;
    const std::unordered_set<const FileDescriptor*>& ref_files_;
    std::vector<const Descriptor*> ordered_top_;
    std::unordered_map<const Descriptor*, std::vector<const Descriptor*>>
        ordered_nested_;
    std::unordered_map<const Descriptor*, size_t> emit_order_;
};

void PrintUsage(const char* argv0) {
    std::cerr << "Usage: " << argv0
              << " --descriptor_set=<file.pb.desc> --proto=<file.proto>"
                 " --out_dir=<dir> [--preserve_unknown_fields]\n";
}

}  // namespace

int main(int argc, char* argv[]) {
    std::string desc_path;
    std::string target_proto;
    std::string out_dir;
    CodegenOptions opts;

    for (int i = 1; i < argc; ++i) {
        std::string arg(argv[i]);
        if (arg.rfind("--descriptor_set=", 0) == 0) {
            desc_path = arg.substr(17);
        } else if (arg.rfind("--proto=", 0) == 0) {
            target_proto = arg.substr(8);
        } else if (arg.rfind("--out_dir=", 0) == 0) {
            out_dir = arg.substr(10);
        } else if (arg == "--preserve_unknown_fields") {
            opts.preserve_unknown_fields = true;
        } else {
            PrintUsage(argv[0]);
            return 1;
        }
    }

    if (desc_path.empty() || target_proto.empty() || out_dir.empty()) {
        PrintUsage(argv[0]);
        return 1;
    }

    std::ifstream ifs(desc_path, std::ios::binary);
    if (!ifs) {
        std::cerr << "Failed to open descriptor set: " << desc_path << "\n";
        return 1;
    }

    FileDescriptorSet fds;
    if (!fds.ParseFromIstream(&ifs)) {
        std::cerr << "Failed to parse FileDescriptorSet: " << desc_path << "\n";
        return 1;
    }

    DescriptorPool pool;
    std::unordered_map<std::string, std::string> file_syntax;
    for (int i = 0; i < fds.file_size(); ++i) {
        const auto& fdp = fds.file(i);
        file_syntax[fdp.name()] = fdp.syntax();
        if (pool.BuildFile(fdp) == nullptr) {
            std::cerr << "Failed to build FileDescriptor for " << fdp.name()
                      << "\n";
            return 1;
        }
    }

    const FileDescriptor* file = pool.FindFileByName(target_proto);
    if (file == nullptr) {
        for (int i = 0; i < fds.file_size(); ++i) {
            const std::string name = fds.file(i).name();
            if (name == target_proto ||
                (name.size() > target_proto.size() &&
                 name.compare(name.size() - target_proto.size(),
                              target_proto.size(), target_proto) == 0 &&
                 name[name.size() - target_proto.size() - 1] == '/')) {
                file = pool.FindFileByName(name);
                break;
            }
        }
    }
    if (file == nullptr) {
        std::cerr << "Proto file not found in descriptor set: " << target_proto
                  << "\n";
        return 1;
    }

    auto check_proto3 = [&](const FileDescriptor* f) -> bool {
        auto it = file_syntax.find(std::string(f->name()));
        const std::string syn = (it != file_syntax.end() && !it->second.empty())
                                    ? it->second
                                    : "proto2";
        if (syn != "proto3") {
            std::cerr << "brpc_fast only supports proto3 syntax; found '" << syn
                      << "' in " << f->name() << "\n";
            return false;
        }
        return true;
    };

    if (!check_proto3(file)) {
        return 1;
    }

    for (int i = 0; i < file->service_count(); ++i) {
        const ServiceDescriptor* svc = file->service(i);
        for (int j = 0; j < svc->method_count(); ++j) {
            const MethodDescriptor* m = svc->method(j);
            if (m->client_streaming() || m->server_streaming()) {
                std::cerr
                    << "brpc_fast only supports unary RPC methods; streaming "
                       "method '"
                    << m->full_name() << "' is not supported\n";
                return 1;
            }
        }
    }

    std::unordered_set<const FileDescriptor*> visited;
    std::unordered_set<const FileDescriptor*> ref_files;
    CollectReferencedFiles(file, visited, ref_files);

    // Emit headers for any transitively referenced google/protobuf/* WKTs.
    // Always use default options for auto-emitted WKTs so multiple .proto files
    // sharing the same out_dir never produce conflicting WKT struct layouts.
    const CodegenOptions wkt_opts{};
    for (const FileDescriptor* dep : ref_files) {
        std::string dep_name(dep->name());
        if (dep_name.rfind("google/protobuf/", 0) != 0) continue;
        if (!check_proto3(dep)) return 1;
        std::unordered_set<const FileDescriptor*> wkt_visited;
        std::unordered_set<const FileDescriptor*> wkt_ref_files;
        CollectReferencedFiles(dep, wkt_visited, wkt_ref_files);
        CodeGenerator wkt_gen(dep, wkt_opts, wkt_ref_files);
        std::filesystem::path wkt_hdr =
            std::filesystem::path(out_dir) / (ProtoStem(dep_name) + ".fast.h");
        if (!WriteFileAtomic(wkt_hdr, wkt_gen.GenerateHeader())) {
            std::cerr << "Failed to write WKT header: " << wkt_hdr << "\n";
            return 1;
        }
    }

    CodeGenerator gen(file, opts, ref_files);
    std::string stem = ProtoStem(std::string(file->name()));
    std::filesystem::path hdr_path =
        std::filesystem::path(out_dir) / (stem + ".fast.h");
    std::filesystem::path src_path =
        std::filesystem::path(out_dir) / (stem + ".fast.cc");

    if (!WriteFileAtomic(hdr_path, gen.GenerateHeader())) {
        std::cerr << "Failed to write header: " << hdr_path << "\n";
        return 1;
    }
    if (!WriteFileAtomic(src_path, gen.GenerateSource())) {
        std::cerr << "Failed to write source: " << src_path << "\n";
        return 1;
    }
    return 0;
}
