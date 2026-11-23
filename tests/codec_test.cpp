#include <gtest/gtest.h>
#include <google/protobuf/timestamp.pb.h>

#include "test_preserve.fast.h"
#include "test_schema.fast.h"
#include "test_schema_v2.fast.h"

#include <butil/iobuf.h>

#include <cmath>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

namespace brpc_fast::test {
namespace {

using namespace ::brpc_fast_pb::brpc_fast::test;

TEST(BrpcFastCodecTest, EmptyAndDefaultValues) {
    AllTypes src;
    butil::IOBuf buf;
    src.EncodeTo(&buf);

    AllTypes dst;
    ASSERT_TRUE(dst.DecodeFrom(buf));
    EXPECT_EQ(src, dst);
    EXPECT_FALSE(dst.opt_int64.has_value());
    EXPECT_FALSE(dst.opt_string.has_value());
    EXPECT_FALSE(dst.opt_enum.has_value());
    EXPECT_FALSE(dst.sub_msg.has_value());
    EXPECT_EQ(dst.choice_case(), AllTypes::ChoiceCase::kNotSet);
}

TEST(BrpcFastCodecTest, AllFieldTypesRoundTrip) {
    AllTypes src;
    src.f_bool = true;
    src.f_int32 = -123456;
    src.f_int64 = -9876543210123LL;
    src.f_uint32 = 4000000000u;
    src.f_uint64 = 18000000000000000000ULL;
    src.f_sint32 = -42;
    src.f_sint64 = -9000000000000LL;
    src.f_fixed32 = 0xDEADBEEFu;
    src.f_fixed64 = 0xCAFEBABE12345678ULL;
    src.f_sfixed32 = std::numeric_limits<int32_t>::min();
    src.f_sfixed64 = std::numeric_limits<int64_t>::max();
    src.f_float = 3.25f;
    src.f_double = 2.718281828459045;
    src.f_string = "hello brpc_fast";
    src.f_bytes = std::string("\x00\x01\x02\xFF\xFE", 5);
    src.f_enum = ReplicaStatus::REPLICA_COMPLETE;
    src.f_local_enum = AllTypes::LocalKind::LOCAL_HOT;

    src.opt_int64 =
        0;  // Explicit optional with value 0 must preserve presence!
    src.opt_string =
        "";  // Explicit optional empty string must preserve presence!
    src.opt_enum = ReplicaStatus::REPLICA_UNKNOWN;

    src.rep_uint64 = {1, 2, 3, 10000000000ULL, 0xFFFFFFFFFFFFFFFFULL};
    src.rep_int32 = {-10, 0, 10, 20};
    src.rep_bool = {true, false, true, true, false};
    src.rep_string = {"alpha", "", "gamma"};
    src.rep_enum = {ReplicaStatus::REPLICA_UNKNOWN,
                    ReplicaStatus::REPLICA_PROCESSING,
                    ReplicaStatus::REPLICA_COMPLETE};

    LaterMessage item1;
    item1.id = 101;
    item1.label = "first";
    item1.offsets = {0, 4096, 8192};
    LaterMessage item2;
    item2.id = 202;
    item2.label = "second";
    src.rep_msg = {item1, item2};

    src.sub_msg = item1;
    src.emplace_choice_alt_text("second-string-branch");

    src.map_str_i64["k1"] = 111;
    src.map_str_i64["k2"] = -222;
    src.map_i32_msg[7] = item2;

    src.large_field_num = 999999999ULL;
    src.very_large_field_num = "field-5000";

    butil::IOBuf buf;
    src.EncodeTo(&buf);

    AllTypes dst;
    ASSERT_TRUE(dst.DecodeFrom(buf));
    EXPECT_EQ(src, dst);

    // Round-trip idempotence: re-encoding the decoded message decodes equally.
    butil::IOBuf reencoded;
    dst.EncodeTo(&reencoded);
    AllTypes dst2;
    ASSERT_TRUE(dst2.DecodeFrom(reencoded));
    EXPECT_EQ(src, dst2);

    ASSERT_TRUE(dst.opt_int64.has_value());
    EXPECT_EQ(*dst.opt_int64, 0);
    ASSERT_TRUE(dst.opt_string.has_value());
    EXPECT_EQ(*dst.opt_string, "");
    ASSERT_TRUE(dst.opt_enum.has_value());
    EXPECT_EQ(*dst.opt_enum, ReplicaStatus::REPLICA_UNKNOWN);

    EXPECT_EQ(dst.choice_case(), AllTypes::ChoiceCase::kChoiceAltText);
    EXPECT_TRUE(dst.has_choice_alt_text());
    EXPECT_FALSE(dst.has_choice_text());
    EXPECT_EQ(dst.choice_alt_text(), "second-string-branch");
}

TEST(BrpcFastCodecTest, OneofDuplicateTypesAndMessageBranch) {
    // First string branch
    {
        AllTypes src;
        src.emplace_choice_text("primary");
        butil::IOBuf buf;
        src.EncodeTo(&buf);
        AllTypes dst;
        ASSERT_TRUE(dst.DecodeFrom(buf));
        EXPECT_EQ(dst.choice_case(), AllTypes::ChoiceCase::kChoiceText);
        EXPECT_TRUE(dst.has_choice_text());
        EXPECT_FALSE(dst.has_choice_alt_text());
        EXPECT_EQ(dst.choice_text(), "primary");
    }

    // Message branch
    {
        AllTypes src;
        auto& m = src.mutable_choice_msg();
        m.id = 777;
        m.label = "nested-oneof";
        m.offsets = {16, 32};
        butil::IOBuf buf;
        src.EncodeTo(&buf);
        AllTypes dst;
        ASSERT_TRUE(dst.DecodeFrom(buf));
        EXPECT_EQ(dst.choice_case(), AllTypes::ChoiceCase::kChoiceMsg);
        ASSERT_TRUE(dst.has_choice_msg());
        EXPECT_EQ(dst.choice_msg().id, 777u);
        EXPECT_EQ(dst.choice_msg().label, "nested-oneof");
        EXPECT_EQ(dst.choice_msg().offsets, (std::vector<uint64_t>{16, 32}));
    }
}

TEST(BrpcFastCodecTest, LargePayloadMultiBlockIOBuf) {
    // Large nested payload whose serialized bytes are split across multiple
    // non-contiguous IOBuf blocks must round-trip losslessly.
    BatchGetReplicaResponse src;
    src.results.resize(256);
    for (size_t i = 0; i < src.results.size(); ++i) {
        auto& r = src.results[i];
        r.error_code = 0;
        r.lease_ttl_ms = 5000 + i;
        r.object_checksum = 0x10000000ULL + i;
        r.replicas.resize(2);
        for (size_t j = 0; j < 2; ++j) {
            r.replicas[j].replica_id = i * 10 + j;
            r.replicas[j].status = ReplicaStatus::REPLICA_COMPLETE;
            if (j == 0) {
                auto& mem = r.replicas[j].mutable_memory();
                mem.buffer_address = 0x7f0000000000ULL + i * 4096;
                mem.size = 1048576;
                mem.protocol = "rdma";
                mem.transport_endpoint = "10.0.0.1:12345";
                mem.numa_node = static_cast<int32_t>(i % 2);
                mem.hca_names = {"mlx5_0", "mlx5_1"};
            } else {
                auto& disk = r.replicas[j].mutable_disk();
                disk.file_path = "/mnt/nvme/obj_" + std::to_string(i);
                disk.object_size = 1048576;
            }
        }
    }

    butil::IOBuf encoded;
    src.EncodeTo(&encoded);

    // Split encoded bytes across multiple non-contiguous IOBuf blocks.
    std::string flat = encoded.to_string();
    butil::IOBuf multi_block;
    const size_t chunk = 1000;
    for (size_t off = 0; off < flat.size(); off += chunk) {
        size_t n = std::min(chunk, flat.size() - off);
        char* seg = new char[n];
        std::memcpy(seg, flat.data() + off, n);
        multi_block.append_user_data(
            seg, n, [](void* p) { delete[] static_cast<char*>(p); });
    }
    ASSERT_GT(multi_block.backing_block_num(), 1u);

    BatchGetReplicaResponse dst;
    ASSERT_TRUE(dst.DecodeFrom(multi_block));
    EXPECT_EQ(src, dst);
}

TEST(BrpcFastCodecTest, SchemaEvolutionForwardAndBackwardCompat) {
    // V2 -> V1 (old reader skips newly added fields 10, 11, 12, 300)
    {
        EvolutionV2 v2;
        v2.id = 42;
        v2.values = {10, 20, 30};
        v2.extra_tag = "v2-only";
        v2.extra_ids = {100, 200};
        v2.extra_meta["env"] = "prod";
        v2.large_tag_field = -999;

        butil::IOBuf buf;
        v2.EncodeTo(&buf);

        EvolutionV1 v1;
        ASSERT_TRUE(v1.DecodeFrom(buf));
        EXPECT_EQ(v1.id, 42u);
        EXPECT_TRUE(v1.deprecated_note.empty());
        EXPECT_EQ(v1.values, (std::vector<uint64_t>{10, 20, 30}));
    }

    // V1 -> V2 (new reader skips removed field 2 and keeps defaults for new
    // fields)
    {
        EvolutionV1 v1;
        v1.id = 88;
        v1.deprecated_note = "old-note-to-ignore";
        v1.values = {7, 8, 9};

        butil::IOBuf buf;
        v1.EncodeTo(&buf);

        EvolutionV2 v2;
        ASSERT_TRUE(v2.DecodeFrom(buf));
        EXPECT_EQ(v2.id, 88u);
        EXPECT_EQ(v2.values, (std::vector<uint64_t>{7, 8, 9}));
        EXPECT_FALSE(v2.extra_tag.has_value());
        EXPECT_TRUE(v2.extra_ids.empty());
        EXPECT_TRUE(v2.extra_meta.empty());
        EXPECT_EQ(v2.large_tag_field, 0);
    }
}

TEST(BrpcFastCodecTest, TruncatedAndCorruptBuffersFailCleanly) {
    AllTypes src;
    src.f_string = "payload-for-truncation-test";
    src.rep_uint64 = {1, 2, 3, 4};
    src.map_str_i64["a"] = 1;

    butil::IOBuf full;
    src.EncodeTo(&full);
    std::string raw = full.to_string();
    ASSERT_GT(raw.size(), 4u);

    // Every strict prefix must terminate cleanly without crashing/UB, and
    // cutting inside variable-length fields must be rejected.
    size_t rejected_prefixes = 0;
    for (size_t cut = 1; cut < raw.size(); ++cut) {
        butil::IOBuf partial;
        partial.append(raw.data(), cut);
        AllTypes dst;
        if (!dst.DecodeFrom(partial)) {
            ++rejected_prefixes;
        }
    }
    EXPECT_GT(rejected_prefixes, 0u);

    // Single-byte corruptions across a valid payload must never crash, hang,
    // or trigger unbounded allocations.
    for (size_t pos = 0; pos < raw.size(); ++pos) {
        std::string mutated = raw;
        mutated[pos] ^= static_cast<char>(0xFF);
        butil::IOBuf bad;
        bad.append(mutated.data(), mutated.size());
        AllTypes dst;
        (void)dst.DecodeFrom(bad);
    }

    // Obviously malformed garbage buffers must be rejected cleanly and leave
    // the destination in a clean default state (no stale/partial fields).
    butil::IOBuf garbage;
    std::string all_ff(64, '\xFF');
    garbage.append(all_ff.data(), all_ff.size());
    AllTypes dst = src;
    EXPECT_FALSE(dst.DecodeFrom(garbage));
    EXPECT_EQ(dst, AllTypes{});
}

TEST(BrpcFastCodecTest, TinyBlockStraddlingBoundary) {
    // Any valid encoded payload must decode identically regardless of how it
    // is fragmented across non-contiguous IOBuf blocks.
    auto make_chunked_iobuf = [](const std::string& flat, size_t chunk) {
        butil::IOBuf out;
        for (size_t off = 0; off < flat.size(); off += chunk) {
            size_t n = std::min(chunk, flat.size() - off);
            char* seg = new char[n];
            std::memcpy(seg, flat.data() + off, n);
            out.append_user_data(
                seg, n, [](void* p) { delete[] static_cast<char*>(p); });
        }
        return out;
    };

    // 1. AllTypes (scalars, strings, optional, repeated, oneof, maps, large
    //    field numbers)
    AllTypes src;
    src.f_bool = true;
    src.f_int32 = -77777;
    src.f_int64 = -9999999999999LL;
    src.f_uint64 = 0x123456789ABCDEF0ULL;
    src.f_double = 3.141592653589793;
    src.f_string = std::string(73, 's');
    src.f_bytes = std::string(41, '\xAB');
    src.opt_int64 = 42;
    src.opt_string = "straddling-optional-string";
    src.rep_uint64 = {11, 22, 33, 44, 55, 66, 77, 88, 99};
    src.rep_bool = {true, false, true, false, true, true, false};
    src.rep_string = {"one", "two-straddle", "three-longer-entry", ""};
    LaterMessage lm;
    lm.id = 9001;
    lm.label = "nested-message-label";
    lm.offsets = {1, 2, 3, 4, 5, 6};
    src.sub_msg = lm;
    src.rep_msg = {lm, lm, lm};
    src.emplace_choice_msg(lm);
    src.map_str_i64["alpha-key"] = 1001;
    src.map_str_i64["beta-key"] = -2002;
    src.map_i32_msg[123] = lm;
    src.large_field_num = 5555555ULL;
    src.very_large_field_num = "tag-5000-multi-byte-varint";

    butil::IOBuf encoded;
    src.EncodeTo(&encoded);
    std::string flat = encoded.to_string();

    for (size_t chunk : {size_t{1}, size_t{3}, size_t{7}, size_t{13}}) {
        butil::IOBuf fragmented = make_chunked_iobuf(flat, chunk);
        ASSERT_GT(fragmented.backing_block_num(), 1u);
        AllTypes dst;
        ASSERT_TRUE(dst.DecodeFrom(fragmented)) << "chunk=" << chunk;
        EXPECT_EQ(src, dst) << "chunk=" << chunk;
    }

    // 2. Evolution V2 -> V1 across fragmented 3-byte blocks
    EvolutionV2 v2;
    v2.id = 314159;
    v2.values = {1, 2, 3, 4, 5};
    v2.extra_tag = "skip-across-many-tiny-blocks-payload";
    v2.extra_ids = {10, 20, 30, 40, 50};
    v2.extra_meta["region"] = "us-west-2";
    v2.large_tag_field = -123456;

    butil::IOBuf v2_buf;
    v2.EncodeTo(&v2_buf);
    butil::IOBuf v2_frag = make_chunked_iobuf(v2_buf.to_string(), 3);
    EvolutionV1 v1;
    ASSERT_TRUE(v1.DecodeFrom(v2_frag));
    EXPECT_EQ(v1.id, 314159u);
    EXPECT_EQ(v1.values, (std::vector<uint64_t>{1, 2, 3, 4, 5}));
}

TEST(BrpcFastCodecTest, ClearResetsMessageStateForReuse) {
    AllTypes msg;
    msg.f_bool = true;
    msg.f_int64 = 12345;
    msg.f_string = std::string(128, 'a');
    msg.opt_int64 = 99;
    msg.rep_uint64.assign(64, 7);
    msg.map_str_i64["k"] = 1;
    msg.emplace_choice_text("branch");
    LaterMessage nested;
    nested.id = 42;
    nested.label = "sub";
    msg.sub_msg = nested;

    msg.Clear();
    EXPECT_EQ(msg, AllTypes{});
    EXPECT_FALSE(msg.opt_int64.has_value());
    EXPECT_FALSE(msg.sub_msg.has_value());
    EXPECT_EQ(msg.choice_case(), AllTypes::ChoiceCase::kNotSet);

    // Cleared message can be populated and round-tripped cleanly with no stale
    // fields leaking from previous state.
    msg.f_string = "after-clear";
    msg.emplace_choice_i64(77);
    butil::IOBuf buf;
    msg.EncodeTo(&buf);

    AllTypes dst;
    ASSERT_TRUE(dst.DecodeFrom(buf));
    EXPECT_EQ(dst, msg);
    EXPECT_FALSE(dst.f_bool);
    EXPECT_EQ(dst.f_int64, 0);
    EXPECT_FALSE(dst.opt_int64.has_value());
    EXPECT_TRUE(dst.rep_uint64.empty());
    EXPECT_TRUE(dst.map_str_i64.empty());
    EXPECT_EQ(dst.choice_case(), AllTypes::ChoiceCase::kChoiceI64);
    EXPECT_EQ(dst.choice_i64(), 77);
}

TEST(BrpcFastCodecTest, DecodeFromReplacesPreviousMessageState) {
    auto make_chunked = [](const std::string& flat, size_t chunk) {
        butil::IOBuf out;
        for (size_t off = 0; off < flat.size(); off += chunk) {
            size_t n = std::min(chunk, flat.size() - off);
            char* seg = new char[n];
            std::memcpy(seg, flat.data() + off, n);
            out.append_user_data(
                seg, n, [](void* p) { delete[] static_cast<char*>(p); });
        }
        return out;
    };

    AllTypes initial;
    initial.f_bool = true;
    initial.f_int64 = 999;
    initial.f_string = std::string(200, 'x');
    initial.opt_int64 = 42;
    initial.opt_string = "old-opt";
    initial.rep_uint64 = {1, 2, 3, 4, 5};
    initial.rep_string = {std::string(150, 'a'), std::string(150, 'b'),
                          std::string(150, 'c')};
    LaterMessage lm1;
    lm1.id = 11;
    lm1.label = "old-nested";
    lm1.offsets = {10, 20, 30};
    initial.sub_msg = lm1;
    initial.rep_msg = {lm1, lm1};
    initial.emplace_choice_text("old-branch");
    initial.map_str_i64["old_k"] = 100;
    initial.large_field_num = 777;
    initial.very_large_field_num = "old-5000";

    // Second message omits several fields, shrinks repeated fields, and changes
    // oneof branch and map contents.
    AllTypes replacement;
    replacement.f_int32 = -5;
    replacement.f_string = "short";
    replacement.rep_string = {std::string(160, 'z')};
    LaterMessage lm2;
    lm2.id = 22;
    lm2.label = "new-nested";
    replacement.rep_msg = {lm2};
    replacement.emplace_choice_i64(12345);
    replacement.map_str_i64["new_k"] = 200;

    // Third message grows repeated fields back and clears everything else.
    AllTypes empty_msg;

    butil::IOBuf rep_buf;
    replacement.EncodeTo(&rep_buf);
    butil::IOBuf empty_buf;
    empty_msg.EncodeTo(&empty_buf);
    butil::IOBuf init_buf;
    initial.EncodeTo(&init_buf);

    // Verify both contiguous and fragmented multi-block decode overwrite
    // cleanly without leaving stale state behind.
    for (bool fragmented : {false, true}) {
        AllTypes dst = initial;
        butil::IOBuf b1 =
            fragmented ? make_chunked(rep_buf.to_string(), 7) : rep_buf;
        ASSERT_TRUE(dst.DecodeFrom(b1)) << "fragmented=" << fragmented;
        EXPECT_EQ(dst, replacement) << "fragmented=" << fragmented;

        butil::IOBuf b2 =
            fragmented ? make_chunked(init_buf.to_string(), 11) : init_buf;
        ASSERT_TRUE(dst.DecodeFrom(b2)) << "fragmented=" << fragmented;
        EXPECT_EQ(dst, initial) << "fragmented=" << fragmented;

        ASSERT_TRUE(dst.DecodeFrom(empty_buf)) << "fragmented=" << fragmented;
        EXPECT_EQ(dst, empty_msg) << "fragmented=" << fragmented;
    }
}

TEST(BrpcFastCodecTest, Proto3CrossWidthSingularRepeatedAndMapEvolution) {
    auto make_chunked = [](const std::string& flat, size_t chunk) {
        butil::IOBuf out;
        for (size_t off = 0; off < flat.size(); off += chunk) {
            size_t n = std::min(chunk, flat.size() - off);
            char* seg = new char[n];
            std::memcpy(seg, flat.data() + off, n);
            out.append_user_data(
                seg, n, [](void* p) { delete[] static_cast<char*>(p); });
        }
        return out;
    };

    // ---- Direction 1: CompatV1 -> CompatV2 ----
    CompatV1 v1;
    v1.scalar_i32 = -42;
    v1.scalar_u32 = 0xFFFFFFFEu;
    v1.scalar_s32 = std::numeric_limits<int32_t>::min();
    v1.scalar_bool = true;
    v1.scalar_enum = static_cast<ReplicaStatus>(99);  // open enum value
    v1.opt_i32 = -7;

    // Even count (4) specifically catches the old kBytes memcpy corruption bug
    // when repeated int32 is decoded as repeated int64!
    v1.rep_i32 = {-100, 0, 42, std::numeric_limits<int32_t>::min()};
    v1.rep_u32 = {0u, 1u, 0xFFFFFFFFu};
    v1.rep_bool = {true, false, true};
    v1.rep_enum = {ReplicaStatus::REPLICA_UNKNOWN,
                   ReplicaStatus::REPLICA_COMPLETE,
                   static_cast<ReplicaStatus>(77)};

    v1.single_to_rep_i32 = -123;
    v1.rep_to_single_i32 = {10, 20, -30};  // last-one-wins -> -30
    v1.single_to_rep_f32 = 1.5f;
    v1.rep_to_single_f64 = {1.25, 2.5, 3.75};  // last-one-wins -> 3.75
    v1.single_to_rep_str = "only-one";
    v1.rep_to_single_str = {"first", "second", "last"};
    LaterMessage lm_single;
    lm_single.id = 77;
    lm_single.label = "single-msg";
    lm_single.offsets = {8, 16};
    v1.single_to_rep_msg = lm_single;

    // Two repeated submessages -> must MERGE into singular submessage in V2!
    LaterMessage lm_part1;
    lm_part1.id = 500;
    lm_part1.offsets = {1, 2};
    LaterMessage lm_part2;
    lm_part2.label = "merged-label";
    lm_part2.offsets = {3, 4};
    v1.rep_to_single_msg = {lm_part1, lm_part2};

    v1.map_to_rep_entries["alpha"] = 100;
    v1.map_to_rep_entries["beta"] = -200;

    CompatMapEntryI32Msg me1;
    me1.key = -5;
    me1.value = lm_single;
    CompatMapEntryI32Msg me2;
    me2.key = 10;
    me2.value = lm_part2;
    v1.rep_entries_to_map = {me1, me2};

    v1.map_widen_kv[-1] = -999;
    v1.map_widen_kv[42] = 12345;

    CompatMapEntryStrI64 se;
    se.key = "solo";
    se.value = 888;
    v1.single_entry_to_map = se;

    v1.emplace_eo_i32(-555);

    butil::IOBuf v1_buf;
    v1.EncodeTo(&v1_buf);

    for (bool fragmented : {false, true}) {
        butil::IOBuf in =
            fragmented ? make_chunked(v1_buf.to_string(), 5) : v1_buf;
        CompatV2 v2;
        // Pre-populate stale values to also exercise overwrite reuse path.
        v2.rep_i32 = {999, 888, 777};
        v2.rep_to_single_str = "stale";
        ASSERT_TRUE(v2.DecodeFrom(in)) << "fragmented=" << fragmented;

        EXPECT_EQ(v2.scalar_i32, -42LL);
        EXPECT_EQ(v2.scalar_u32, 0xFFFFFFFEULL);
        EXPECT_EQ(v2.scalar_s32,
                  static_cast<int64_t>(std::numeric_limits<int32_t>::min()));
        EXPECT_EQ(v2.scalar_bool, 1);
        EXPECT_EQ(v2.scalar_enum, 99LL);
        ASSERT_TRUE(v2.opt_i32.has_value());
        EXPECT_EQ(*v2.opt_i32, -7LL);

        EXPECT_EQ(
            v2.rep_i32,
            (std::vector<int64_t>{
                -100LL, 0LL, 42LL,
                static_cast<int64_t>(std::numeric_limits<int32_t>::min())}));
        EXPECT_EQ(v2.rep_u32,
                  (std::vector<uint64_t>{0ULL, 1ULL, 0xFFFFFFFFULL}));
        EXPECT_EQ(v2.rep_bool, (std::vector<int32_t>{1, 0, 1}));
        EXPECT_EQ(v2.rep_enum, (std::vector<int64_t>{0LL, 2LL, 77LL}));

        EXPECT_EQ(v2.single_to_rep_i32, (std::vector<int64_t>{-123LL}));
        ASSERT_TRUE(v2.rep_to_single_i32.has_value());
        EXPECT_EQ(*v2.rep_to_single_i32, -30LL);
        EXPECT_EQ(v2.single_to_rep_f32, (std::vector<float>{1.5f}));
        ASSERT_TRUE(v2.rep_to_single_f64.has_value());
        EXPECT_DOUBLE_EQ(*v2.rep_to_single_f64, 3.75);
        EXPECT_EQ(v2.single_to_rep_str, (std::vector<std::string>{"only-one"}));
        ASSERT_TRUE(v2.rep_to_single_str.has_value());
        EXPECT_EQ(*v2.rep_to_single_str, "last");
        ASSERT_EQ(v2.single_to_rep_msg.size(), 1u);
        EXPECT_EQ(v2.single_to_rep_msg[0], lm_single);

        ASSERT_TRUE(v2.rep_to_single_msg.has_value());
        EXPECT_EQ(v2.rep_to_single_msg->id, 500u);
        EXPECT_EQ(v2.rep_to_single_msg->label, "merged-label");
        EXPECT_EQ(v2.rep_to_single_msg->offsets,
                  (std::vector<uint64_t>{1, 2, 3, 4}));

        ASSERT_EQ(v2.map_to_rep_entries.size(), 2u);
        std::unordered_map<std::string, int64_t> rebuilt;
        for (const auto& entry : v2.map_to_rep_entries) {
            rebuilt[entry.key] = entry.value;
        }
        EXPECT_EQ(rebuilt, v1.map_to_rep_entries);

        ASSERT_EQ(v2.rep_entries_to_map.size(), 2u);
        EXPECT_EQ(v2.rep_entries_to_map.at(-5LL), lm_single);
        EXPECT_EQ(v2.rep_entries_to_map.at(10LL), lm_part2);

        ASSERT_EQ(v2.map_widen_kv.size(), 2u);
        EXPECT_EQ(v2.map_widen_kv.at(-1LL), -999LL);
        EXPECT_EQ(v2.map_widen_kv.at(42LL), 12345LL);

        ASSERT_EQ(v2.single_entry_to_map.size(), 1u);
        EXPECT_EQ(v2.single_entry_to_map.at("solo"), 888LL);

        ASSERT_TRUE(v2.has_eo_i32());
        EXPECT_EQ(v2.eo_i32(), -555LL);
    }

    // ---- Direction 2: CompatV2 -> CompatV1 ----
    CompatV2 v2_src;
    v2_src.scalar_i32 = -1024;
    v2_src.scalar_u32 = 99999;
    v2_src.scalar_s32 = -77;
    v2_src.scalar_bool = 5;  // non-zero -> true
    v2_src.scalar_enum = 2;  // REPLICA_COMPLETE
    v2_src.opt_i32 = 321;

    v2_src.rep_i32 = {-1, 2, -3};
    v2_src.rep_u32 = {10, 20};
    v2_src.rep_bool = {0, 3, 0, -1};
    v2_src.rep_enum = {1, 2};

    v2_src.single_to_rep_i32 = {11, 22, -33};  // last-one-wins -> -33
    v2_src.rep_to_single_i32 = -44;
    v2_src.single_to_rep_f32 = {0.5f, 2.25f};
    v2_src.rep_to_single_f64 = 9.5;
    v2_src.single_to_rep_str = {"a", "b"};
    v2_src.rep_to_single_str = "from-v2";
    v2_src.single_to_rep_msg = {lm_part1, lm_part2};  // merges into singular
    v2_src.rep_to_single_msg = lm_single;             // becomes 1-elem vector

    CompatMapEntryStrI64 e1;
    e1.key = "k1";
    e1.value = 11;
    CompatMapEntryStrI64 e2;
    e2.key = "k1";  // duplicate key -> last-one-wins
    e2.value = 22;
    CompatMapEntryStrI64 e3;
    e3.key = "";  // default key & default value
    e3.value = 0;
    v2_src.map_to_rep_entries = {e1, e2, e3};

    v2_src.rep_entries_to_map[7] = lm_single;
    v2_src.map_widen_kv[-9] = 456;
    v2_src.emplace_eo_i32(888);

    butil::IOBuf v2_buf;
    v2_src.EncodeTo(&v2_buf);

    for (bool fragmented : {false, true}) {
        butil::IOBuf in =
            fragmented ? make_chunked(v2_buf.to_string(), 7) : v2_buf;
        CompatV1 v1_dst;
        ASSERT_TRUE(v1_dst.DecodeFrom(in)) << "fragmented=" << fragmented;

        EXPECT_EQ(v1_dst.scalar_i32, -1024);
        EXPECT_EQ(v1_dst.scalar_u32, 99999u);
        EXPECT_EQ(v1_dst.scalar_s32, -77);
        EXPECT_TRUE(v1_dst.scalar_bool);
        EXPECT_EQ(v1_dst.scalar_enum, ReplicaStatus::REPLICA_COMPLETE);
        ASSERT_TRUE(v1_dst.opt_i32.has_value());
        EXPECT_EQ(*v1_dst.opt_i32, 321);

        EXPECT_EQ(v1_dst.rep_i32, (std::vector<int32_t>{-1, 2, -3}));
        EXPECT_EQ(v1_dst.rep_u32, (std::vector<uint32_t>{10u, 20u}));
        EXPECT_EQ(v1_dst.rep_bool,
                  (std::vector<bool>{false, true, false, true}));
        EXPECT_EQ(v1_dst.rep_enum, (std::vector<ReplicaStatus>{
                                       ReplicaStatus::REPLICA_PROCESSING,
                                       ReplicaStatus::REPLICA_COMPLETE}));

        EXPECT_EQ(v1_dst.single_to_rep_i32, -33);
        EXPECT_EQ(v1_dst.rep_to_single_i32, (std::vector<int32_t>{-44}));
        EXPECT_FLOAT_EQ(v1_dst.single_to_rep_f32, 2.25f);
        EXPECT_EQ(v1_dst.rep_to_single_f64, (std::vector<double>{9.5}));
        EXPECT_EQ(v1_dst.single_to_rep_str, "b");
        EXPECT_EQ(v1_dst.rep_to_single_str,
                  (std::vector<std::string>{"from-v2"}));
        ASSERT_TRUE(v1_dst.single_to_rep_msg.has_value());
        EXPECT_EQ(v1_dst.single_to_rep_msg->id, 500u);
        EXPECT_EQ(v1_dst.single_to_rep_msg->label, "merged-label");
        EXPECT_EQ(v1_dst.single_to_rep_msg->offsets,
                  (std::vector<uint64_t>{1, 2, 3, 4}));
        ASSERT_EQ(v1_dst.rep_to_single_msg.size(), 1u);
        EXPECT_EQ(v1_dst.rep_to_single_msg[0], lm_single);

        ASSERT_EQ(v1_dst.map_to_rep_entries.size(), 2u);
        EXPECT_EQ(v1_dst.map_to_rep_entries.at("k1"), 22);
        EXPECT_EQ(v1_dst.map_to_rep_entries.at(""), 0);

        ASSERT_EQ(v1_dst.rep_entries_to_map.size(), 1u);
        EXPECT_EQ(v1_dst.rep_entries_to_map[0].key, 7);
        ASSERT_TRUE(v1_dst.rep_entries_to_map[0].value.has_value());
        EXPECT_EQ(*v1_dst.rep_entries_to_map[0].value, lm_single);

        ASSERT_EQ(v1_dst.map_widen_kv.size(), 1u);
        EXPECT_EQ(v1_dst.map_widen_kv.at(-9), 456);

        ASSERT_TRUE(v1_dst.has_eo_i32());
        EXPECT_EQ(v1_dst.eo_i32(), 888);
    }
}

TEST(BrpcFastCodecTest, ConcatenatedMessageMergeSemantics) {
    auto make_chunked = [](const std::string& flat, size_t chunk) {
        butil::IOBuf out;
        for (size_t off = 0; off < flat.size(); off += chunk) {
            size_t n = std::min(chunk, flat.size() - off);
            char* seg = new char[n];
            std::memcpy(seg, flat.data() + off, n);
            out.append_user_data(
                seg, n, [](void* p) { delete[] static_cast<char*>(p); });
        }
        return out;
    };

    // Part A sets sub.a, sub.nums={1,2}, leaf_branch.a, tags={"t1"}, map["k1"]
    MergeContainer part_a;
    part_a.seq = 1;
    part_a.sub.emplace();
    part_a.sub->a = 111;
    part_a.sub->nums = {1, 2};
    part_a.tags = {"t1"};
    MergeLeaf mk1;
    mk1.a = 10;
    mk1.b = "old";
    part_a.leaf_map["k1"] = mk1;
    part_a.leaf_map["keep"] = mk1;
    auto& oa = part_a.mutable_leaf_branch();
    oa.a = 777;
    oa.nums = {10};

    // Part B sets sub.b, sub.nums={3}, leaf_branch.b, tags={"t2"}, map["k1"]
    MergeContainer part_b;
    part_b.seq = 2;
    part_b.sub.emplace();
    part_b.sub->b = "from-b";
    part_b.sub->nums = {3};
    part_b.tags = {"t2"};
    MergeLeaf mk1_new;
    mk1_new.b = "replaced";  // Map duplicate key replaces whole value (PB spec)
    part_b.leaf_map["k1"] = mk1_new;
    auto& ob = part_b.mutable_leaf_branch();
    ob.b = "oneof-merged";
    ob.nums = {20};

    butil::IOBuf buf_a;
    part_a.EncodeTo(&buf_a);
    butil::IOBuf buf_b;
    part_b.EncodeTo(&buf_b);

    std::string concat_flat = buf_a.to_string() + buf_b.to_string();
    butil::IOBuf concat_contiguous;
    concat_contiguous.append(concat_flat);

    for (bool fragmented : {false, true}) {
        butil::IOBuf in =
            fragmented ? make_chunked(concat_flat, 5) : concat_contiguous;

        // Test on a pre-populated reused object to ensure stale fields from
        // previous RPCs are cleared while fields within this message merge.
        MergeContainer dst;
        dst.seq = 999;
        dst.sub.emplace();
        dst.sub->a = 8888;
        dst.sub->b = "stale-sub";
        dst.sub->nums = {99};
        dst.tags = {"stale-tag"};
        dst.leaf_map["stale-key"] = mk1;
        dst.emplace_str_branch("stale-oneof");

        ASSERT_TRUE(dst.DecodeFrom(in)) << "fragmented=" << fragmented;
        EXPECT_EQ(dst.seq, 2);
        ASSERT_TRUE(dst.sub.has_value());
        EXPECT_EQ(dst.sub->a, 111);
        EXPECT_EQ(dst.sub->b, "from-b");
        EXPECT_EQ(dst.sub->nums, (std::vector<uint64_t>{1, 2, 3}));
        EXPECT_EQ(dst.tags, (std::vector<std::string>{"t1", "t2"}));

        EXPECT_EQ(dst.leaf_map.count("stale-key"), 0u);
        ASSERT_EQ(dst.leaf_map.size(), 2u);
        EXPECT_EQ(dst.leaf_map.at("keep"), mk1);
        // Map duplicate key replaces previous value (a == 0, b == "replaced").
        EXPECT_EQ(dst.leaf_map.at("k1"), mk1_new);

        ASSERT_TRUE(dst.has_leaf_branch());
        EXPECT_EQ(dst.leaf_branch().a, 777);
        EXPECT_EQ(dst.leaf_branch().b, "oneof-merged");
        EXPECT_EQ(dst.leaf_branch().nums, (std::vector<uint64_t>{10, 20}));
    }

    // Switching oneof branch away and back must NOT merge across the switch.
    MergeContainer mid_switch;
    mid_switch.emplace_str_branch("interrupt");
    butil::IOBuf buf_mid;
    mid_switch.EncodeTo(&buf_mid);

    butil::IOBuf switched;
    switched.append(buf_a.to_string() + buf_mid.to_string() +
                    buf_b.to_string());
    MergeContainer dst_sw;
    ASSERT_TRUE(dst_sw.DecodeFrom(switched));
    ASSERT_TRUE(dst_sw.has_leaf_branch());
    EXPECT_EQ(dst_sw.leaf_branch().a, 0);
    EXPECT_EQ(dst_sw.leaf_branch().b, "oneof-merged");
    EXPECT_EQ(dst_sw.leaf_branch().nums, (std::vector<uint64_t>{20}));
}

TEST(BrpcFastCodecTest, NegativeZeroAndMalformedWireRejection) {
    // 1. Proto3 must preserve -0.0f and -0.0 sign bit, while omitting +0.0.
    AllTypes pos_zero;
    pos_zero.f_float = 0.0f;
    pos_zero.f_double = 0.0;
    butil::IOBuf pos_buf;
    pos_zero.EncodeTo(&pos_buf);
    EXPECT_EQ(pos_buf.size(), 0u);

    AllTypes neg_zero;
    neg_zero.f_float = -0.0f;
    neg_zero.f_double = -0.0;
    butil::IOBuf neg_buf;
    neg_zero.EncodeTo(&neg_buf);
    EXPECT_GT(neg_buf.size(), 0u);

    AllTypes decoded;
    ASSERT_TRUE(decoded.DecodeFrom(neg_buf));
    EXPECT_EQ(decoded.f_float, 0.0f);
    EXPECT_TRUE(std::signbit(decoded.f_float));
    EXPECT_EQ(decoded.f_double, 0.0);
    EXPECT_TRUE(std::signbit(decoded.f_double));

    // 2. Tag with field_number == 0 must be rejected.
    for (uint8_t bad_tag : {uint8_t{0x00}, uint8_t{0x01}, uint8_t{0x03}}) {
        butil::IOBuf bad;
        const uint8_t bytes[5] = {bad_tag, 0, 0, 0, 0};
        bad.append(bytes, sizeof(bytes));
        AllTypes dst;
        EXPECT_FALSE(dst.DecodeFrom(bad)) << "bad_tag=" << int(bad_tag);
    }

    // 3. Overflowing 5-byte varint32 (5th byte >= 0x10) must be rejected.
    {
        butil::IOBuf bad;
        const uint8_t bytes[6] = {0x80, 0x80, 0x80, 0x80, 0x10, 0x00};
        bad.append(bytes, sizeof(bytes));
        AllTypes dst;
        EXPECT_FALSE(dst.DecodeFrom(bad));
    }
}

TEST(BrpcFastCodecTest, RecursiveAndWellKnownTypesRoundTrip) {
    // Self-recursive linked list + cyclic oneof branch
    RecursiveNode root;
    root.value = 1;
    root.next.emplace();
    root.next->value = 2;
    root.next->next.emplace();
    root.next->next->value = 3;
    auto& alt = root.mutable_alt_node();
    alt.value = 99;
    alt.emplace_note("leaf-note");

    butil::IOBuf buf;
    root.EncodeTo(&buf);
    RecursiveNode dst;
    ASSERT_TRUE(dst.DecodeFrom(buf));
    EXPECT_EQ(root, dst);

    // Value-copy semantics on IndirectOptional / IndirectBox
    RecursiveNode copy = dst;
    EXPECT_EQ(copy, dst);
    copy.next->value = 222;
    EXPECT_EQ(dst.next->value, 2);

    // Mutual recursion: MutualA <-> MutualB
    MutualA ma;
    ma.id = 10;
    ma.b.emplace();
    ma.b->name = "level-1";
    ma.b->a.emplace();
    ma.b->a->id = 20;
    ma.b->a->b.emplace();
    ma.b->a->b->name = "level-2";

    butil::IOBuf mbuf;
    ma.EncodeTo(&mbuf);
    MutualA ma_dst;
    ASSERT_TRUE(ma_dst.DecodeFrom(mbuf));
    EXPECT_EQ(ma, ma_dst);

    // Google Well-Known Types (Timestamp, Empty, and mutually-recursive Struct)
    WithWellKnownTypes wkt;
    wkt.event_id = "evt-42";
    wkt.created_at.emplace();
    wkt.created_at->seconds = 1795408294LL;
    wkt.created_at->nanos = 123456789;
    wkt.marker.emplace();
    wkt.attributes.emplace();
    wkt.attributes->fields["env"].emplace_string_value("prod");
    wkt.attributes->fields["replicas"].emplace_number_value(3.0);
    auto& nested_struct =
        wkt.attributes->fields["nested"].mutable_struct_value();
    nested_struct.fields["ok"].emplace_bool_value(true);

    butil::IOBuf wbuf;
    ASSERT_TRUE(wkt.EncodeTo(&wbuf));
    WithWellKnownTypes wkt_dst;
    ASSERT_TRUE(wkt_dst.DecodeFrom(wbuf));
    EXPECT_EQ(wkt, wkt_dst);
    ASSERT_TRUE(wkt_dst.created_at.has_value());
    EXPECT_EQ(wkt_dst.created_at->seconds, 1795408294LL);
    EXPECT_EQ(wkt_dst.created_at->nanos, 123456789);
    EXPECT_TRUE(wkt_dst.marker.has_value());
    ASSERT_TRUE(wkt_dst.attributes.has_value());
    EXPECT_EQ(wkt_dst.attributes->fields.at("env").string_value(), "prod");
    EXPECT_TRUE(wkt_dst.attributes->fields.at("nested")
                    .struct_value()
                    .fields.at("ok")
                    .bool_value());
}

TEST(BrpcFastCodecTest, MutualOneofNestedCycleKeywordAndManyFields) {
    // 1. Mutually recursive oneof + map value (IndirectBox)
    MutualOneofA moa;
    moa.id = 1;
    auto& b_br = moa.mutable_b_branch();
    b_br.name = "branch-b";
    b_br.a_ref.emplace();
    b_br.a_ref->id = 2;
    b_br.a_ref->emplace_label("leaf-a");
    MutualOneofB mb_map;
    mb_map.name = "mapped-b";
    moa.b_map[7] = mb_map;

    butil::IOBuf mobuf;
    ASSERT_TRUE(moa.EncodeTo(&mobuf));
    MutualOneofA moa_dst;
    ASSERT_TRUE(moa_dst.DecodeFrom(mobuf));
    EXPECT_EQ(moa, moa_dst);

    // 2. Mutually recursive nested messages (Left <-> Right)
    NestedCycleOuter nc;
    nc.root.emplace();
    nc.root->val = 10;
    nc.root->right.emplace();
    nc.root->right->tag = "mid";
    nc.root->right->left.emplace();
    nc.root->right->left->val = 20;

    butil::IOBuf ncbuf;
    ASSERT_TRUE(nc.EncodeTo(&ncbuf));
    NestedCycleOuter nc_dst;
    ASSERT_TRUE(nc_dst.DecodeFrom(ncbuf));
    EXPECT_EQ(nc, nc_dst);

    // Fragmented multi-block decode forces DecodeLoop on
    // NestedCycleOuter::Right (catching any local `tag` variable shadowing of
    // `Right.tag`).
    {
        std::string flat = ncbuf.to_string();
        butil::IOBuf frag;
        for (size_t off = 0; off < flat.size(); off += 2) {
            size_t n = std::min<size_t>(2, flat.size() - off);
            char* seg = new char[n];
            std::memcpy(seg, flat.data() + off, n);
            frag.append_user_data(
                seg, n, [](void* p) { delete[] static_cast<char*>(p); });
        }
        NestedCycleOuter nc_frag;
        ASSERT_TRUE(nc_frag.DecodeFrom(frag));
        EXPECT_EQ(nc_frag, nc);
        ASSERT_TRUE(nc_frag.root.has_value());
        ASSERT_TRUE(nc_frag.root->right.has_value());
        EXPECT_EQ(nc_frag.root->right->tag, "mid");
    }

    // 3. C++ keyword as message & field name
    UsesKeywordType kw;
    kw.item.emplace();
    kw.item->class_ = 42;
    butil::IOBuf kwbuf;
    ASSERT_TRUE(kw.EncodeTo(&kwbuf));
    UsesKeywordType kw_dst;
    ASSERT_TRUE(kw_dst.DecodeFrom(kwbuf));
    EXPECT_EQ(kw, kw_dst);
    EXPECT_EQ(kw_dst.item->class_, 42);

    // 4. > 64 regular fields (contiguous + fragmented overwrite)
    ManyFieldsMsg many;
    many.f01 = 1;
    many.f32 = 32;
    many.f64 = 64;
    many.f65 = 65;
    many.f66 = 66;
    many.f67 = "sixty-seven";
    many.f68 = 68000LL;

    butil::IOBuf many_buf;
    ASSERT_TRUE(many.EncodeTo(&many_buf));
    std::string flat = many_buf.to_string();
    butil::IOBuf frag;
    for (size_t off = 0; off < flat.size(); off += 3) {
        size_t n = std::min<size_t>(3, flat.size() - off);
        char* seg = new char[n];
        std::memcpy(seg, flat.data() + off, n);
        frag.append_user_data(seg, n,
                              [](void* p) { delete[] static_cast<char*>(p); });
    }

    ManyFieldsMsg dst_contig;
    dst_contig.f02 = 999;
    dst_contig.f66 = 888;
    ASSERT_TRUE(dst_contig.DecodeFrom(many_buf));
    EXPECT_EQ(dst_contig, many);

    ManyFieldsMsg dst_frag;
    dst_frag.f02 = 999;
    dst_frag.f66 = 888;
    ASSERT_TRUE(dst_frag.DecodeFrom(frag));
    EXPECT_EQ(dst_frag, many);

    // 5. IndirectOptional::value() throws on empty; moved-from IndirectBox ==
    IndirectOptional<RecursiveNode> empty_opt;
    EXPECT_THROW((void)empty_opt.value(), std::bad_optional_access);
    IndirectBox<MutualOneofB> box1;
    box1->name = "x";
    IndirectBox<MutualOneofB> box2 = std::move(box1);
    EXPECT_FALSE(box1 == box2);
    EXPECT_TRUE(box1 == box1);
}

TEST(BrpcFastCodecTest, UnknownFieldsDefaultOffAndOptInPreserve) {
    EvolutionV2 v2;
    v2.id = 777;
    v2.values = {10, 20, 30};
    v2.extra_tag = "preserve-me";
    v2.extra_ids = {100, 200, 300};
    v2.extra_meta["region"] = "ap-east-1";
    v2.extra_meta["tier"] = "hot";
    v2.large_tag_field = -987654321LL;

    butil::IOBuf v2_buf;
    v2.EncodeTo(&v2_buf);

    // Default generated EvolutionV1 drops unknown fields (zero struct
    // overhead).
    {
        EvolutionV1 v1_default;
        ASSERT_TRUE(v1_default.DecodeFrom(v2_buf));
        EXPECT_EQ(v1_default.id, 777u);
        EXPECT_EQ(v1_default.values, (std::vector<uint64_t>{10, 20, 30}));

        butil::IOBuf reencoded;
        v1_default.EncodeTo(&reencoded);
        EvolutionV2 v2_after;
        ASSERT_TRUE(v2_after.DecodeFrom(reencoded));
        EXPECT_EQ(v2_after.id, 777u);
        EXPECT_EQ(v2_after.values, (std::vector<uint64_t>{10, 20, 30}));
        EXPECT_FALSE(v2_after.extra_tag.has_value());
        EXPECT_TRUE(v2_after.extra_ids.empty());
        EXPECT_TRUE(v2_after.extra_meta.empty());
        EXPECT_EQ(v2_after.large_tag_field, 0);
    }

    // Opt-in PRESERVE_UNKNOWN_FIELDS retains unknown fields across round-trip.
    {
        preserve::EvolutionV1Preserve v1_preserve;
        ASSERT_TRUE(v1_preserve.DecodeFrom(v2_buf));
        EXPECT_EQ(v1_preserve.id, 777u);
        EXPECT_EQ(v1_preserve.values, (std::vector<uint64_t>{10, 20, 30}));
        EXPECT_FALSE(v1_preserve._unknown_fields.Empty());

        butil::IOBuf reencoded;
        v1_preserve.EncodeTo(&reencoded);
        EvolutionV2 v2_roundtrip;
        ASSERT_TRUE(v2_roundtrip.DecodeFrom(reencoded));
        EXPECT_EQ(v2_roundtrip, v2);
    }
}

TEST(BrpcFastCodecTest, Proto3OneofOptionalStringBytesFixedAndNarrowingCompat) {
    auto make_chunked = [](const std::string& flat, size_t chunk) {
        butil::IOBuf out;
        for (size_t off = 0; off < flat.size(); off += chunk) {
            size_t n = std::min(chunk, flat.size() - off);
            char* seg = new char[n];
            std::memcpy(seg, flat.data() + off, n);
            out.append_user_data(
                seg, n, [](void* p) { delete[] static_cast<char*>(p); });
        }
        return out;
    };

    LaterMessage lm;
    lm.id = 404;
    lm.label = "moved-msg";
    lm.offsets = {12, 24};

    // ---- Direction 1: Proto3CompatExtV1 -> Proto3CompatExtV2 ----
    Proto3CompatExtV1 v1;
    v1.plain_to_oneof_i64 = -123456789LL;
    v1.opt_to_oneof_str =
        "";  // Explicit empty optional must stay set in oneof.
    v1.msg_to_oneof = lm;
    v1.emplace_otp_i64(987654321LL);
    v1.emplace_oto_str("");  // Explicit empty oneof must stay set in optional.

    v1.str_to_bytes = "utf8-payload";
    v1.bytes_to_str = std::string("\x00\x01\xFE\xFF", 4);
    v1.rep_str_to_bytes = {"first", "", std::string("a\0b", 3)};

    // fixed32 <-> sfixed32 and fixed64 <-> sfixed64 preserve raw 32/64-bit
    // patterns (signed/unsigned reinterpret per Proto3 spec).
    v1.f32_to_sf32 = 0xFFFFFFF6u;  // -10 as int32_t
    v1.sf64_to_f64 = -25LL;
    // Narrowing int64/uint64 -> int32/uint32 truncates to low 32 bits.
    v1.narrow_i64 = (int64_t{0x1234} << 32) | int64_t{0x0000002A};
    v1.narrow_u64 = (uint64_t{0xABCD} << 32) | uint64_t{0xDEADBEEFu};

    // Incompatible wire types (kBytes on fields that are int32/int64 in V2)
    // must be skipped cleanly without failing trailing fields.
    v1.incompat_str_to_i32 = "not-an-int32";
    v1.incompat_msg_to_i64 = lm;
    v1.trailing_sentinel = 777777LL;

    butil::IOBuf v1_buf;
    v1.EncodeTo(&v1_buf);

    for (bool fragmented : {false, true}) {
        butil::IOBuf in =
            fragmented ? make_chunked(v1_buf.to_string(), 5) : v1_buf;
        Proto3CompatExtV2 v2;
        // Pre-populate stale values on incompatible fields to verify reset.
        v2.incompat_str_to_i32 = 999;
        v2.incompat_msg_to_i64 = 888;
        ASSERT_TRUE(v2.DecodeFrom(in)) << "fragmented=" << fragmented;

        ASSERT_TRUE(v2.has_plain_to_oneof_i64());
        EXPECT_EQ(v2.plain_to_oneof_i64(), -123456789LL);
        ASSERT_TRUE(v2.has_opt_to_oneof_str());
        EXPECT_EQ(v2.opt_to_oneof_str(), "");
        ASSERT_TRUE(v2.has_msg_to_oneof());
        EXPECT_EQ(v2.msg_to_oneof(), lm);
        EXPECT_EQ(v2.otp_i64, 987654321LL);
        ASSERT_TRUE(v2.oto_str.has_value());
        EXPECT_EQ(*v2.oto_str, "");

        EXPECT_EQ(v2.str_to_bytes, "utf8-payload");
        ASSERT_TRUE(v2.bytes_to_str.has_value());
        EXPECT_EQ(*v2.bytes_to_str, std::string("\x00\x01\xFE\xFF", 4));
        EXPECT_EQ(
            v2.rep_str_to_bytes,
            (std::vector<std::string>{"first", "", std::string("a\0b", 3)}));

        EXPECT_EQ(v2.f32_to_sf32, -10);
        EXPECT_EQ(v2.sf64_to_f64, static_cast<uint64_t>(-25LL));
        EXPECT_EQ(v2.narrow_i64, 42);
        EXPECT_EQ(v2.narrow_u64, 0xDEADBEEFu);

        EXPECT_EQ(v2.incompat_str_to_i32, 0);
        EXPECT_EQ(v2.incompat_msg_to_i64, 0);
        EXPECT_EQ(v2.trailing_sentinel, 777777LL);
    }

    // ---- Direction 2: Proto3CompatExtV2 -> Proto3CompatExtV1 ----
    Proto3CompatExtV2 v2_src;
    v2_src.emplace_plain_to_oneof_i64(54321LL);
    v2_src.emplace_opt_to_oneof_str("from-oneof");
    v2_src.emplace_msg_to_oneof(lm);
    v2_src.otp_i64 = -11111LL;
    v2_src.oto_str = "from-opt";
    v2_src.str_to_bytes = std::string("\x00bytes\xFF", 7);
    v2_src.bytes_to_str = "plain-str";
    v2_src.rep_str_to_bytes = {"x", "y"};
    v2_src.f32_to_sf32 = -1;
    v2_src.sf64_to_f64 = 0xFFFFFFFFFFFFFFFFULL;
    v2_src.narrow_i64 = -77;
    v2_src.narrow_u64 = 0x80000001u;
    v2_src.incompat_str_to_i32 = 12345;
    v2_src.incompat_msg_to_i64 = -67890LL;
    v2_src.trailing_sentinel = -314159LL;

    butil::IOBuf v2_buf;
    v2_src.EncodeTo(&v2_buf);

    for (bool fragmented : {false, true}) {
        butil::IOBuf in =
            fragmented ? make_chunked(v2_buf.to_string(), 7) : v2_buf;
        Proto3CompatExtV1 v1_dst;
        v1_dst.incompat_str_to_i32 = "stale-str";
        v1_dst.incompat_msg_to_i64 = lm;
        ASSERT_TRUE(v1_dst.DecodeFrom(in)) << "fragmented=" << fragmented;

        EXPECT_EQ(v1_dst.plain_to_oneof_i64, 54321LL);
        ASSERT_TRUE(v1_dst.opt_to_oneof_str.has_value());
        EXPECT_EQ(*v1_dst.opt_to_oneof_str, "from-oneof");
        ASSERT_TRUE(v1_dst.msg_to_oneof.has_value());
        EXPECT_EQ(*v1_dst.msg_to_oneof, lm);
        ASSERT_TRUE(v1_dst.has_otp_i64());
        EXPECT_EQ(v1_dst.otp_i64(), -11111LL);
        ASSERT_TRUE(v1_dst.has_oto_str());
        EXPECT_EQ(v1_dst.oto_str(), "from-opt");

        EXPECT_EQ(v1_dst.str_to_bytes, std::string("\x00bytes\xFF", 7));
        ASSERT_TRUE(v1_dst.bytes_to_str.has_value());
        EXPECT_EQ(*v1_dst.bytes_to_str, "plain-str");
        EXPECT_EQ(v1_dst.rep_str_to_bytes,
                  (std::vector<std::string>{"x", "y"}));

        EXPECT_EQ(v1_dst.f32_to_sf32, 0xFFFFFFFFu);
        EXPECT_EQ(v1_dst.sf64_to_f64, -1LL);
        EXPECT_EQ(v1_dst.narrow_i64, -77LL);
        EXPECT_EQ(v1_dst.narrow_u64, 0x80000001ULL);

        // Incompatible int32/int64 on string/message fields must be skipped
        // and stale fields cleared.
        EXPECT_TRUE(v1_dst.incompat_str_to_i32.empty());
        EXPECT_FALSE(v1_dst.incompat_msg_to_i64.has_value());
        EXPECT_EQ(v1_dst.trailing_sentinel, -314159LL);
    }
}

TEST(BrpcFastCodecTest, Proto3IncompatibleWireTypePreserveRoundTrip) {
    auto make_chunked = [](const std::string& flat, size_t chunk) {
        butil::IOBuf out;
        for (size_t off = 0; off < flat.size(); off += chunk) {
            size_t n = std::min(chunk, flat.size() - off);
            char* seg = new char[n];
            std::memcpy(seg, flat.data() + off, n);
            out.append_user_data(
                seg, n, [](void* p) { delete[] static_cast<char*>(p); });
        }
        return out;
    };

    Proto3CompatExtV1 src;
    src.incompat_str_to_i32 = "preserve-incompatible-string";
    LaterMessage lm;
    lm.id = 808;
    lm.label = "preserve-incompatible-msg";
    lm.offsets = {1, 2, 3};
    src.incompat_msg_to_i64 = lm;
    src.trailing_sentinel = 424242LL;

    butil::IOBuf buf;
    src.EncodeTo(&buf);

    for (bool fragmented : {false, true}) {
        butil::IOBuf in = fragmented ? make_chunked(buf.to_string(), 5) : buf;
        preserve::IncompatiblePreserve mid;
        ASSERT_TRUE(mid.DecodeFrom(in)) << "fragmented=" << fragmented;
        EXPECT_EQ(mid.incompat_str_to_i32, 0);
        EXPECT_EQ(mid.incompat_msg_to_i64, 0);
        EXPECT_EQ(mid.trailing_sentinel, 424242LL);
        EXPECT_FALSE(mid._unknown_fields.Empty());

        butil::IOBuf reencoded;
        mid.EncodeTo(&reencoded);
        Proto3CompatExtV1 roundtrip;
        ASSERT_TRUE(roundtrip.DecodeFrom(reencoded))
            << "fragmented=" << fragmented;
        EXPECT_EQ(roundtrip, src) << "fragmented=" << fragmented;
    }
}

TEST(BrpcFastCodecTest, Proto3NestedUnknownFieldsPreserveRoundTrip) {
    auto make_chunked = [](const std::string& flat, size_t chunk) {
        butil::IOBuf out;
        for (size_t off = 0; off < flat.size(); off += chunk) {
            size_t n = std::min(chunk, flat.size() - off);
            char* seg = new char[n];
            std::memcpy(seg, flat.data() + off, n);
            out.append_user_data(
                seg, n, [](void* p) { delete[] static_cast<char*>(p); });
        }
        return out;
    };

    auto make_v2 = [](uint64_t id, const std::string& tag, int64_t big) {
        EvolutionV2 v2;
        v2.id = id;
        v2.values = {id, id + 1};
        v2.extra_tag = tag;
        v2.extra_ids = {id * 10, id * 20};
        v2.extra_meta["k"] = tag;
        v2.large_tag_field = big;
        return v2;
    };

    NestedEvolutionV2 src;
    src.child = make_v2(1, "child-unknown", -101);
    src.items = {make_v2(2, "item-0-unknown", -202),
                 make_v2(3, "item-1-unknown", -303)};

    butil::IOBuf buf;
    src.EncodeTo(&buf);

    for (bool fragmented : {false, true}) {
        butil::IOBuf in = fragmented ? make_chunked(buf.to_string(), 7) : buf;
        preserve::NestedPreserveV1 mid;
        ASSERT_TRUE(mid.DecodeFrom(in)) << "fragmented=" << fragmented;
        ASSERT_TRUE(mid.child.has_value());
        EXPECT_EQ(mid.child->id, 1u);
        EXPECT_FALSE(mid.child->_unknown_fields.Empty());
        ASSERT_EQ(mid.items.size(), 2u);
        EXPECT_FALSE(mid.items[0]._unknown_fields.Empty());
        EXPECT_FALSE(mid.items[1]._unknown_fields.Empty());

        butil::IOBuf reencoded;
        mid.EncodeTo(&reencoded);
        NestedEvolutionV2 roundtrip;
        ASSERT_TRUE(roundtrip.DecodeFrom(reencoded))
            << "fragmented=" << fragmented;
        EXPECT_EQ(roundtrip, src) << "fragmented=" << fragmented;
    }
}

TEST(BrpcFastCodecTest, Proto3RecursionDepthLimitRejected) {
    auto make_chain = [](int depth) {
        RecursiveNode root;
        root.value = 0;
        RecursiveNode* cur = &root;
        for (int i = 1; i <= depth; ++i) {
            cur->next.emplace();
            cur = &(*cur->next);
            cur->value = i;
        }
        return root;
    };
    auto make_chunked = [](const std::string& flat, size_t chunk) {
        butil::IOBuf out;
        for (size_t off = 0; off < flat.size(); off += chunk) {
            size_t n = std::min(chunk, flat.size() - off);
            char* seg = new char[n];
            std::memcpy(seg, flat.data() + off, n);
            out.append_user_data(
                seg, n, [](void* p) { delete[] static_cast<char*>(p); });
        }
        return out;
    };

    // Depth well within the recursion limit must round-trip cleanly.
    {
        RecursiveNode ok_chain = make_chain(32);
        butil::IOBuf buf;
        ok_chain.EncodeTo(&buf);
        for (bool fragmented : {false, true}) {
            butil::IOBuf in =
                fragmented ? make_chunked(buf.to_string(), 5) : buf;
            RecursiveNode dst;
            ASSERT_TRUE(dst.DecodeFrom(in)) << "fragmented=" << fragmented;
            EXPECT_EQ(dst, ok_chain) << "fragmented=" << fragmented;
        }
    }

    // Depth exceeding the recursion limit (> 64) must fail cleanly without
    // stack overflow and leave the destination in a clean default state.
    {
        RecursiveNode deep_chain = make_chain(70);
        butil::IOBuf buf;
        deep_chain.EncodeTo(&buf);
        for (bool fragmented : {false, true}) {
            butil::IOBuf in =
                fragmented ? make_chunked(buf.to_string(), 5) : buf;
            RecursiveNode dst;
            dst.value = 12345;
            EXPECT_FALSE(dst.DecodeFrom(in)) << "fragmented=" << fragmented;
            EXPECT_EQ(dst, RecursiveNode{}) << "fragmented=" << fragmented;
        }
    }
}

TEST(BrpcFastCodecTest, ShadowingFieldNamesWktCoexistenceAndMovedIndirectBox) {
    auto make_chunked = [](const std::string& flat, size_t chunk) {
        butil::IOBuf out;
        for (size_t off = 0; off < flat.size(); off += chunk) {
            size_t n = std::min(chunk, flat.size() - off);
            char* seg = new char[n];
            std::memcpy(seg, flat.data() + off, n);
            out.append_user_data(
                seg, n, [](void* p) { delete[] static_cast<char*>(p); });
        }
        return out;
    };

    // 1. Official libprotobuf WKT and generated brpc_fast_pb WKT coexist in the
    // same translation unit without redefinition or ODR conflicts.
    ::google::protobuf::Timestamp official_ts;
    official_ts.set_seconds(1700000000LL);
    official_ts.set_nanos(42);
    ::brpc_fast_pb::google::protobuf::Timestamp fast_ts;
    fast_ts.seconds = official_ts.seconds();
    fast_ts.nanos = official_ts.nanos();
    butil::IOBuf ts_buf;
    ASSERT_TRUE(fast_ts.EncodeTo(&ts_buf));
    ::brpc_fast_pb::google::protobuf::Timestamp fast_ts_dst;
    ASSERT_TRUE(fast_ts_dst.DecodeFrom(ts_buf));
    EXPECT_EQ(fast_ts_dst, fast_ts);

    // 2. Proto fields/oneofs sharing names with codegen locals & parameters
    // must encode and decode identically on both contiguous and multi-block
    // paths.
    ShadowingFieldsMsg s;
    s.tag = "field-named-tag";
    s.sz = 9876543210ULL;
    s.tape = "field-named-tape";
    s.dst = "field-named-dst";
    s.cur = 11;
    s.end = 22;
    s.remaining_depth = 33;
    s.r = 44;
    s.wt = 55;
    s.seen_fields = 66;
    s.field_number = 77;
    s.emplace_arg_text("oneof-named-args");

    butil::IOBuf sbuf;
    ASSERT_TRUE(s.EncodeTo(&sbuf));
    for (bool fragmented : {false, true}) {
        butil::IOBuf in = fragmented ? make_chunked(sbuf.to_string(), 3) : sbuf;
        ShadowingFieldsMsg dst;
        dst.tag = "stale";
        dst.sz = 1;
        ASSERT_TRUE(dst.DecodeFrom(in)) << "fragmented=" << fragmented;
        EXPECT_EQ(dst, s) << "fragmented=" << fragmented;
    }

    // 3. Reusing a moved-from message whose variant still selects an
    // IndirectBox branch must not dereference a null pointer on decode,
    // encode, or mutable access.
    RecursiveNode src;
    src.value = 1;
    src.mutable_alt_node().value = 2;

    butil::IOBuf rbuf;
    ASSERT_TRUE(src.EncodeTo(&rbuf));

    RecursiveNode moved_to = std::move(src);
    EXPECT_EQ(moved_to.alt_node().value, 2);

    // Const access and encode on the moved-from source must stay safe.
    EXPECT_EQ(src.alt_node().value, 0);
    butil::IOBuf moved_src_buf;
    ASSERT_TRUE(src.EncodeTo(&moved_src_buf));

    // Reusing the moved-from source for both contiguous and fragmented decode
    // into the same IndirectBox oneof branch must restore a valid box.
    for (bool fragmented : {false, true}) {
        RecursiveNode moved_from;
        moved_from.value = 99;
        moved_from.mutable_alt_node().value = 100;
        RecursiveNode sink = std::move(moved_from);
        (void)sink;

        butil::IOBuf in = fragmented ? make_chunked(rbuf.to_string(), 2) : rbuf;
        ASSERT_TRUE(moved_from.DecodeFrom(in)) << "fragmented=" << fragmented;
        EXPECT_EQ(moved_from, moved_to) << "fragmented=" << fragmented;
        EXPECT_EQ(moved_from.alt_node().value, 2);
    }

    // mutable_alt_node() on a moved-from source must also reallocate cleanly.
    {
        RecursiveNode moved_from;
        moved_from.mutable_alt_node().value = 7;
        RecursiveNode sink = std::move(moved_from);
        (void)sink;
        moved_from.mutable_alt_node().value = 88;
        EXPECT_EQ(moved_from.alt_node().value, 88);
    }

    // 4. Forward reference to a nested enum declared in a later message
    // (singular field and map value), plus cyclic message <-> nested-enum and
    // mutual nested-enum <-> nested-enum references.
    {
        ForwardNestedEnumUser eu;
        eu.kind = ForwardNestedEnumOwner::Kind::KIND_ALPHA;
        eu.kind_map["b"] = ForwardNestedEnumOwner::Kind::KIND_BETA;

        butil::IOBuf eubuf;
        ASSERT_TRUE(eu.EncodeTo(&eubuf));
        ForwardNestedEnumUser eu_dst;
        ASSERT_TRUE(eu_dst.DecodeFrom(eubuf));
        EXPECT_EQ(eu_dst, eu);

        CycleEnumOwner owner;
        owner.user.emplace();
        owner.user->kind = CycleEnumOwner::Kind::CYCLE_ACTIVE;
        owner.user->label = "cyclic-enum";
        butil::IOBuf owner_buf;
        ASSERT_TRUE(owner.EncodeTo(&owner_buf));
        CycleEnumOwner owner_dst;
        ASSERT_TRUE(owner_dst.DecodeFrom(owner_buf));
        EXPECT_EQ(owner_dst, owner);

        MutualEnumA mea;
        mea.b_kind = MutualEnumB::Kind::B_TWO;
        butil::IOBuf mea_buf;
        ASSERT_TRUE(mea.EncodeTo(&mea_buf));
        MutualEnumA mea_dst;
        ASSERT_TRUE(mea_dst.DecodeFrom(mea_buf));
        EXPECT_EQ(mea_dst, mea);

        MutualEnumB meb;
        meb.a_kind = MutualEnumA::Kind::A_ONE;
        butil::IOBuf meb_buf;
        ASSERT_TRUE(meb.EncodeTo(&meb_buf));
        MutualEnumB meb_dst;
        ASSERT_TRUE(meb_dst.DecodeFrom(meb_buf));
        EXPECT_EQ(meb_dst, meb);
    }
}

}  // namespace
}  // namespace brpc_fast::test
