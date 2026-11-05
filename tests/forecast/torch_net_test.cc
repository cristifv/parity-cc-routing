// The forecaster network (PLAN §5.9): shapes, quantile order, < 1 MB, save/load with the sha256 check
// before torch::jit::load, and concurrent run() from several threads (the documented thread contract).
#include <gtest/gtest.h>

#include <thread>

#include "src/forecast/sha256.h"
#include "src/forecast/torch_net.h"

namespace pcr::fc {
namespace {

NetInput input(float v) {
  NetInput in;
  in.h1.assign(kH1Inputs, v);
  in.ctx.assign(kCtxTokens * kCtxFeats, v / 2);
  in.glob.assign(kGlobFeats, 0.25f);
  return in;
}

TEST(Sha256, KnownAnswers) {
  EXPECT_EQ(sha256_hex(""), "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
  EXPECT_EQ(sha256_hex("abc"), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
  EXPECT_EQ(sha256_hex("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"),
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1");
}

TEST(TorchNet, ShapesQuantileOrderAndSize) {
  auto n = make_net(NetConfig{}, 7);
  ASSERT_TRUE(n);
  EXPECT_LT(n->parameters() * 4, 1'000'000u);  // < 1 MB of float32 weights
  std::vector<NetOutput> out;
  ASSERT_TRUE(n->run({input(0.3f), input(0.9f)}, &out));
  ASSERT_EQ(out.size(), 2u);
  for (const auto& o : out) {
    ASSERT_EQ(o.h1.size(), static_cast<size_t>(kH1Steps * kQuantiles));
    ASSERT_EQ(o.h2a.size(), static_cast<size_t>(kH2Buckets * kQuantiles));
    ASSERT_EQ(o.h2h.size(), static_cast<size_t>(kH2Buckets * kQuantiles));
    for (size_t i = 0; i < o.h1.size(); i += kQuantiles) {
      EXPECT_LE(o.h1[i], o.h1[i + 1]);
      EXPECT_LE(o.h1[i + 1], o.h1[i + 2]);
    }
  }
}

TEST(TorchNet, SaveLoadChecksTheSha256First) {
  auto n = make_net(NetConfig{}, 11);
  std::string bytes, sha;
  ASSERT_TRUE(save_net(*n, &bytes, &sha));
  EXPECT_LT(bytes.size(), 1'000'000u);
  std::string why;
  auto m = load_net(NetConfig{}, bytes, sha, &why);
  ASSERT_TRUE(m) << why;
  std::vector<NetOutput> a, b;
  ASSERT_TRUE(n->run({input(0.5f)}, &a));
  ASSERT_TRUE(m->run({input(0.5f)}, &b));
  EXPECT_EQ(a[0].h1, b[0].h1);
  // One flipped byte: refused before parsing.
  std::string bad = bytes;
  bad[bad.size() / 2] ^= 0x01;
  EXPECT_FALSE(load_net(NetConfig{}, bad, sha, &why));
  EXPECT_EQ(why, "sha256");
  // Bytes that match their digest but aren't an archive of this config: refused.
  NetConfig other;
  other.layers = 3;
  EXPECT_FALSE(load_net(other, bytes, sha, &why));
  EXPECT_EQ(why, "archive");
  EXPECT_FALSE(load_net(NetConfig{}, "junk", sha256_hex("junk"), &why));
}

TEST(TorchNet, ConcurrentRunsMatchASingleThread) {
  auto n = make_net(NetConfig{}, 3);
  std::vector<NetOutput> want;
  ASSERT_TRUE(n->run({input(0.4f)}, &want));
  std::vector<std::thread> th;
  std::atomic<int> bad{0};
  for (int t = 0; t < 4; ++t) {
    th.emplace_back([&] {
      for (int i = 0; i < 50; ++i) {
        std::vector<NetOutput> got;
        if (!n->run({input(0.4f)}, &got) || got[0].h1 != want[0].h1) ++bad;
      }
    });
  }
  for (auto& x : th) x.join();
  EXPECT_EQ(bad.load(), 0);
}

}  // namespace
}  // namespace pcr::fc
