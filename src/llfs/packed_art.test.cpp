//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/packed_art.hpp>
//
#include <llfs/packed_art.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <llfs/trie.test.hpp>

#include <algorithm>
#include <random>

namespace {

using namespace ::llfs::int_types;

inline void dump_packed_art(std::ostream& out, const llfs::PackedART::NodeBase* root, usize depth)
{
  std::string indent(depth * 4, '.');

  switch (root->kind) {
    case llfs::PackedART::kLeaf:
      out << indent << "Leaf{.prefix=" << batt::c_str_literal(root->prefix_str()) << "}"
          << std::endl;
      break;
    case llfs::PackedART::kSmall:
      out << indent << "Small{.prefix=" << batt::c_str_literal(root->prefix_str()) << "}"
          << std::endl;

      llfs::PackedART::visit_branches(
          (const llfs::PackedART::SmallNode*)root,
          [&](u8 branch_byte, const llfs::PackedART::NodeBase* branch_root) {
            out << indent << batt::c_str_literal(std::string_view{(const char*)&branch_byte, 1})
                << std::endl;
            dump_packed_art(out, branch_root, depth + 1);
          });
      break;
    case llfs::PackedART::kMedium:
      out << indent << "Medium{.prefix=" << batt::c_str_literal(root->prefix_str()) << "}"
          << std::endl;
      break;
    case llfs::PackedART::kLarge:
      out << indent << "Large{.prefix=" << batt::c_str_literal(root->prefix_str()) << "}"
          << std::endl;
      break;
    default:
      BATT_PANIC() << BATT_INSPECT((void*)root) << BATT_INSPECT((int)root->kind);
      BATT_UNREACHABLE();
  }
}

TEST(PackedArtTest, Test)
{
  std::vector<std::string> words = llfs::testing::load_words();

  std::vector<std::string> small_words;
  std::vector<std::string> large_words;
  for (const std::string& s : words) {
    if (s.size() < 8 && s[0] >= 'a') {
      small_words.push_back(s);
    } else if (s.size() > 15 && s[0] >= 'a') {
      large_words.push_back(s);
    }
  }

  std::default_random_engine rng{1};

  {
    std::vector<std::string> inputs(100);

    std::sample(large_words.begin() + 5000, large_words.begin() + 5500, inputs.begin(),
                inputs.size(), rng);
    std::sort(inputs.begin(), inputs.end());

    std::cerr << BATT_INSPECT_RANGE(inputs) << std::endl;

    const usize buffer_len = usize{1} << 20;
    std::unique_ptr<char[]> buffer{new char[buffer_len]};

    llfs::MutableBuffer dst{buffer.get(), buffer_len};

    llfs::StatusOr<llfs::PackedART::NodeBase*> root = llfs::pack_art(
        inputs.begin(), inputs.end(),
        [](const std::string& s) -> std::string_view {
          return s;
        },
        dst);

    ASSERT_TRUE(root.ok());

    dump_packed_art(std::cerr, *root, 0);
  }
}

}  // namespace
