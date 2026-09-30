//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/stable_string_store.hpp>
//

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string>

namespace {

using namespace batt::int_types;

TEST(StableStringStore, StaticAllocationTest)
{
  llfs::StableStringStore strings;

  auto out =
      strings.concat(std::string_view{"Hello"}, std::string_view{", "}, std::string_view{"World!"});

  std::string_view out_str{(const char*)out.data(), out.size()};

  EXPECT_THAT(out_str, ::testing::StrEq("Hello, World!"));

  // Since "Hello, World!" is less than kStaticAllocSize, test to see that the string's data was
  // allocated statically, i.e., it "lives" inside the bounds of the StableStringStore object
  // itself.
  //
  EXPECT_TRUE(out.data() >= static_cast<const void*>(&strings) &&
              out.data() < static_cast<const void*>(&strings + 1));
}

TEST(StableStringStore, DynamicAllocationTest)
{
  llfs::StableStringStore strings;
  const usize data_size = 1;
  const usize num_iterations_of_static_alloc = strings.kStaticAllocSize / data_size;

  // Statically allocate a bunch of string data up to the static allocation limit.
  //
  for (usize i = 0; i < num_iterations_of_static_alloc; ++i) {
    std::string_view string_to_store{"a"};
    std::string_view copied_string = strings.store(string_to_store);
    EXPECT_TRUE(
        static_cast<const void*>(copied_string.data()) >= static_cast<const void*>(&strings) &&
        static_cast<const void*>(copied_string.data()) < static_cast<const void*>(&strings + 1));
  }

  // Now perform another store. Since we have already allocated an amount of data greater than the
  // size of kStaticAllocSize, we end up dynmically allocating the data for this string.
  //
  std::string_view dynamically_allocated_string{"b"};
  std::string_view copy_stored = strings.store(dynamically_allocated_string);
  EXPECT_TRUE(static_cast<const void*>(copy_stored.data()) < static_cast<const void*>(&strings) ||
              static_cast<const void*>(copy_stored.data()) >=
                  static_cast<const void*>(&strings + 1));
}

TEST(StableStringStore, LargeDynamicAllocationTest)
{
  llfs::StableStringStore strings;
  const usize data_size = strings.kDynamicAllocSize + 1;
  const usize num_allocations = 10;

  // Allocate large strings, all with a size greater that kDynamicAllocSize. This will trigger
  // multiple dynamic memory allocations.
  //
  std::string_view previous_string;
  for (usize i = 0; i < num_allocations; ++i) {
    if (i > 0) {
      // Check to make sure that the memory for previously allocated strings doesn't go out of
      // scope; memory of the string data is scoped to the lifetime of the StableStringObject.
      //
      std::string expected_previous_string(data_size, 'a' + (i - 1));
      EXPECT_EQ(previous_string, expected_previous_string);
    }

    std::string large_string_data(data_size, 'a' + i);
    std::string_view string_to_store{large_string_data};
    previous_string = strings.store(string_to_store);
  }
}

}  // namespace
