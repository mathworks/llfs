#include <llfs/ring_buffer.hpp>
//
#include <llfs/ring_buffer.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstring>

namespace {

using llfs::RingBuffer;

TEST(RingBufferTest, Test)
{
  RingBuffer rb{RingBuffer::TempFile{4096}};

  ASSERT_GE(rb.size(), 4096ul);

  auto b = rb.get_mut(0);
  char* p = (char*)b.data();

  const char kTestData[] = "Four score and seven years ago...";

  std::memcpy(&p[0], &kTestData[10], std::strlen(kTestData) - 10);
  std::memcpy(&p[rb.size() - 10], kTestData, 10);

  auto b2 = rb.get(rb.size() * 19 - 10);

  EXPECT_EQ(0, std::memcmp(b2.data(), kTestData, std::strlen(kTestData)));
}

}  // namespace
