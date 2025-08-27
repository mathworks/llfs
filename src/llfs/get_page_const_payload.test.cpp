//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/get_page_const_payload.hpp>
//
#include <llfs/get_page_const_payload.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <llfs/opaque_page_view.hpp>
#include <llfs/page_cache_slot.hpp>

#include <batteries/utility.hpp>

namespace {

using namespace llfs::int_types;

TEST(GetPageConstPayloadTest, Test)
{
  // Case 0: ConstBuffer from ConstBuffer
  //
  {
    char str[6] = "hello";
    llfs::ConstBuffer src{str, 6};
    llfs::ConstBuffer payload = llfs::get_page_const_payload(src);

    EXPECT_EQ(payload.data(), src.data());
    EXPECT_EQ(payload.size(), src.size());
  }

  // Case 1: ConstBuffer from std::shared_ptr<PageBuffer>
  //
  const llfs::PageSize page_size{8192};
  const llfs::PageId page_id{1};
  std::shared_ptr<llfs::PageBuffer> page_buffer = llfs::PageBuffer::allocate(page_size, page_id);

  ASSERT_NE(page_buffer, nullptr);

  const void* const page_payload_data = page_buffer->const_payload().data();
  const usize page_payload_size = page_buffer->const_payload().size();
  {
    llfs::ConstBuffer payload = llfs::get_page_const_payload(page_buffer);

    EXPECT_EQ(payload.data(), page_payload_data);
    EXPECT_EQ(payload.size(), page_payload_size);
  }

  // Case 2: ConstBuffer from PageBuffer
  //
  {
    llfs::ConstBuffer payload = llfs::get_page_const_payload(*page_buffer);

    EXPECT_EQ(payload.data(), page_payload_data);
    EXPECT_EQ(payload.size(), page_payload_size);
  }

  // Case 3: ConstBuffer from PageView
  //
  const llfs::OpaquePageView page_view{batt::make_copy(page_buffer)};
  {
    llfs::ConstBuffer payload = llfs::get_page_const_payload(page_view);

    EXPECT_EQ(payload.data(), page_payload_data);
    EXPECT_EQ(payload.size(), page_payload_size);
  }

  // Case 4: ConstBuffer from PinnedPage
  //
  const auto num_slots = llfs::SlotCount{1};
  const char* const pool_name = "test_cache_slot_pool";

  boost::intrusive_ptr<llfs::PageCacheSlot::Pool> slot_pool = llfs::PageCacheSlot::Pool::make_new(
      num_slots, llfs::MaxCacheSizeBytes{num_slots * page_size}, pool_name);

  ASSERT_NE(slot_pool, nullptr);

  llfs::PageCacheSlot* slot = nullptr;
  llfs::PageCacheSlot::Pool::ExternalAllocation claim;
  std::tie(slot, claim) = slot_pool->allocate(page_size);

  ASSERT_NE(slot, nullptr);

  const llfs::PageCacheSlot::PinnedRef pinned_ref =
      slot->fill(page_id, page_size, llfs::LruPriority{1}, std::move(claim));
  const llfs::PinnedPage pinned_page{&page_view, batt::make_copy(pinned_ref)};

  {
    llfs::ConstBuffer payload = llfs::get_page_const_payload(pinned_page);

    EXPECT_EQ(payload.data(), page_payload_data);
    EXPECT_EQ(payload.size(), page_payload_size);
  }
}

}  // namespace
