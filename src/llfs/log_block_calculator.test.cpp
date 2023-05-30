//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/log_block_calculator.hpp>
//
#include <llfs/log_block_calculator.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <llfs/constants.hpp>
#include <llfs/int_types.hpp>

#include <batteries/checked_cast.hpp>

namespace {

using namespace llfs::int_types;
using namespace llfs::constants;

TEST(LogBlockCalculator, Test)
{
  ASSERT_EQ(sizeof(llfs::PackedLogPageHeader), 64u);

  const usize log_size = 1 * kMiB;

  for (const usize queue_depth : {1, 2, 4, 4096}) {
    for (const usize block_size :
         {usize{512}, usize{1 * kKiB}, usize{16 * kKiB}, usize{64 * kKiB}}) {
      ASSERT_EQ(block_size % llfs::kLogPageSize, 0u);

      for (const i64 begin_file_offset : {i64(0), i64(llfs::kLogPageSize * 3)}) {
        const usize pages_per_block = block_size / llfs::kLogPageSize;

        const usize pages_per_block_log2 = batt::log2_ceil(pages_per_block);

        const u64 physical_size =
            llfs::LogBlockCalculator::disk_size_required_for_log_size(log_size, block_size);

        const i64 end_file_offset =
            BATT_CHECKED_CAST(i64, BATT_CHECKED_CAST(u64, begin_file_offset) + physical_size);

        ASSERT_EQ(1ull << pages_per_block_log2, pages_per_block);
        ASSERT_EQ(pages_per_block * llfs::kLogPageSize, block_size);

        llfs::LogBlockCalculator calculate{
            llfs::IoRingLogConfig{
                .logical_size = log_size,
                .physical_offset = begin_file_offset,
                .physical_size = physical_size,
                .pages_per_block_log2 = pages_per_block_log2,
            },
            llfs::IoRingLogDriverOptions::with_default_values()  //
                .set_queue_depth(queue_depth),
        };

        EXPECT_EQ(calculate.pages_per_block(), pages_per_block);
        EXPECT_EQ(calculate.block_size(), block_size);
        EXPECT_EQ(calculate.block_capacity(), block_size - 64);
        EXPECT_GE(calculate.block_capacity() * calculate.block_count(), log_size);
        EXPECT_EQ(calculate.block_count(),
                  (log_size + block_size - 64 - 1) / (block_size - 64) + 1);
        EXPECT_EQ(calculate.physical_size(), physical_size);
        EXPECT_GT(physical_size, log_size);
        EXPECT_EQ(physical_size, calculate.block_count() * calculate.block_size());
        EXPECT_LT(log_size, calculate.block_count() * calculate.block_capacity());
        EXPECT_EQ(calculate.queue_depth(), queue_depth);

        {
          usize logical_block_index = 0;
          usize physical_block_index = 0;
          usize flush_op_index = 0;
          i64 file_offset = begin_file_offset;

          for (llfs::slot_offset_type slot_offset = 0; slot_offset < log_size * 3;) {
            const llfs::slot_offset_type slot_begin = slot_offset;
            const llfs::slot_offset_type slot_end = slot_offset + calculate.block_capacity();

            EXPECT_EQ(physical_block_index,
                      calculate.physical_block_index_from(
                          llfs::LogBlockCalculator::LogicalBlockIndex{logical_block_index}));

            EXPECT_EQ((llfs::SlotRange{slot_begin, slot_end}),
                      calculate.block_slot_range_from(
                          llfs::LogBlockCalculator::LogicalBlockIndex{logical_block_index}));

            EXPECT_EQ(flush_op_index,
                      calculate.flush_op_index_from(
                          llfs::LogBlockCalculator::LogicalBlockIndex{logical_block_index}));

            EXPECT_EQ(file_offset,
                      calculate.block_start_file_offset_from(
                          llfs::LogBlockCalculator::PhysicalBlockIndex{physical_block_index}));

            EXPECT_EQ(file_offset,
                      calculate.block_start_file_offset_from(
                          llfs::LogBlockCalculator::LogicalBlockIndex{logical_block_index}));

            for (usize i = 0; i < calculate.block_capacity(); ++i, ++slot_offset) {
              EXPECT_EQ(logical_block_index,
                        calculate.logical_block_index_from(llfs::SlotLowerBoundAt{slot_offset}));

              EXPECT_EQ(logical_block_index, calculate.logical_block_index_from(
                                                 llfs::SlotUpperBoundAt{slot_offset + 1}));

              EXPECT_EQ(physical_block_index,
                        calculate.physical_block_index_from(llfs::SlotLowerBoundAt{slot_offset}));

              EXPECT_EQ(physical_block_index, calculate.physical_block_index_from(
                                                  llfs::SlotUpperBoundAt{slot_offset + 1}));

              EXPECT_EQ((llfs::SlotRange{slot_begin, slot_end}),
                        calculate.block_slot_range_from(llfs::SlotLowerBoundAt{slot_offset}));

              EXPECT_EQ((llfs::SlotRange{slot_begin, slot_end}),
                        calculate.block_slot_range_from(llfs::SlotUpperBoundAt{slot_offset + 1}));

              EXPECT_EQ(flush_op_index,
                        calculate.flush_op_index_from(llfs::SlotLowerBoundAt{slot_offset}));

              EXPECT_EQ(flush_op_index,
                        calculate.flush_op_index_from(llfs::SlotUpperBoundAt{slot_offset + 1}));

              EXPECT_EQ(file_offset, calculate.block_start_file_offset_from(
                                         llfs::SlotLowerBoundAt{slot_offset}));

              EXPECT_EQ(file_offset, calculate.block_start_file_offset_from(
                                         llfs::SlotUpperBoundAt{slot_offset + 1}));
            }

            logical_block_index += 1;

            physical_block_index += 1;
            if (physical_block_index >= calculate.block_count()) {
              physical_block_index = 0;
            }

            const usize prev_flush_op_index = flush_op_index;
            flush_op_index += 1;
            if (flush_op_index >= queue_depth) {
              flush_op_index = 0;
            }

            EXPECT_EQ(flush_op_index,
                      calculate.next_flush_op_index(
                          llfs::LogBlockCalculator::FlushOpIndex{prev_flush_op_index}));

            file_offset += block_size;
            if (file_offset >= end_file_offset) {
              file_offset -= physical_size;
              EXPECT_EQ(file_offset, begin_file_offset);
            }
          }
        }
      }
    }
  }
}

}  // namespace
