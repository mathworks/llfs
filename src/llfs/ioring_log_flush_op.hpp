//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_IORING_LOG_FLUSH_OP_HPP
#define LLFS_IORING_LOG_FLUSH_OP_HPP

#include <llfs/config.hpp>

#ifndef LLFS_DISABLE_IO_URING

#include <llfs/data_layout.hpp>
#include <llfs/int_types.hpp>
#include <llfs/ioring.hpp>
#include <llfs/page_buffer.hpp>
#include <llfs/slot.hpp>
#include <llfs/status.hpp>

#include <batteries/metrics/metric_collectors.hpp>

#include <batteries/async/handler.hpp>
#include <batteries/async/watch.hpp>

namespace llfs {

using LogPageBuffer = std::aligned_storage_t<kLogPageSize, 512>;

class IoRingLogDriver;

class IoRingLogFlushOp
{
 public:
  struct Metrics {
    LatencyMetric write_latency;
    CountMetric<u64> bytes_written{0};
  };

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  // Used internally to frame on-disk pages of log data.
  //
  struct PackedPageHeader {
    static constexpr u64 kMagic = 0xc3392dfb394e0349ull;

    // Must be `kMagic`.
    //
    big_u64 magic;

    // The slot offset of the first payload byte in this block.
    //
    little_u64 slot_offset;

    // The number of valid bytes in the payload section of this block.
    //
    little_u64 commit_size;

    // The crc64 of this page, with this field set to 0.
    //
    little_u64 crc64;  // TODO [tastolfi 2021-06-16] implement me

    // The current last known trim pos for the log.
    //
    little_u64 trim_pos;

    // The current last known flush pos for the log.
    //
    little_u64 flush_pos;

    // Reserved for future use.
    //
    little_u8 reserved_[16];
  };
  static_assert(sizeof(PackedPageHeader) == 64, "");

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  IoRingLogFlushOp() = default;

  IoRingLogFlushOp(const IoRingLogFlushOp&) = delete;
  IoRingLogFlushOp& operator=(const IoRingLogFlushOp&) = delete;

  ~IoRingLogFlushOp() noexcept;

  //-----

  void initialize(IoRingLogDriver* driver);

  void activate();

  IoRing& get_ioring();

  void poll_commit_state();

  //-----

  void poll_commit_pos(slot_offset_type known_commit_pos);

  void wait_for_commit(slot_offset_type known_commit_pos, slot_offset_type min_required);

  //-----

  void start_flush();

  void handle_flush(const StatusOr<i32>& result);

  auto get_flush_handler()
  {
    return make_custom_alloc_handler(this->handler_memory_, [this](const StatusOr<i32>& result) {
      this->handle_flush(result);
      this->poll_commit_state();
    });
  }

  //-----

  usize self_index();

  PackedPageHeader* get_header() const;

  // Copy data from the device ring buffer to this->page_block; return true if some data was copied.
  //
  bool fill_buffer(slot_offset_type known_commit_pos);

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  slot_offset_type flush_pos() const
  {
    return this->flush_pos_;
  }

  MutableBuffer get_buffer() const;

  const Metrics& metrics() const
  {
    return this->metrics_;
  }

 private:
  void finish_flush();

  Metrics metrics_;

  IoRingLogDriver* driver_ = nullptr;

  std::unique_ptr<LogPageBuffer[]> page_block_;

  slot_offset_type flush_pos_ = 0;

  ConstBuffer ready_to_write_{nullptr, 0};

  // Dedicated static memory buffer to lower the overhead of asynchronous calls.
  //
  batt::HandlerMemory<128> handler_memory_;

  // The offset within the log file to which this op's current page should be flushed.
  //
  u64 file_offset_ = 0;

  u64 next_write_offset_ = 0;

  // Active only during an asynchronous write (flush) operation.
  //
  Optional<LatencyTimer> write_timer_;
};

BATT_STATIC_ASSERT_EQ(sizeof(IoRingLogFlushOp::PackedPageHeader), 64);

}  // namespace llfs

#endif  // LLFS_DISABLE_IO_URING
#endif  // LLFS_IORING_LOG_FLUSH_OP_HPP
