//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_BASIC_LOG_STORAGE_READER_HPP
#define LLFS_BASIC_LOG_STORAGE_READER_HPP

#include <llfs/basic_log_storage_driver.hpp>
#include <llfs/buffer.hpp>
#include <llfs/log_device.hpp>
#include <llfs/slot.hpp>
#include <llfs/status.hpp>

#include <batteries/assert.hpp>
#include <batteries/case_of.hpp>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename Impl>
class BasicLogStorageReader : public LogDevice::Reader
{
 public:
  BasicLogStorageReader(LogStorageDriverContext& context, BasicLogStorageDriver<Impl>& driver,
                        LogReadMode mode, slot_offset_type offset) noexcept
      : context_{context}
      , driver_{driver}
      , mode_{mode}
      , offset_{offset}
  {
    this->refresh_view(this->get_upper_bound());
  }

  bool is_closed() override
  {
    return this->is_closed_const();
  }

  bool is_closed_const() const
  {
    return this->context_.closed_.load();
  }

  ConstBuffer data() override
  {
    BATT_CHECK(!slot_less_than(this->offset_, this->driver_.get_trim_pos()))
        << "offset=" << this->offset_ << " trim_pos=" << this->driver_.get_trim_pos();

    return data_;
  }

  slot_offset_type slot_offset() override
  {
    return offset_;
  }

  void consume(std::size_t byte_count) override
  {
    BATT_CHECK_LE(byte_count, this->data_.size());

    this->offset_ += byte_count;
    this->data_ += byte_count;

    BATT_CHECK(!slot_less_than(this->offset_, this->driver_.get_trim_pos()))
        << "offset=" << this->offset_ << " trim_pos=" << this->driver_.get_trim_pos()
        << " byte_count=" << byte_count << " buffer.size()=" << this->context_.buffer_.size();
  }

  Status await(LogDevice::ReaderEvent event) override
  {
    if (this->is_closed()) {
      return Status{batt::StatusCode::kClosed};
    }

    return batt::case_of(
        event,
        [&](const SlotUpperBoundAt& slot) {
          BATT_DEBUG_INFO("[Reader::await] waiting for commit_flush_pos (="
                          << this->get_upper_bound() << ") to reach minimum of " << slot.offset
                          << "; trim_pos=" << this->driver_.get_trim_pos()
                          << " commit_pos=" << this->get_upper_bound());

          const StatusOr<slot_offset_type> new_upper_bound = this->await_upper_bound(slot.offset);

          if (new_upper_bound.ok()) {
            this->refresh_view(*new_upper_bound);
          }

          return new_upper_bound.status();
        },
        [&](const BytesAvailable& avail) -> Status {
          BATT_DEBUG_INFO("[Reader::await] waiting for BytesAvailable.size=" << avail.size);
          return this->await(SlotUpperBoundAt{this->offset_ + avail.size});
        });
  }

  //=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
 private:
  slot_offset_type get_upper_bound() const
  {
    switch (this->mode_) {
      case LogReadMode::kInconsistent:
      case LogReadMode::kSpeculative:
        return this->driver_.get_commit_pos();

      case LogReadMode::kDurable:
        return this->driver_.get_flush_pos();
    }

    BATT_PANIC() << "bad mode";
    BATT_UNREACHABLE();
  }

  StatusOr<slot_offset_type> await_upper_bound(slot_offset_type target) const
  {
    if (this->is_closed_const()) {
      return Status{batt::StatusCode::kClosed};
    }

    switch (this->mode_) {
      case LogReadMode::kInconsistent:
      case LogReadMode::kSpeculative:
        return this->driver_.await_commit_pos(target);

      case LogReadMode::kDurable:
        return this->driver_.await_flush_pos(target);
    }

    BATT_PANIC() << "bad mode";
    BATT_UNREACHABLE();
  }

  void refresh_view(slot_offset_type upper_bound)
  {
    data_ = [&] {
      ConstBuffer b = this->context_.buffer_.get(this->offset_);

      return ConstBuffer{b.data(), slot_distance(this->offset_, upper_bound)};
    }();
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  LogStorageDriverContext& context_;
  BasicLogStorageDriver<Impl>& driver_;
  LogReadMode mode_;
  slot_offset_type offset_;
  ConstBuffer data_;
};

}  // namespace llfs

#endif  // LLFS_BASIC_LOG_STORAGE_READER_HPP
