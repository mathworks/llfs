//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_BASIC_LOG_STORAGE_DRIVER_HPP
#define LLFS_BASIC_LOG_STORAGE_DRIVER_HPP

#include <llfs/log_device.hpp>
#include <llfs/log_storage_driver_context.hpp>
#include <llfs/ring_buffer.hpp>
#include <llfs/slot.hpp>
#include <llfs/status.hpp>

#include <batteries/type_traits.hpp>
#include <batteries/utility.hpp>

#include <atomic>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <class Impl>
class BasicLogStorageDriver
{
 public:
  using Context = LogStorageDriverContext;

  template <typename... Args, typename = batt::EnableIfNoShadow<
                                  BasicLogStorageDriver, LogStorageDriverContext&, Args&&...>>
  explicit BasicLogStorageDriver(Context& context, Args&&... args) noexcept
      : impl_{context, BATT_FORWARD(args)...}
  {
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  // A storage driver impl must provide these 10 methods.
  //
  Status set_trim_pos(slot_offset_type trim_pos)
  {
    return this->impl_.set_trim_pos(trim_pos);
  }

  slot_offset_type get_trim_pos() const
  {
    return this->impl_.get_trim_pos();
  }

  StatusOr<slot_offset_type> await_trim_pos(slot_offset_type trim_pos)
  {
    return this->impl_.await_trim_pos(trim_pos);
  }

  //----

  // There is no set_flush_pos because it's up to the storage driver to flush log data and update
  // the flush pos in the background.

  slot_offset_type get_flush_pos() const
  {
    return this->impl_.get_flush_pos();
  }

  StatusOr<slot_offset_type> await_flush_pos(slot_offset_type flush_pos)
  {
    return this->impl_.await_flush_pos(flush_pos);
  }

  //----

  Status set_commit_pos(slot_offset_type commit_pos)
  {
    return this->impl_.set_commit_pos(commit_pos);
  }

  slot_offset_type get_commit_pos() const
  {
    return this->impl_.get_commit_pos();
  }

  StatusOr<slot_offset_type> await_commit_pos(slot_offset_type commit_pos)
  {
    return this->impl_.await_commit_pos(commit_pos);
  }

  //----

  Status open()
  {
    return this->impl_.open();
  }

  Status close()
  {
    return this->impl_.close();
  }

  void halt()
  {
    this->impl_.halt();
  }

  void join()
  {
    this->impl_.join();
  }
  //
  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  Impl& impl() noexcept
  {
    return this->impl_;
  }

  const Impl& impl() const noexcept
  {
    return this->impl_;
  }

 private:
  Impl impl_;
};

}  // namespace llfs

#endif  // LLFS_BASIC_LOG_STORAGE_DRIVER_HPP
