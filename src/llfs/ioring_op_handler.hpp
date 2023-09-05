//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_IORING_OP_HANDLER_HPP
#define LLFS_IORING_OP_HANDLER_HPP

#include <llfs/config.hpp>
//

#ifndef LLFS_DISABLE_IO_URING

#include <llfs/int_types.hpp>

#include <batteries/utility.hpp>

#include <boost/asio/associated_allocator.hpp>

#include <liburing.h>

#include <algorithm>
#include <iterator>

namespace llfs {

template <typename Fn>
class IoRingOpHandler
{
 public:
  using allocator_type = boost::asio::associated_allocator_t<Fn>;

  explicit IoRingOpHandler(Fn&& fn) noexcept : fn_{BATT_FORWARD(fn)}
  {
  }

  allocator_type get_allocator() const noexcept
  {
    return boost::asio::get_associated_allocator(this->fn_);
  }

  template <typename... Args>
  void operator()(Args&&... args)
  {
    this->fn_(BATT_FORWARD(args)...);
  }

  // Append a buffer to the end of `this->iov_`.
  //
  // This function MUST NOT be called more times than the value `entries` passed to
  // `IoRingOpHandler::make_new` used to allocate this object.
  //
  template <typename B>
  void push_buffer(const B& buf)
  {
    struct iovec& iov = this->iov_[this->iov_count_];
    this->iov_count_ += 1;
    iov.iov_base = (void*)buf.data();
    iov.iov_len = buf.size();
  }

  // Consume `byte_count` bytes from the front of the buffers list in `this->iov_`.
  //
  void shift_buffer(usize byte_count)
  {
    struct iovec* next = this->iov_;
    struct iovec* last = this->iov_ + this->iov_count_;

    while (next != last && byte_count) {
      usize n_to_consume = std::min(next->iov_len, byte_count);
      next->iov_base = ((u8*)next->iov_base) + n_to_consume;
      next->iov_len -= n_to_consume;
      byte_count -= n_to_consume;
      ++next;
    }

    if (this->iov_ != next) {
      struct iovec* new_last = std::copy(next, last, this->iov_);
      this->iov_count_ = std::distance(this->iov_, new_last);
    }
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Fn fn_;
  usize iov_count_ = 0;
  struct iovec iov_[0];  // MUST BE LAST!
};

}  //namespace llfs

#endif  // LLFS_DISABLE_IO_URING

#endif  // LLFS_IORING_OP_HANDLER_HPP
