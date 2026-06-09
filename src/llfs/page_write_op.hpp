//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_WRITE_OP_HPP
#define LLFS_PAGE_WRITE_OP_HPP

#include <llfs/int_types.hpp>
#include <llfs/page_cache.hpp>
#include <llfs/page_device.hpp>
#include <llfs/page_id.hpp>
#include <llfs/seq.hpp>

#include <batteries/async/handler.hpp>
#include <batteries/async/watch.hpp>

#include <memory>
#include <vector>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief Represents/tracks a single async PageDevice::write/drop operation.
 */

struct PageWriteOp {
  struct HandlerImpl {
    PageWriteOp* op_;

    void operator()(PageDevice::WriteResult result) const noexcept;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit PageWriteOp() noexcept;

  PageWriteOp(const PageWriteOp&) = delete;
  PageWriteOp& operator=(const PageWriteOp&) = delete;

  ~PageWriteOp() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns a write handler for this op; there must only be one active handler per op at a
   * time!  If `this->get_handler()` is called twice before invoking the first returned handler (or
   * a copy of it), this function will panic.
   */
  batt::CustomAllocHandler<PageWriteOp::HandlerImpl> get_handler(
      PageId id, batt::Watch<i64>* done_counter) noexcept;

  /** \brief Returns the most recent result that was passed to the handler; only valid after
   * get_handler has been called and the returned handler has been invoked.
   */
  const PageDevice::WriteResult& result() const noexcept
  {
    return this->result_;
  }

  /** \brief Returns the most recent PageId passed to get_handler.
   */
  PageId page_id() const noexcept
  {
    return this->page_id_;
  }

  /** \brief Returns true iff this op has a handler which hasn't been invoked.
   */
  bool is_pending() const noexcept
  {
    return this->pending_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  void handle_write(PageDevice::WriteResult result) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Pre-allocated memory for the write handler.
   */
  batt::HandlerMemory<256> handler_memory_;

  /** \brief The Watch to increment when this operation's handler is invoked.
   */
  batt::Watch<i64>* done_counter_ = nullptr;

  /** \brief The most recent page associated with this op.
   */
  PageId page_id_;

  /** \brief The result of the last operation.
   */
  PageDevice::WriteResult result_;

  /** \brief Set to true when get_handler is called; set to false when that handler is invoked.
   */
  bool pending_ = false;
};

// Drops a range of PageIds in parallel.
//
Status parallel_drop_pages(const std::vector<PageId>& deleted_pages, PageCache& cache, u64 job_id,
                           u64 callers);

}  // namespace llfs

#endif  // LLFS_PAGE_WRITE_OP_HPP
