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

// Represents/tracks a single async PageDevice::write/drop operation.
//
struct PageWriteOp {
  PageWriteOp(const PageWriteOp&) = delete;
  PageWriteOp& operator=(const PageWriteOp&) = delete;

  batt::HandlerMemory<256> handler_memory;
  PageId page_id;
  batt::Watch<i64>* done_counter = nullptr;
  PageDevice::WriteResult result;

  static std::unique_ptr<PageWriteOp[]> allocate_array(usize n, batt::Watch<i64>& done_counter);

  explicit PageWriteOp() noexcept;

  ~PageWriteOp() noexcept;

  auto get_handler()
  {
    return make_custom_alloc_handler(this->handler_memory, [this](PageDevice::WriteResult result) {
      this->result = std::move(result);
      this->done_counter->fetch_add(1);
    });
  }
};

// Drops a range of PageIds in parallel.
//
Status parallel_drop_pages(const std::vector<PageId>& deleted_pages, PageCache& cache, u64 job_id,
                           u64 callers);

}  // namespace llfs

#endif  // LLFS_PAGE_WRITE_OP_HPP
