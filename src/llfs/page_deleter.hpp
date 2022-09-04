//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_DELETER_HPP
#define LLFS_PAGE_DELETER_HPP

#include <llfs/page_recycler_events.hpp>
#include <llfs/slice.hpp>
#include <llfs/slot.hpp>
#include <llfs/status.hpp>

#include <batteries/async/grant.hpp>

namespace llfs {

class PageRecycler;

// Used by a PageRecycler to atomically delete a group of pages.  The PageDeleter impl is
// responsible to turn around and call PageRecycler::recycle_pages for any pages whose ref_count
// goes to zero as a result of calling `PageDeleter::delete_pages`.
//
class PageDeleter
{
 public:
  PageDeleter(const PageDeleter&) = delete;
  PageDeleter& operator=(const PageDeleter&) = delete;

  virtual ~PageDeleter() = default;

  virtual Status delete_pages(const Slice<const PageToRecycle>& to_delete, PageRecycler& recycler,
                              slot_offset_type caller_slot, batt::Grant& recycle_grant,
                              i32 recycle_depth) = 0;

  // Called to indicate that the PageRecycler identified by `caller_uuid` has drained its backlog of
  // pages to recycle.
  //
  virtual void notify_caught_up(PageRecycler& recycler, slot_offset_type caller_slot)
  {
    (void)recycler;
    (void)caller_slot;
  }

  // Called to indicate that the PageRecycler identified by `caller_uuid` has encountered a fatal
  // error and is halting.
  //
  virtual void notify_failure(PageRecycler& recycler, Status failure)
  {
    (void)recycler;
    (void)failure;
  }

 protected:
  PageDeleter() = default;
};

}  // namespace llfs

#endif  // LLFS_PAGE_DELETER_HPP
