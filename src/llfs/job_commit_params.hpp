//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_JOB_COMMIT_PARAMS_HPP
#define LLFS_JOB_COMMIT_PARAMS_HPP

#include <llfs/int_types.hpp>
#include <llfs/page_recycler.hpp>
#include <llfs/ref.hpp>
#include <llfs/slot.hpp>

#include <batteries/async/grant.hpp>

#include <boost/uuid/uuid.hpp>

namespace llfs {

struct JobCommitParams {
  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // A universally unique identifier providing the scope for `caller_slot`; together these two
  // params guarantee "exactly once" commit semantics (i.e., idempotence in the face of crashes).
  //
  const boost::uuids::uuid* caller_uuid = nullptr;

  // A job committed for a given caller_uuid/caller_slot is guaranteed to be durably committed
  // exactly once.
  //
  slot_offset_type caller_slot = 0;
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // The PageRecycler to use if ref count updates produce dead pages.
  //
  Ref<PageRecycler> recycler;

  // (Optional) Passed through to the PageRecycler; mostly used by PageRecycler itself when doing
  // recursive garbage collection.
  //
  batt::Grant* recycle_grant = nullptr;
  i32 recycle_depth = -1;
};

}  // namespace llfs

#endif  // LLFS_JOB_COMMIT_PARAMS_HPP
