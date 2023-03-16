//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_RECYCLER_RECOVERY_VISITOR_HPP
#define LLFS_PAGE_RECYCLER_RECOVERY_VISITOR_HPP

#include <llfs/optional.hpp>
#include <llfs/page_id.hpp>
#include <llfs/page_layout.hpp>
#include <llfs/page_recycler.hpp>
#include <llfs/status.hpp>

#include <unordered_map>

namespace llfs {

// Visits the slots of a PageRecycler log to recover its state after restart/crash.
//
class PageRecyclerRecoveryVisitor
{
 public:
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit PageRecyclerRecoveryVisitor(const PageRecyclerOptions& default_options) noexcept;

  ~PageRecyclerRecoveryVisitor() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  const PageRecyclerOptions& options() const;

  std::vector<PageToRecycle> recovered_pages() const;

  StatusOr<Optional<PageRecycler::Batch>> consume_latest_batch();

  const boost::uuids::uuid& recycler_uuid() const;

  Optional<SlotRange> latest_info_refresh_slot() const;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Status operator()(const SlotParse&, const PageToRecycle& to_recycle);
  Status operator()(const SlotParse&, const PackedRecyclePagePrepare& prepare);
  Status operator()(const SlotParse&, const PackedRecycleBatchCommit& commit);
  Status operator()(const SlotParse&, const PackedPageRecyclerInfo& info);

 private:
  PageRecyclerOptions options_;
  std::unordered_map<PageId, PageToRecycle, PageId::Hash> recovered_pages_;
  Optional<slot_offset_type> latest_batch_slot_;
  std::vector<PageId> latest_batch_pages_;
  boost::uuids::uuid recycler_uuid_;
  Optional<SlotRange> latest_info_refresh_slot_;
};

}  // namespace llfs

#endif  // LLFS_PAGE_RECYCLER_RECOVERY_VISITOR_HPP
