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

  void set_trim_pos(slot_offset_type trim_pos) noexcept;

  const PageRecyclerOptions& options() const;

  std::vector<PageToRecycle> recovered_pages() const;

  StatusOr<Optional<PageRecycler::Batch>> consume_latest_batch();

  const boost::uuids::uuid& recycler_uuid() const;

  Optional<SlotRange> latest_info_refresh_slot() const;

  slot_offset_type volume_trim_offset() const;
  u32 page_index() const;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Status operator()(const SlotParse&, const PageToRecycle& to_recycle);
  Status operator()(const SlotParse&, const PackedRecycleBatchCommit& commit);
  Status operator()(const SlotParse&, const PackedPageRecyclerInfo& info);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

 private:
  /** \brief The recycler options passed in at construction time.
   */
  PageRecyclerOptions options_;

  /** \brief The recycler log trim position.
   */
  slot_offset_type trim_pos_ = 0;

  /** \brief Contains all the pages from PackedRecyclePageInserted/PageToRecycle events found
   * during the scan.
   */
  std::unordered_map<PageId, PageToRecycle, PageId::Hash> recovered_pages_;

  /** \brief The most recent (highest slot offset) batch for which no PackedRecycleBatchCommit event
   * has yet been seen.
   */
  Optional<PageRecycler::Batch> latest_batch_;

  /** \brief The recycler UUID, as read in the most recent recycler info slot.
   */
  boost::uuids::uuid recycler_uuid_;

  /** \brief The most recent slot at which recycler info was refreshed.
   */
  Optional<SlotRange> latest_info_refresh_slot_;

  // This is to track largest unique_offset value during recovery.
  //
  slot_offset_type volume_trim_slot_;

  // This tracks the largest page_index seen so far for a given offset.
  //
  u32 largest_page_index_;
};

}  // namespace llfs

#endif  // LLFS_PAGE_RECYCLER_RECOVERY_VISITOR_HPP
