//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_VOLUME_READER_HPP
#define LLFS_VOLUME_READER_HPP

#include <llfs/log_device.hpp>
#include <llfs/page_loader.hpp>
#include <llfs/slot_read_lock.hpp>
#include <llfs/slot_reader.hpp>
#include <llfs/volume_options.hpp>

#include <functional>
#include <string_view>

namespace llfs {

class Volume;
class VolumeReader;

template <typename T>
class TypedVolumeReader;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class VolumeReader : public PageLoader
{
 public:
  template <typename T>
  friend class TypedVolumeReader;

  class Impl;

  using SlotVisitorFn = std::function<Status(const SlotParse& slot, std::string_view user_data)>;

  explicit VolumeReader(Volume& volume, SlotReadLock&& read_lock, LogReadMode mode) noexcept;

  VolumeReader() = default;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // VolumeReader is a move-only type.
  //
  VolumeReader(const VolumeReader&) = delete;
  VolumeReader& operator=(const VolumeReader&) = delete;

  VolumeReader(VolumeReader&& that) noexcept : impl_{std::move(that.impl_)}
  {
  }

  VolumeReader& operator=(VolumeReader&& that) noexcept
  {
    this->impl_ = std::move(that.impl_);
    return *this;
  }
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  const VolumeOptions& volume_options() const;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // PageLoader methods
  //
  PageCache* page_cache() const override;

  void prefetch_hint(PageId page_id) override;

  StatusOr<PinnedPage> try_pin_cached_page(PageId page_id, const PageLoadOptions& options) override;

  StatusOr<PinnedPage> load_page(PageId page_id, const PageLoadOptions& options) override;
  //
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // The current range of slot offsets locked by this VolumeReader.
  //
  SlotRange slot_range() const;

  // Returns a copy of the lock for this VolumeReader's slot range.
  //
  SlotReadLock clone_lock() const;

  // Release part of this VolumeReader's lock on the log.
  //
  Status trim(slot_offset_type trim_upper_bound);

  // Apply the given visitor function to the next unvisited slot.  Returns the number of slots
  // visited (0 or 1).
  //
  template <typename F = SlotVisitorFn>
  StatusOr<usize> visit_next(batt::WaitForResource wait_for_commit, F&& visitor_fn);

  // Loops calling `visit_next`/`trim` until non-OK is encountered.  Returns the number of slots
  // visited.
  //
  template <typename F = SlotVisitorFn>
  StatusOr<usize> consume_slots(batt::WaitForResource wait_for_commit, F&& visitor_fn);

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  class ImplDeleter
  {
   public:
    void operator()(Impl*) const;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::unique_ptr<Impl, ImplDeleter> impl_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// VolumeReader that coerces user data into packed type `T` before dispatching to an overloaded set
// of case handler functions.
//
template <typename... Ts>
class TypedVolumeReader<PackedVariant<Ts...>> : public VolumeReader
{
 public:
  using Self = TypedVolumeReader;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit TypedVolumeReader(VolumeReader&& reader) noexcept : VolumeReader{}
  {
    this->impl_ = std::move(reader.impl_);
  }

  // Apply the given visitor function to the SlotRange and Payload of the slot containing the
  // given offset.
  //
  template <typename... CaseHandlerFns>
  StatusOr<usize> visit_typed_next(batt::WaitForResource wait_for_commit,
                                   CaseHandlerFns&&... case_handler_fns)
  {
    return this->visit_next(wait_for_commit,
                            TypedSlotReader<PackedVariant<Ts...>>::make_slot_visitor(
                                BATT_FORWARD(case_handler_fns)...));
  }

  // Loops calling `visit_next`/`trim` until non-OK is encountered.
  //
  template <typename... CaseHandlerFns>
  StatusOr<usize> consume_typed_slots(batt::WaitForResource wait_for_commit,
                                      CaseHandlerFns&&... case_handler_fns)
  {
    return this->consume_slots(wait_for_commit,
                               TypedSlotReader<PackedVariant<Ts...>>::make_slot_visitor(
                                   BATT_FORWARD(case_handler_fns)...));
  }
};

}  // namespace llfs

#endif  // LLFS_VOLUME_READER_HPP
