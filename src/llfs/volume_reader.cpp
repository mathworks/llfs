//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/volume_reader.hpp>
//
#include <llfs/volume.hpp>
#include <llfs/volume_reader.ipp>
#include <llfs/volume_slot_demuxer.hpp>

namespace llfs {

namespace {

}  // namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ VolumeReader::Impl::Impl(Volume& volume, SlotReadLock&& read_lock,
                                      LogReadMode mode) noexcept
    : volume_{volume}
    , read_lock_{std::move(read_lock)}
    , job_{this->volume_.cache().new_job()}
    , log_reader_{this->volume_.root_log_->new_reader(this->read_lock_.slot_range().lower_bound,
                                                      mode)}
    , slot_reader_{*this->log_reader_}
    , paused_{true}
    , trim_lock_update_lower_bound_{this->log_reader_->slot_offset() +
                                    this->volume_.options().trim_lock_update_interval}
{
  this->slot_reader_.set_pre_slot_fn([this](slot_offset_type) {
    if (this->paused_) {
      return seq::LoopControl::kBreak;
    } else {
      return seq::LoopControl::kContinue;
    }
  });

  BATT_CHECK(this->read_lock_);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void VolumeReader::ImplDeleter::operator()(Impl* ptr) const
{
  delete ptr;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const VolumeOptions& VolumeReader::volume_options() const
{
  return this->impl_->volume_.options();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ VolumeReader::VolumeReader(Volume& volume, SlotReadLock&& read_lock,
                                        LogReadMode mode) noexcept
    : impl_{new Impl{volume, std::move(read_lock), mode}}
{
  BATT_CHECK(this->impl_->read_lock_);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageCache* VolumeReader::page_cache() const /*override*/
{
  BATT_UNTESTED_LINE();
  return this->impl_->job_->page_cache();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void VolumeReader::prefetch_hint(PageId page_id) /*override*/
{
  BATT_UNTESTED_LINE();
  this->impl_->job_->prefetch_hint(page_id);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PinnedPage> VolumeReader::try_pin_cached_page(PageId page_id,
                                                       const PageLoadOptions& options) /*override*/
{
  BATT_UNTESTED_LINE();
  return this->impl_->job_->try_pin_cached_page(page_id, options);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PinnedPage> VolumeReader::load_page(PageId page_id,
                                             const PageLoadOptions& options) /*override*/
{
  BATT_UNTESTED_LINE();
  return this->impl_->job_->load_page(page_id, options);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
SlotRange VolumeReader::slot_range() const
{
  return this->impl_->read_lock_.slot_range();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
SlotReadLock VolumeReader::clone_lock() const
{
  return this->impl_->read_lock_.clone();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status VolumeReader::trim(slot_offset_type trim_upper_bound)
{
  BATT_CHECK(this->impl_->read_lock_);

  const SlotRange& old_slot_range = this->impl_->read_lock_.slot_range();

  const slot_offset_type new_lower_bound = slot_max(old_slot_range.lower_bound, trim_upper_bound);
  const slot_offset_type new_upper_bound = new_lower_bound + 1;

  SlotRange new_slot_range{
      .lower_bound = new_lower_bound,
      .upper_bound = new_upper_bound,
  };

  // Make sure the new locked range is non-empty!
  //
  if (new_slot_range.upper_bound == new_slot_range.lower_bound) {
    new_slot_range.upper_bound += 1;
  }

  BATT_CHECK(this->impl_->read_lock_);

  StatusOr<SlotReadLock> updated = this->impl_->volume_.trim_control_->update_lock(
      std::move(this->impl_->read_lock_), new_slot_range, "VolumeReader::trim");

  BATT_UNTESTED_COND(!updated.ok());
  BATT_REQUIRE_OK(updated);

  this->impl_->read_lock_ = std::move(*updated);
  BATT_CHECK(this->impl_->read_lock_);

  return OkStatus();
}

}  // namespace llfs
