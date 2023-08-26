//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_MEMORY_LOG_DEVICE_HPP
#define LLFS_MEMORY_LOG_DEVICE_HPP

#include <llfs/basic_ring_buffer_log_device.hpp>
#include <llfs/log_device.hpp>
#include <llfs/ring_buffer.hpp>

#include <batteries/async/watch.hpp>

namespace llfs {

class MemoryLogDeviceFactory;

class MemoryLogStorageDriver /*Impl*/
{
 public:
  explicit MemoryLogStorageDriver(LogStorageDriverContext&) noexcept
  {
    // Verify default setting for auto-flush.
    //
    BATT_CHECK(this->is_auto_flush());
  }

  //----

  Status set_trim_pos(slot_offset_type trim_pos)
  {
    this->trim_pos_.set_value(trim_pos);

    return OkStatus();
  }

  slot_offset_type get_trim_pos() const
  {
    return this->trim_pos_.get_value();
  }

  StatusOr<slot_offset_type> await_trim_pos(slot_offset_type min_offset)
  {
    return this->trim_pos_.await_true([&](slot_offset_type latest_pos) {
      return !slot_less_than(latest_pos, min_offset);
    });
  }

  //----

  slot_offset_type get_flush_pos() const
  {
    return this->flush_pos_->get_value();
  }

  StatusOr<slot_offset_type> await_flush_pos(slot_offset_type min_offset)
  {
    return await_slot_offset(min_offset, *this->flush_pos_);
  }

  //----

  Status set_commit_pos(slot_offset_type commit_pos)
  {
    this->commit_pos_.set_value(commit_pos);
    return OkStatus();
  }

  slot_offset_type get_commit_pos() const
  {
    return this->commit_pos_.get_value();
  }

  StatusOr<slot_offset_type> await_commit_pos(slot_offset_type min_offset)
  {
    return await_slot_offset(min_offset, this->commit_pos_);
  }

  //----

  Status close()
  {
    this->trim_pos_.close();
    this->manual_flush_pos_.close();
    this->commit_pos_.close();

    return OkStatus();
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns the current auto-flush status; if true, then the flush and commit pos are
   * automatically kept in sync.  Otherwise the flush_pos must be manually advanced via one of:
   *
   *   - sync_flush_pos()
   *   - advance_flush_pos(isize delta)
   *   - flush_up_to_offset(slot_offset_type offset)
   */
  bool is_auto_flush() const noexcept;

  /** \brief Set the auto-flush mode status.
   * \see is_auto_flush
   */
  void set_auto_flush(bool on) noexcept;

  /** \brief Sets the manual flush pos to match the current commit pos; this only synchronizes
   * *once*.  To keep the flush and commit pos in sync, use `set_auto_flush(true)`.
   */
  void sync_flush_pos() noexcept;

  /** \brief Returns the current delta (in bytes) between the flush_pos and commit_pos.
   */
  usize unflushed_size() noexcept;

  /** \brief Advances the flush pos by at most `offset_delta` bytes.  This function automatically
   * clamps the passed offset so it is never negative and so that the flush pos never goes past the
   * commit pos.
   *
   * \return The actual number of bytes advanced.
   */
  isize advance_flush_pos(isize offset_delta) noexcept;

  /** \brief Brings the flush pos up to the specified offset.  If flush pos is already at or beyond
   * offset, this function has no effect.  If offset is beyond the current commit pos, then this
   * function has the same effect as this->sync_flush_pos().
   */
  slot_offset_type flush_up_to_offset(slot_offset_type offset) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

 private:
  friend class LogTruncateAccess;

  void truncate(slot_offset_type truncate_pos)
  {
    this->commit_pos_.set_value(truncate_pos);
  }

  batt::Watch<slot_offset_type> trim_pos_{0};
  batt::Watch<slot_offset_type> manual_flush_pos_{0};
  batt::Watch<slot_offset_type> commit_pos_{0};
  batt::Watch<slot_offset_type>* flush_pos_ = &this->commit_pos_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief An emphemeral LogDevice that stores data in memory only.
 *
 * The commit pos and flush pos are always in sync, so there is no difference between
 * LogReadMode::kSpeculative and LogReadMode::kDurable for this log device type.
 */
class MemoryLogDevice : public BasicRingBufferLogDevice</*Impl=*/MemoryLogStorageDriver>
{
 public:
  explicit MemoryLogDevice(usize size) noexcept;

  /** \brief Equivalent to constructing a MemoryLogDevice with `size` and then calling
   * `restore_snapshot(snapshot, mode)`.
   */
  explicit MemoryLogDevice(usize size, const LogDeviceSnapshot& snapshot,
                           LogReadMode mode) noexcept;

  /** \brief Restores the state of the log device from the passed snapshot.
   *
   * If `mode` is LogReadMode::kDurable, the commit pos is set to the flush pos, emulating the
   * effect of post-crash recovery.
   *
   * If `mode` is LogReadMode::kSpeculative, then the saved commit pos from the snapshot is used to
   * set the commit pos of this device.
   */
  void restore_snapshot(const LogDeviceSnapshot& snapshot, LogReadMode mode);
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief A factory that produces MemoryLogDevice instances of the given size.
 */
class MemoryLogDeviceFactory : public LogDeviceFactory
{
 public:
  explicit MemoryLogDeviceFactory(slot_offset_type size) noexcept;

  StatusOr<std::unique_ptr<LogDevice>> open_log_device(const LogScanFn& scan_fn) override;

 private:
  slot_offset_type size_;
};

}  // namespace llfs

#endif  // LLFS_MEMORY_LOG_DEVICE_HPP
