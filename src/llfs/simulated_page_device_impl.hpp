//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -
#pragma once
#ifndef LLFS_SIMULATED_PAGE_DEVICE_IMPL_HPP
#define LLFS_SIMULATED_PAGE_DEVICE_IMPL_HPP

#include <llfs/config.hpp>
//
#include <llfs/page_id_factory.hpp>
#include <llfs/page_size.hpp>
#include <llfs/simulated_page_device.hpp>
#include <llfs/simulated_storage_object.hpp>
#include <llfs/storage_simulation.hpp>

#include <batteries/async/mutex.hpp>
#include <batteries/async/watch.hpp>
#include <batteries/checked_cast.hpp>
#include <batteries/small_vec.hpp>

#include <memory>
#include <type_traits>
#include <unordered_map>

namespace llfs {

class StorageSimulation;

/** \brief The internal representation of a simulated page device.
 *
 * A single SimulatedPageDevice::Impl object persists across simulated crashes and recoveries; each
 * recovery cycle produces a new SimulatedPageDevice, however.  Because of this, the Impl keeps
 * track of when the last recovery took place (in simulation steps); that way it can guarantee that
 * any attempts by an _older_ SimulatedPageDevice to initiate or complete some I/O operation is
 * simply ignored (in the scenario we are simulating, this is appropriate because the old page
 * device would have died with the OS process that originally created it).  If PageDevice object ==
 * SimulatedPageDevice object, then SimulatedPageDevice::Impl == (the nonvolatile storage device
 * accessed by the PageDevice object).
 */
class SimulatedPageDevice::Impl : public SimulatedStorageObject
{
 public:
  /** \brief The size of a simulated minimum atomically writable storage unit.
   */
  static constexpr usize kDataBlockSize = kDirectIOBlockSize;

  /** \brief The alignment of a DataBlock; this is set to kDirectIOBlockAlign to match the
   * requirements of direct (raw) I/O on Linux.
   */
  static constexpr usize kDataBlockAlign = kDirectIOBlockAlign;

  /** \brief Buffer type used to store simulated atomic writes to storage.
   */
  using DataBlock = std::aligned_storage_t<kDataBlockSize, kDataBlockAlign>;

  // Forward-declaration (see below).
  //
  struct MultiBlockOp;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Creates a new simulated page device impl.
   *
   * \param simulation the StorageSimulation managing this device
   * \param name a unique name for the device in the context of the simulation
   * \param page_size the uniform page size for the device
   * \param page_count how many pages the device contains
   * \param device_id the unique integer for this device, within the simulation (used by the
   * PageCache to route operations to the correct PageArena (PageDevice/PageAllocator))
   */
  explicit Impl(StorageSimulation& simulation, const std::string& name, PageSize page_size,
                PageCount page_count, page_device_id_int device_id) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Logs a simulation event.
   *
   * All arguments are ostream-inserted to the log message.
   */
  template <typename... Args>
  void log_event(Args&&... args) const noexcept
  {
    this->simulation_.log_event("SimulatedPageDevice{", batt::c_str_literal(this->name_), "} ",
                                BATT_FORWARD(args)...);
  }

  /** \brief Invalidates any ongoing simulated I/O operations and updates this object's
   * last-recovered-at simulation step.
   */
  void crash_and_recover(u64 step) override;

  /** \brief Returns a reference to the simulation object managing this.
   */
  StorageSimulation& simulation() const noexcept
  {
    return this->simulation_;
  }

  /** \brief Returns true if all blocks for the given page are present, false if none are,
   * batt::StatusCode::kUnknown otherwise.
   *
   * A successful call to write for the given page will result in this function returning true
   * afterwards.  Similarly, calling drop on the page will cause it to return false.  If these
   * operations succeed in writing some blocks but not others (because the simulation injects a
   * failure), this may leave the page in an unknown/inconsistent state.
   */
  StatusOr<bool> has_data_for_page_id(PageId page_id) const noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns a copy of the PageIdFactory for this device.
   */
  PageIdFactory page_ids()
  {
    return this->page_id_factory_;
  }

  /** \brief Returns the page size in bytes.
   */
  PageSize page_size()
  {
    return this->page_size_;
  }

  /** \brief Implements PageDevice::prepare.
   *
   * If device_create_step is older than the last `step` value that was passed into
   * `this->crash_and_recover`, the prepare operation fails returning batt::StatusCode::kClosed.
   */
  StatusOr<std::shared_ptr<PageBuffer>> prepare(u32 device_create_step, PageId page_id);

  /** \brief Implements PageDevice::write.
   *
   * If device_create_step is older than the last `step` value passed into
   * `this->crash_and_recover`, the write operation fails, invoking the handler with status
   * batt::StatusCode::kClosed.
   */
  void write(u32 device_create_step, std::shared_ptr<const PageBuffer>&& page_buffer,
             PageDevice::WriteHandler&& handler);

  /** \brief Implements PageDevice::read.
   *
   * If device_create_step is older than the last `step` value passed into
   * `this->crash_and_recover`, the read operation fails, invoking the handler with status
   * batt::StatusCode::kClosed.
   */
  void read(u32 device_create_step, PageId page_id, PageDevice::ReadHandler&& handler);

  /** \brief Implements PageDevice::drop.
   *
   * If device_create_step is older than the last `step` value passed into
   * `this->crash_and_recover`, the drop operation fails, invoking the handler with status
   * batt::StatusCode::kClosed.
   */
  void drop(u32 device_create_step, PageId page_id, PageDevice::WriteHandler&& handler);

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  /** \brief Calculates and returns the physical page index for the given page_id by using the
   * PageIdFactory to mask out device id and generation.
   */
  i64 get_physical_page(PageId page_id) const noexcept;

  /** \brief Checks the passed page index to make sure it is in range for the device; if it is,
   * returns true, else returns false and invokes `handler` with batt::StatusCode::kInvalidArgument.
   */
  template <typename Handler /* = ReadHandler or WriteHandler */>
  [[no_discard]] bool validate_physical_page_async(i64 physical_page, Handler&& handler);

  /** \brief Invokes `fn` for each block index comprising the given page.  The first block index
   * (block_0) is also passed to each call as the first argument, so the function can calculate the
   * relative offset of the block (in units of kDataBlockSize) within the page.
   */
  template <typename Fn = void(i64 block_0, i64 block_i)>
  void for_each_page_block(i64 physical_page, Fn&& fn) const noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  StorageSimulation& simulation_;

  const std::string name_;

  const PageSize page_size_;

  const PageCount page_count_;

  const page_device_id_int device_id_;

  const i64 blocks_per_page_ = BATT_CHECKED_CAST(i64, this->page_size_ / kDataBlockSize);

  PageIdFactory page_id_factory_{this->page_count_, this->device_id_};

  batt::Watch<u64> latest_recovery_step_{0};

  batt::Mutex<std::unordered_map<i64, std::unique_ptr<DataBlock>>> blocks_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

/** \brief Tracks the fan-out/in of operations on all the blocks comprising a page.
 */
class SimulatedPageDevice::Impl::MultiBlockOp : public batt::RefCounted<MultiBlockOp>
{
 public:
  explicit MultiBlockOp(Impl& impl) noexcept;

  /** \brief Sets one part of the overall result for the op.
   *
   * `relative_block_i` must be in the range [0, impl.blocks_per_page_).  Each block index may only
   * be passed to a SINGLE `set_block_result` call per MultiBlockOp object; i.e., a block's result
   * may only be set ONCE.
   *
   * Each call to set_block_result causes the internal `pending_blocks_` counter to decrease by one;
   * when it reaches zero, handlers passed to `this->on_completion` (either in the past or future)
   * will be invoked with a combination of all the Status values.  If any Status is an error, then
   * an error will be passed to the handlers.  If all are OkStatus(), then OkStatus() is passed to
   * the handler.
   */
  void set_block_result(i64 relative_block_i, const batt::Status& status);

  /** \brief Runs the passed handler as soon as all block results have been set.
   */
  void on_completion(PageDevice::WriteHandler&& handler);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

 private:
  static constexpr usize kPreAllocBlocksPerPage = 4;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Reference to the Impl that created this object.
   */
  Impl& impl_;

  /** \brief Tracks the number of blocks for which
   */
  batt::Watch<i64> pending_blocks_;
  batt::SmallVec<batt::Status, kPreAllocBlocksPerPage> block_status_;
};

}  //namespace llfs

#endif  // LLFS_SIMULATED_PAGE_DEVICE_IMPL_HPP
