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

#include <memory>
#include <type_traits>
#include <unordered_map>

namespace llfs {

class StorageSimulation;

class SimulatedPageDevice::Impl : public SimulatedStorageObject
{
 public:
  static constexpr usize kDataBlockSize = 512;
  static constexpr usize kDataBlockAlign = 512;

  using DataBlock = std::aligned_storage_t<kDataBlockSize, kDataBlockAlign>;

  struct MultiBlockOp;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit Impl(StorageSimulation& simulation, const std::string& name, PageSize page_size,
                PageCount page_count, page_device_id_int device_id) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Logs a simulation event.
   */
  template <typename... Args>
  void log_event(Args&&... args) const noexcept
  {
    this->simulation_.log_event("SimulatedPageDevice{", batt::c_str_literal(this->name_), "} ",
                                BATT_FORWARD(args)...);
  }

  void crash_and_recover(u64 step) override;

  StorageSimulation& simulation() const noexcept
  {
    return this->simulation_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  PageIdFactory page_ids()
  {
    return this->page_id_factory_;
  }

  PageSize page_size()
  {
    return this->page_size_;
  }

  StatusOr<std::shared_ptr<PageBuffer>> prepare(u32 device_create_step, PageId page_id);

  void write(u32 device_create_step, std::shared_ptr<const PageBuffer>&& page_buffer,
             PageDevice::WriteHandler&& handler);

  void read(u32 device_create_step, PageId page_id, PageDevice::ReadHandler&& handler);

  void drop(u32 device_create_step, PageId page_id, PageDevice::WriteHandler&& handler);

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  i64 get_physical_page(PageId page_id) const noexcept;

  template <typename Handler /* = ReadHandler or WriteHandler */>
  [[no_discard]] bool validate_physical_page_async(i64 physical_page, Handler&& handler);

  template <typename Fn = void(i64 block_0, i64 block_i)>
  void for_each_page_block(i64 physical_page, Fn&& fn);

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

struct SimulatedPageDevice::Impl::MultiBlockOp : batt::RefCounted<MultiBlockOp> {
  Impl& impl;
  batt::Watch<i64> pending_blocks;
  std::vector<batt::Status> block_status;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit MultiBlockOp(Impl& impl, i64 n_blocks) noexcept;

  void set_block_result(i64 block_i, const batt::Status& status);

  void on_completion(PageDevice::WriteHandler&& handler);
};

}  //namespace llfs

#endif  // LLFS_SIMULATED_PAGE_DEVICE_IMPL_HPP
