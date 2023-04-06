//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_SIMULATED_PAGE_DEVICE_HPP
#define LLFS_SIMULATED_PAGE_DEVICE_HPP

#include <llfs/config.hpp>
//
#include <llfs/page_device.hpp>

#include <memory>

namespace llfs {

class SimulatedPageDevice : public PageDevice
{
 public:
  class Impl;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit SimulatedPageDevice(std::shared_ptr<Impl>&& impl) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  PageIdFactory page_ids() override;

  PageSize page_size() override;

  //----- --- -- -  -  -   -
  // Write phase
  //
  StatusOr<std::shared_ptr<PageBuffer>> prepare(PageId page_id) override;

  void write(std::shared_ptr<const PageBuffer>&& page_buffer,
             PageDevice::WriteHandler&& handler) override;

  //----- --- -- -  -  -   -
  // Read phase
  //
  void read(PageId id, PageDevice::ReadHandler&& handler) override;

  //----- --- -- -  -  -   -
  // Delete phase
  //
  void drop(PageId id, PageDevice::WriteHandler&& handler) override;

 private:
  std::shared_ptr<Impl> impl_;
};

}  //namespace llfs

#endif  // LLFS_SIMULATED_PAGE_DEVICE_HPP
