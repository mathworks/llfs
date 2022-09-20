//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_IORING_PAGE_FILE_DEVICE_HPP
#define LLFS_IORING_PAGE_FILE_DEVICE_HPP

#include <llfs/config.hpp>

#ifndef LLFS_DISABLE_IO_URING

#include <llfs/file_offset_ptr.hpp>
#include <llfs/ioring.hpp>
#include <llfs/page_device.hpp>
#include <llfs/page_device_config.hpp>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class IoRingPageFileDevice : public PageDevice
{
 public:
  explicit IoRingPageFileDevice(IoRing::File&& file,
                                const FileOffsetPtr<PackedPageDeviceConfig>& config) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  PageIdFactory page_ids() override;

  PageSize page_size() override;

  StatusOr<std::shared_ptr<PageBuffer>> prepare(PageId page_id) override;

  void write(std::shared_ptr<const PageBuffer>&& page_buffer, WriteHandler&& handler) override;

  void read(PageId id, ReadHandler&& handler) override;

  void drop(PageId id, WriteHandler&& handler) override;

 private:
  //+++++++++++-+-+--+----- --- -- -  -  -   -
  StatusOr<u64> get_physical_page(PageId page_id) const;

  StatusOr<i64> get_file_offset_of_page(PageId page_id) const;

  void write_some(i64 page_offset_in_file, std::shared_ptr<const PageBuffer>&& page_buffer,
                  ConstBuffer remaining_data, WriteHandler&& handler);

  void read_some(PageId page_id, i64 page_offset_in_file, std::shared_ptr<PageBuffer>&& page_buffer,
                 usize n_read_so_far, ReadHandler&& handler);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // The backing file for this page device.  Could be a flat file or a raw block device.
  //
  IoRing::File file_;

  // The config for this device.
  //
  FileOffsetPtr<PackedPageDeviceConfig> config_;

  // Used to construct and parse PageIds for this device.
  //
  PageIdFactory page_ids_;
};

}  // namespace llfs

#endif  // LLFS_DISABLE_IO_URING
#endif  // LLFS_IORING_PAGE_FILE_DEVICE_HPP
