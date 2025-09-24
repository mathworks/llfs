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
  /** \brief Describes the location and shape of this PageDevice within its file.
   */
  struct PhysicalLayout {
    using Self = PhysicalLayout;

    /** \brief Converts a packed page device config to a IoRingPageFileDevice::PhysicalLayout.
     */
    static Self from_packed_config(const FileOffsetPtr<PackedPageDeviceConfig>& config);

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    /** \brief The size of each page, in bytes.  Must be a power of 2.
     */
    PageSize page_size;

    /** \brief The number of pages in this device.
     */
    PageCount page_count;

    /** \brief The offset of the first page, relative to the start of the file.
     */
    FileOffset page_0_offset;

    /** \brief Must be equal to lg(this->page_size).
     */
    u16 page_size_log2;

    /** \brief Whether this page device is at the end of its containing file.
     */
    bool is_last_in_file;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit IoRingPageFileDevice(IoRing::File&& file,
                                const FileOffsetPtr<PackedPageDeviceConfig>& config) noexcept;

  /** \brief Creates a read-only, sharded view of the page device.
   */
  explicit IoRingPageFileDevice(page_device_id_int device_id, std::shared_ptr<IoRing::File>&& file,
                                const PhysicalLayout& physical_layout) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns a reference to the file object.
   */
  IoRing::File& file()
  {
    return this->file_;
  }

  /** \brief Returns the shared_ptr which manages the file object.
   */
  const std::shared_ptr<IoRing::File>& shared_file() const
  {
    return this->shared_file_;
  }

  /** \brief Returns the physical layout of this page device within its file.
   */
  const PhysicalLayout& layout() const
  {
    return this->physical_layout_;
  }

  /** \brief Creates a sharded view of the page device, used to load subsets of pages on
   * `shard_size`-aligned boundaries.
   */
  std::unique_ptr<PageDevice> make_sharded_view(page_device_id_int device_id,
                                                PageSize shard_size) override;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  PageIdFactory page_ids() override;

  PageSize page_size() override;

  bool is_last_in_file() const override;

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
                 usize page_buffer_size, usize n_read_so_far, ReadHandler&& handler);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  const bool is_read_only_;
  const bool is_sharded_view_;

  // The backing file for this page device.  Could be a flat file or a raw block device.
  //
  std::shared_ptr<IoRing::File> shared_file_;

  // Access to the file is done via this reference.
  //
  IoRing::File& file_;

  // The layout for this device.
  //
  PhysicalLayout physical_layout_;

  // Used to construct and parse PageIds for this device.
  //
  PageIdFactory page_ids_;
};

}  // namespace llfs

#endif  // LLFS_DISABLE_IO_URING
#endif  // LLFS_IORING_PAGE_FILE_DEVICE_HPP
