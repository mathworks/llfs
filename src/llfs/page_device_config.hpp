//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_DEVICE_CONFIG_HPP
#define LLFS_PAGE_DEVICE_CONFIG_HPP

#include <llfs/config.hpp>
#include <llfs/int_types.hpp>
#include <llfs/ioring_file_runtime_options.hpp>
#include <llfs/packed_config.hpp>
#include <llfs/page_id_factory.hpp>
#include <llfs/page_size.hpp>
#include <llfs/simple_packed_type.hpp>
#include <llfs/storage_context.hpp>
#include <llfs/storage_file_builder.hpp>

#include <batteries/static_assert.hpp>

#include <boost/uuid/uuid.hpp>

#include <ostream>
#include <variant>

namespace llfs {

class PageDevice;
struct PackedPageDeviceConfig;
struct PageDeviceConfigOptions;

//+++++++++++-+-+--+----- --- -- -  -  -   -

Status configure_storage_object(StorageFileBuilder::Transaction&,
                                FileOffsetPtr<PackedPageDeviceConfig&> p_config,
                                const PageDeviceConfigOptions& options);

#ifndef LLFS_DISABLE_IO_URING

StatusOr<std::unique_ptr<PageDevice>> recover_storage_object(
    const batt::SharedPtr<StorageContext>& storage_context, const std::string& file_name,
    const FileOffsetPtr<const PackedPageDeviceConfig&>& p_config,
    const IoRingFileRuntimeOptions& file_options);

#endif  // LLFS_DISABLE_IO_URING

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct PageDeviceConfigOptions {
  using PackedConfigType = PackedPageDeviceConfig;

  // The unique identifier for the device; if None, a random UUID is generated.
  //
  Optional<boost::uuids::uuid> uuid;

  // The device id for this device; if None, this will be set to the lowest unused id in the storage
  // context where this device is configured.
  //
  Optional<page_device_id_int> device_id;

  // The number of pages in this device. If max_page_count is specified, the initial number of pages
  // in this device.
  //
  PageCount page_count;

  // The maximum number of pages that can be allocated on this device.
  //
  Optional<PageCount> max_page_count;

  // log2(the page size of the device)
  //
  PageSizeLog2 page_size_log2;

  // Information on if this PageDevice is placed at the end of the llfs file.
  // Allows for dynamic growth of the Page Device and llfs file.
  //
  Optional<bool> last_in_file;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // Returns the page size in bytes.
  //
  u64 page_size() const noexcept
  {
    return u64{1} << this->page_size_log2;
  }

  // Sets the page size (bytes).  `n` must be a power of 2, >=512.
  //
  void page_size(u64 n)
  {
    this->page_size_log2 = PageSizeLog2(batt::log2_ceil(n));
    BATT_CHECK_EQ(this->page_size(), n);
  }
};

inline bool operator==(const PageDeviceConfigOptions& l, const PageDeviceConfigOptions& r)
{
  return l.uuid == r.uuid                         //
         && l.device_id == r.device_id            //
         && l.page_count == r.page_count          //
         && l.page_size_log2 == r.page_size_log2  //
         && l.last_in_file == r.last_in_file      //
         && l.max_page_count == r.max_page_count;
}

inline bool operator!=(const PageDeviceConfigOptions& l, const PageDeviceConfigOptions& r)
{
  return !(l == r);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// These types are provided for the convenience of more complex configs that nest one or more
// LogDevice configs.
//

struct CreateNewPageDevice {
  PageDeviceConfigOptions options;
};

inline bool operator==(const CreateNewPageDevice& l, const CreateNewPageDevice& r)
{
  return l.options == r.options;
}

inline bool operator!=(const CreateNewPageDevice& l, const CreateNewPageDevice& r)
{
  return !(l == r);
}

//+++++++++++-+-+--+----- --- -- -  -  -   -

struct LinkToExistingPageDevice {
  boost::uuids::uuid uuid;
  page_device_id_int device_id;
};

inline bool operator==(const LinkToExistingPageDevice& l, const LinkToExistingPageDevice& r)
{
  return l.uuid == r.uuid  //
         && l.device_id == r.device_id;
}

inline bool operator!=(const LinkToExistingPageDevice& l, const LinkToExistingPageDevice& r)
{
  return !(l == r);
}

//+++++++++++-+-+--+----- --- -- -  -  -   -

struct LinkToNewPageDevice {
};

inline bool operator==(const LinkToNewPageDevice&, const LinkToNewPageDevice&)
{
  return false;
}

inline bool operator!=(const LinkToNewPageDevice& l, const LinkToNewPageDevice& r)
{
  return !(l == r);
}

//+++++++++++-+-+--+----- --- -- -  -  -   -

using NestedPageDeviceConfig =
    std::variant<CreateNewPageDevice, LinkToExistingPageDevice, LinkToNewPageDevice>;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct PackedPageDeviceConfig : PackedConfigSlotHeader {
  static constexpr usize kSize = PackedConfigSlot::kSize;

  static constexpr u8 LAST_IN_FILE_MASK = 0b00000001;

  // The offset in bytes of the first page, relative to this structure.
  //
  little_i64 page_0_offset;

  // The default short device_id for this device.
  //
  little_u64 device_id;

  // Number of pages physically addresssable on this PageDevice.
  //
  little_i64 page_count;

  // The log2 of the page size in bytes.
  //
  little_u16 page_size_log2;

  // Bit mask for PageDevice options
  //
  little_u8 options_mask;

  // Reserved for future use.
  //
  little_u8 reserved_[17];

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  usize page_size() const
  {
    return usize{1} << this->page_size_log2.value();
  }

  // Set whether or not this PageDevice is the last object in an llfs file.
  //
  PackedPageDeviceConfig set_last_in_file(bool last_in_file)
  {
    if (last_in_file) {
      this->options_mask |= PackedPageDeviceConfig::LAST_IN_FILE_MASK;
    } else {
      this->options_mask &= ~PackedPageDeviceConfig::LAST_IN_FILE_MASK;
    }
    return *this;
  }

  // Get whether or not this PageDevice is the last object in an llfs file.
  //
  bool is_last_in_file() const
  {
    return this->options_mask & PackedPageDeviceConfig::LAST_IN_FILE_MASK;
  }
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedPageDeviceConfig), PackedPageDeviceConfig::kSize);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

template <>
struct PackedConfigTagFor<PackedPageDeviceConfig> {
  static constexpr u32 value = PackedConfigSlot::Tag::kPageDevice;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

std::ostream& operator<<(std::ostream& out, const PackedPageDeviceConfig& t);

}  // namespace llfs

#endif  // LLFS_PACKED_PAGE_DEVICE_CONFIG_HPP
