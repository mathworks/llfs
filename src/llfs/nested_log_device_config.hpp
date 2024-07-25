//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_NESTED_LOG_DEVICE_CONFIG_HPP
#define LLFS_NESTED_LOG_DEVICE_CONFIG_HPP

#include <llfs/config.hpp>
//

#include <llfs/int_types.hpp>
#include <llfs/log_device_config.hpp>
#include <llfs/log_device_config2.hpp>
#include <llfs/optional.hpp>
#include <llfs/uuid.hpp>

#include <variant>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// These types are provided for the convenience of more complex configs that nest one or more
// LogDevice configs.
//

//+++++++++++-+-+--+----- --- -- -  -  -   -
//
struct CreateNewLogDevice {
  LogDeviceConfigOptions options;
};

inline bool operator==(const CreateNewLogDevice& l, const CreateNewLogDevice& r)
{
  return l.options == r.options;
}

inline bool operator!=(const CreateNewLogDevice& l, const CreateNewLogDevice& r)
{
  return !(l == r);
}

//+++++++++++-+-+--+----- --- -- -  -  -   -
//
struct CreateNewLogDeviceWithDefaultSize {
  Optional<boost::uuids::uuid> uuid;
  Optional<u16> pages_per_block_log2;
};

inline bool operator==(const CreateNewLogDeviceWithDefaultSize& l,
                       const CreateNewLogDeviceWithDefaultSize& r)
{
  return l.uuid == r.uuid  //
         && l.pages_per_block_log2 == r.pages_per_block_log2;
}

inline bool operator!=(const CreateNewLogDeviceWithDefaultSize& l,
                       const CreateNewLogDeviceWithDefaultSize& r)
{
  return !(l == r);
}

//+++++++++++-+-+--+----- --- -- -  -  -   -
//
struct LinkToExistingLogDevice {
  boost::uuids::uuid uuid;
};

inline bool operator==(const LinkToExistingLogDevice& l, const LinkToExistingLogDevice& r)
{
  return l.uuid == r.uuid;
}

inline bool operator!=(const LinkToExistingLogDevice& l, const LinkToExistingLogDevice& r)
{
  return !(l == r);
}

//+++++++++++-+-+--+----- --- -- -  -  -   -
//
struct CreateNewLogDevice2 {
  LogDeviceConfigOptions2 options;
};

inline bool operator==(const CreateNewLogDevice2& l, const CreateNewLogDevice2& r)
{
  return l.options == r.options;
}

inline bool operator!=(const CreateNewLogDevice2& l, const CreateNewLogDevice2& r)
{
  return !(l == r);
}

//+++++++++++-+-+--+----- --- -- -  -  -   -
//
struct CreateNewLogDevice2WithDefaultSize {
  Optional<boost::uuids::uuid> uuid;
  Optional<u16> device_page_size_log2;
  Optional<u16> data_alignment_log2;
};

inline bool operator==(const CreateNewLogDevice2WithDefaultSize& l,
                       const CreateNewLogDevice2WithDefaultSize& r)
{
  return l.uuid == r.uuid                                       //
         && l.device_page_size_log2 == r.device_page_size_log2  //
         && l.data_alignment_log2 == r.data_alignment_log2;
}

inline bool operator!=(const CreateNewLogDevice2WithDefaultSize& l,
                       const CreateNewLogDevice2WithDefaultSize& r)
{
  return !(l == r);
}

//+++++++++++-+-+--+----- --- -- -  -  -   -

using NestedLogDeviceConfig =
    std::variant<CreateNewLogDevice, CreateNewLogDeviceWithDefaultSize, LinkToExistingLogDevice,
                 CreateNewLogDevice2, CreateNewLogDevice2WithDefaultSize>;

}  //namespace llfs

#endif  // LLFS_NESTED_LOG_DEVICE_CONFIG_HPP
