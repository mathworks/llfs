//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/file_log_driver.hpp>
//

#include <llfs/logging.hpp>

#include <boost/algorithm/string/predicate.hpp>

#include <cctype>
#include <charconv>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
fs::path FileLogDriver::Location::config_file_path() const
{
  return this->parent_dir_ / std::string(Location::config_file_name());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
fs::path FileLogDriver::Location::active_segment_file_path() const
{
  return this->parent_dir_ / std::string(Location::active_segment_file_name());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::string FileLogDriver::Location::segment_file_name_from_slot_range(
    const SlotRange& slot_range) const
{
  std::ostringstream oss;
  oss << (this->parent_dir_ / Location::segment_file_prefix()).string();
  oss << std::hex << std::setw(10) << std::setfill('0') << slot_range.lower_bound << "." << std::hex
      << std::setw(1) << slot_range.size() << Location::segment_ext();
  return std::move(oss).str();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<SlotRange> FileLogDriver::Location::slot_range_from_segment_file_name(
    const std::string& name) const
{
  if (!boost::algorithm::ends_with(name, Location::segment_ext()) ||
      name.length() < Location::segment_ext().length() + 1) {
    return None;
  }

  const auto offset_end = name.length() - Location::segment_ext().length();
  const auto segment_end = name.rfind('.', offset_end - 1);
  if (segment_end == std::string::npos) {
    return None;
  }
  auto segment_begin = segment_end;
  while (segment_begin > 0) {
    --segment_begin;
    if (!std::isxdigit(name[segment_begin])) {
      ++segment_begin;
      break;
    }
  }
  while (name[segment_begin] == '0' && segment_begin + 1 < segment_end) {
    ++segment_begin;
  }
  const auto offset_begin = segment_end + 1;

  if (offset_begin >= offset_end) {
    return None;
  }

  slot_offset_type range_begin = 0;
  {
    std::from_chars_result result = std::from_chars(
        name.c_str() + segment_begin, name.c_str() + segment_end, range_begin, /*base=*/16);
    if (result.ec != std::errc()) {
      return None;
    }
  }

  slot_offset_type range_size = 0;
  {
    std::from_chars_result result = std::from_chars(
        name.c_str() + offset_begin, name.c_str() + offset_end, range_size, /*base=*/16);
    if (result.ec != std::errc()) {
      return None;
    }
  }

  return SlotRange{
      .lower_bound = range_begin,
      .upper_bound = range_begin + range_size,
  };
}

}  // namespace llfs
