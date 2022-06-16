//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_FILTER_POLICY_HPP
#define LLFS_PAGE_FILTER_POLICY_HPP

#include <llfs/bloom_filter.hpp>

#include <batteries/optional.hpp>

#include <variant>

namespace llfs {

using PageFilterPolicy = std::variant<batt::NoneType, llfs::BloomFilterParams>;

}  // namespace llfs

#endif  // LLFS_PAGE_FILTER_POLICY_HPP
