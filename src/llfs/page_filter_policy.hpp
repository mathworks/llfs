//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef TURTLE_DB_PAGE_FILTER_POLICY_HPP
#define TURTLE_DB_PAGE_FILTER_POLICY_HPP

#include <llfs/bloom_filter.hpp>

#include <turtle/util/empty.hpp>

namespace turtle_db {

using PageFilterPolicy = std::variant<Empty, llfs::BloomFilterParams>;

}  // namespace turtle_db

#endif  // TURTLE_DB_PAGE_FILTER_POLICY_HPP
