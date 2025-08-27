//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_LOADER_DECL_HPP
#define LLFS_PAGE_LOADER_DECL_HPP

#include <llfs/pinned_page.hpp>

namespace llfs {

template <typename PinnedPageParamT>
class BasicPageLoader;

using PageLoader = BasicPageLoader<PinnedPage>;

}  //namespace llfs

#endif  // LLFS_PAGE_LOADER_DECL_HPP
