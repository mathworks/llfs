//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_READER_HPP
#define LLFS_PAGE_READER_HPP

#include <llfs/page_buffer.hpp>
#include <llfs/page_view.hpp>
#include <llfs/status.hpp>

#include <functional>
#include <memory>

namespace llfs {

// Parses the layout of a PageBuffer containing raw data, producing a PageView.
//
using PageReader =
    std::function<StatusOr<std::shared_ptr<const PageView>>(std::shared_ptr<const PageBuffer>)>;

}  // namespace llfs

#endif  // LLFS_PAGE_READER_HPP
