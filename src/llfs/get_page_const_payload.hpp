//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once

#include <llfs/buffer.hpp>
#include <llfs/page_buffer.hpp>
#include <llfs/page_view.hpp>
#include <llfs/pinned_page.hpp>

#include <memory>

namespace llfs {

inline ConstBuffer get_page_const_payload(const PinnedPage& pinned_page) noexcept
{
  return pinned_page.const_payload();
}

inline ConstBuffer get_page_const_payload(const PageBuffer& page_buffer) noexcept
{
  return page_buffer.const_payload();
}

inline ConstBuffer get_page_const_payload(
    const std::shared_ptr<const PageBuffer>& p_page_buffer) noexcept
{
  return get_page_const_payload(*p_page_buffer);
}

inline ConstBuffer get_page_const_payload(const PageView& page_view) noexcept
{
  return page_view.const_payload();
}

inline ConstBuffer get_page_const_payload(const ConstBuffer& buffer) noexcept
{
  return buffer;
}

}  //namespace llfs
