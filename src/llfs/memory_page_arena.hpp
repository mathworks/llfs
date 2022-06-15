//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_MEMORY_PAGE_ARENA_HPP
#define LLFS_MEMORY_PAGE_ARENA_HPP

#include <llfs/int_types.hpp>
#include <llfs/page_arena.hpp>
#include <llfs/page_id_factory.hpp>
#include <llfs/page_size.hpp>

#include <batteries/async/task_scheduler.hpp>

#include <string>

namespace llfs {

PageArena make_memory_page_arena(batt::TaskScheduler& scheduler, isize n_pages, PageSize page_size,
                                 std::string&& name, page_device_id_int device_id);

}  // namespace llfs

#endif  // LLFS_MEMORY_PAGE_ARENA_HPP
