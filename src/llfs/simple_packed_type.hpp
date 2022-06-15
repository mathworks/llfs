//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_SIMPLE_PACKED_TYPE_HPP
#define LLFS_SIMPLE_PACKED_TYPE_HPP

#include <llfs/data_packer.hpp>
#include <llfs/data_reader.hpp>
#include <llfs/define_packed_type.hpp>
#include <llfs/int_types.hpp>
#include <llfs/status.hpp>

#include <batteries/type_traits.hpp>

#define LLFS_SIMPLE_PACKED_TYPE(type)                                                              \
  inline ::llfs::usize packed_sizeof(const type&)                                                  \
  {                                                                                                \
    return ::llfs::packed_sizeof(::batt::StaticType<type>{});                                      \
  }                                                                                                \
                                                                                                   \
  inline type* pack_object_to(const type& from, type* to, ::llfs::DataPacker* /*dst*/)             \
  {                                                                                                \
    *to = from;                                                                                    \
    return to;                                                                                     \
  }                                                                                                \
                                                                                                   \
  inline ::llfs::StatusOr<type> unpack_object(const type& obj, ::llfs::DataReader*)                \
  {                                                                                                \
    return obj;                                                                                    \
  }                                                                                                \
                                                                                                   \
  LLFS_DEFINE_PACKED_TYPE_FOR(type, type)

#endif  // LLFS_SIMPLE_PACKED_TYPE_HPP
