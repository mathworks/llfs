//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PACKED_SEQ_HPP
#define LLFS_PACKED_SEQ_HPP

#include <llfs/define_packed_type.hpp>
#include <llfs/packed_array.hpp>
#include <llfs/seq.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename BoxedSeqT,
          typename = std::enable_if_t<batt::IsBoxedSeq<std::decay_t<BoxedSeqT>>::value>>
usize packed_sizeof(BoxedSeqT&& seq)
{
  return packed_array_size(BATT_SINK(seq) | seq::count(),
                           batt::StaticType<PackedTypeFor<SeqItem<BoxedSeqT>>>{});
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename BoxedSeqT, typename Dst,
          typename = std::enable_if_t<batt::IsBoxedSeq<std::decay_t<BoxedSeqT>>::value>>
PackedArray<PackedTypeFor<SeqItem<BoxedSeqT>>>* pack_object_to(
    BoxedSeqT&& obj, PackedArray<PackedTypeFor<SeqItem<BoxedSeqT>>>* packed, Dst* dst)
{
  bool ok = true;

  packed->item_count = 0;

  BATT_SINK(obj) | seq::for_each([&](auto&& item) {
    auto* packed_item = pack_object(BATT_FORWARD(item), dst);
    if (!packed_item) {
      ok = false;
      return seq::LoopControl::kBreak;
    }
    packed->item_count += 1;
    return seq::LoopControl::kContinue;
  });

  if (!ok) {
    return nullptr;
  }

  return packed;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename T, typename Src,
          typename ItemT = batt::RemoveStatusOr<decltype(unpack_object(std::declval<const T&>(),
                                                                       std::declval<Src*>()))>>
StatusOr<BoxedSeq<ItemT>> unpack_object(const PackedArray<T>& packed, Src* src)
{
  if ((const void*)src->buffer_end() < (const void*)packed.end()) {
    return {batt::StatusCode::kOutOfRange};
  }
  return as_seq(packed)                        //
         | seq::map([src](const T& item) {     //
             return unpack_object(item, src);  //
           })                                  //
         | seq::status_ok()                    //
         | seq::boxed();
}

}  // namespace llfs

namespace batt {

template <typename T>
LLFS_DEFINE_PACKED_TYPE_FOR(::llfs::BoxedSeq<T>, ::llfs::PackedArray<::llfs::PackedTypeFor<T>>);

}

#endif  // LLFS_PACKED_SEQ_HPP
