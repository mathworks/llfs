//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_ARRAY_PACKER_HPP
#define LLFS_ARRAY_PACKER_HPP

#include <llfs/data_layout.hpp>
#include <llfs/packed_array.hpp>
#include <llfs/seq.hpp>

namespace llfs {

template <typename T, typename Dst>
class BasicArrayPacker
{
 public:
  BasicArrayPacker(PackedArray<T>* array, Dst* dst) noexcept : array_{array}, dst_{dst}
  {
  }

  template <typename Src>
  bool pack_item(Src&& src)
  {
    T* item = dst_->template pack_record<T>();
    if (!item) {
      return false;
    }
    if (!pack_object_to(BATT_FORWARD(src), item, dst_)) {
      return false;
    }
    array_->item_count += 1;
    return true;
  }

  bool pack_item(const std::string_view& s)
  {
    static_assert(std::is_same_v<T, PackedBytes>, "");

    if (!this->dst_->pack_string(s)) {
      return false;
    }
    this->array_->item_count += 1;
    return true;
  }

  bool pack_item(u32 i)
  {
    static_assert(std::is_same_v<T, little_u32>, "");

    if (!this->dst_->pack_u32(i)) {
      return false;
    }
    this->array_->item_count += 1;
    return true;
  }

  template <typename Seq>
  bool pack_seq(Seq&& seq)
  {
    bool ok = true;
    BATT_FORWARD(seq) | seq::for_each([&ok, this](auto&& item) {
      if (!this->pack_item(BATT_FORWARD(item))) {
        ok = false;
        return seq::LoopControl::kBreak;
      }
      return seq::LoopControl::kContinue;
    });
    return ok;
  }

  PackedArray<T>* finish()
  {
    auto* final_array = array_;
    array_ = nullptr;
    dst_ = nullptr;
    return final_array;
  }

 private:
  PackedArray<T>* array_;
  Dst* dst_;
};

}  // namespace llfs

#endif  // LLFS_ARRAY_PACKER_HPP
