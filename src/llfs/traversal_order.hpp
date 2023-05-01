//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_TRAVERSAL_ORDER_HPP
#define LLFS_TRAVERSAL_ORDER_HPP

#include <llfs/config.hpp>
//

#include <llfs/int_types.hpp>

#include <algorithm>
#include <deque>
#include <vector>

namespace llfs {

template <typename T>
struct TraversalItem {
  T item;
  usize insert_order;
  i32 insert_depth;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

template <typename T>
class BreadthFirstOrder
{
 public:
  using Item = TraversalItem<T>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  bool empty() const noexcept
  {
    return this->queue_.empty();
  }

  template <typename... Args>
  void push(Args&&... args)
  {
    this->queue_.emplace_back(Item{
        T{BATT_FORWARD(args)...},
        this->insert_count_,
        this->current_depth_ + 1,
    });
    this->insert_count_ += 1;
  }

  T pop()
  {
    Item& next = this->queue_.front();
    this->current_depth_ = next.insert_depth;

    T item = std::move(next.item);

    this->queue_.pop_front();

    return item;
  }

 private:
  usize insert_count_ = 0;
  i32 current_depth_ = -1;
  std::deque<Item> queue_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

template <typename T>
class VanEmdeBoasOrder
{
 public:
  using Item = TraversalItem<T>;

  static i32 get_priority(i32 depth)
  {
    if (depth == 0) {
      return 64;
    }

    // Find the position of the most significant bit that differs between depth and parent_depth
    // (depth - 1); the more leading zeroes (i.e., the lower that bit's position), the _higher_ the
    // priority in the heap.
    //
    const i32 parent_depth = depth - 1;
    return __builtin_clz(parent_depth ^ depth);
  }

  struct CompareFn {
    bool operator()(const Item& l, const Item& r) const
    {
      i32 l_priority = get_priority(l.insert_depth);
      i32 r_priority = get_priority(r.insert_depth);

      return l_priority < r_priority ||
             (l_priority == r_priority && (l.insert_order > r.insert_order));
    }
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  bool empty() const noexcept
  {
    return this->heap_.empty();
  }

  template <typename... Args>
  void push(Args&&... args)
  {
    this->heap_.push_back(Item{
        T{BATT_FORWARD(args)...},
        this->insert_count_,
        this->current_depth_ + 1,
    });
    this->insert_count_ += 1;

    std::push_heap(this->heap_.begin(), this->heap_.end(), VanEmdeBoasOrder::CompareFn{});
  }

  T pop()
  {
    std::pop_heap(this->heap_.begin(), this->heap_.end(), VanEmdeBoasOrder::CompareFn{});

    Item& next = this->heap_.back();
    this->current_depth_ = next.insert_depth;

    T item = std::move(next.item);

    this->heap_.pop_back();

    return item;
  }

 private:
  usize insert_count_;
  i32 current_depth_ = -1;
  std::vector<Item> heap_;
};

}  //namespace llfs

#endif  // LLFS_TRAVERSAL_ORDER_HPP
