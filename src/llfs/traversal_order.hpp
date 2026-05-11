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

#include <batteries/algo/traversal_order.hpp>

namespace llfs {

using batt::algo::BreadthFirstOrder;
using batt::algo::TraversalItem;
using batt::algo::VanEmdeBoasOrder;

}  //namespace llfs

#endif  // LLFS_TRAVERSAL_ORDER_HPP
