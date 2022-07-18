//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_TESTING_MOCK_SLOT_VISITOR_FN_HPP
#define LLFS_TESTING_MOCK_SLOT_VISITOR_FN_HPP

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace llfs {

class MockSlotVisitorFn
{
 public:
  MOCK_METHOD(Status, visit_slot, (const SlotParse&, std::string_view), ());

  Status operator()(const SlotParse& slot, std::string_view payload)
  {
    return this->visit_slot(slot, payload);
  }
};

}  // namespace llfs

#endif  // LLFS_TESTING_MOCK_SLOT_VISITOR_FN_HPP
