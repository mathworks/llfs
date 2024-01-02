//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/ioring_stream_buffer.hpp>
//
#include <llfs/ioring_stream_buffer.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace {

// Test Plan:
//  Sizes:
//   1. Empty
//   2. Full
//   3. Low (contains <=1 buffer worth, >0 bytes)
//   4. High (contains >1 buffer worth, not full)
//  States:
//   a. open
//   b. closed
//  Actions:
//   i. close
//   ii. prepare
//   iii. commit
//   iv. consume(start, end)
//   v. consume_some
//  Results:
//   w. blocking until resolved into one of the following...
//   x. success, no blocking
//   y. fail, error
//   z. fail, panic
//

TEST(IoringStreamBufferTest, Test)
{
}

}  // namespace
