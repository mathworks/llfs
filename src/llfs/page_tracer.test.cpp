//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_tracer.hpp>
#include <llfs/trace_refs_recursive.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <unordered_set>
#include <vector>

namespace {
using namespace batt::int_types;

class MockPageTracer : public llfs::PageTracer
{
 public:
  MOCK_METHOD(batt::StatusOr<batt::BoxedSeq<llfs::PageId>>, trace_page_refs,
              (llfs::PageId from_page_id), (override, noexcept));
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// Test Plan:
//  1. Mock a PageTracer and test its integration with trace_refs_recursive by creating a DAG of
//  PageIds.

TEST(PageTracerTest, TraceRefsRecursiveTest)
{
  MockPageTracer mock_tracer;

  // Create a DAG of PageIds, where nodes have PageIds in the range [1, num_nodes]. At first, every
  // node (page) in the graph is a root (no incoming edges/refs), and we erase from the root set as
  // we create edges.
  //
  std::unordered_set<llfs::PageId, llfs::PageId::Hash> roots;
  usize num_nodes = 100;
  std::mt19937 rng{num_nodes};

  for (u16 u = 1; u <= num_nodes; ++u) {
    roots.insert(llfs::PageId{u});
  }

  u64 expected_total_num_edges = 0;
  for (usize u = 1; u <= num_nodes; ++u) {
    std::vector<llfs::PageId> outgoing_refs;
    for (usize v = u + 1; v <= num_nodes; ++v) {
      // Randomly create edges. PageId{1} will always be a root, and all nodes (pages) can have
      // outgoing edges/refs to pages with a greater id value.
      //
      std::uniform_int_distribution<> has_edge(0, 1);
      if (has_edge(rng)) {
        outgoing_refs.emplace_back(llfs::PageId{v});
        ++expected_total_num_edges;
        roots.erase(llfs::PageId{v});
      }
    }

    EXPECT_CALL(mock_tracer, trace_page_refs(llfs::PageId{u}))
        .WillOnce(::testing::Invoke([outgoing_refs]() {
          batt::BoxedSeq<llfs::PageId> seq_outgoing_refs{batt::as_seq(outgoing_refs)};
          return batt::StatusOr<batt::BoxedSeq<llfs::PageId>>{seq_outgoing_refs};
        }));
  }

  u64 calculated_num_edges = 0;
  batt::Status status = llfs::trace_refs_recursive(
      mock_tracer, batt::as_seq(roots.begin(), roots.end()),
      []([[maybe_unused]] llfs::PageId page_id) {
        return true;
      },
      [&calculated_num_edges](llfs::PageId page_id) {
        if (page_id) {
          ++calculated_num_edges;
        }
      });

  ASSERT_TRUE(status.ok());

  // Ensure that all edges were traced correctly in trace_refs_recursive.
  //
  EXPECT_EQ(expected_total_num_edges, calculated_num_edges);
}
}  // namespace
