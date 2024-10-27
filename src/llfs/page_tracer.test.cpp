//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_tracer.hpp>

#include <llfs/memory_log_device.hpp>
#include <llfs/memory_page_cache.hpp>
#include <llfs/page_graph_node.hpp>
#include <llfs/trace_refs_recursive.hpp>
#include <llfs/volume.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <unordered_set>
#include <vector>

namespace {
using namespace llfs::constants;
using namespace llfs::int_types;

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

TEST(PageTracerMockTest, TraceRefsRecursiveTest)
{
  MockPageTracer mock_tracer;

  // Create a DAG of PageIds, where nodes have PageIds in the range [1, num_nodes]. At first, every
  // node (page) in the graph is a root (no incoming edges/refs), and we erase from the root set as
  // we create edges.
  //
  std::unordered_set<llfs::PageId, llfs::PageId::Hash> roots;
  usize num_nodes = 100;
  std::mt19937 rng{num_nodes};

  for (usize u = 1; u <= num_nodes; ++u) {
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

class PageTracerTest : public ::testing::Test
{
 public:
  using PageTracerTestEvent = llfs::PackedVariant<llfs::PackedPageId>;

  void SetUp() override
  {
    this->reset_cache();
    this->reset_logs();
  }

  void TearDown() override
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  void reset_cache()
  {
    llfs::StatusOr<batt::SharedPtr<llfs::PageCache>> page_cache_created =
        llfs::make_memory_page_cache(batt::Runtime::instance().default_scheduler(),
                                     /*arena_sizes=*/
                                     {
                                         {llfs::PageCount{16}, llfs::PageSize{256}},
                                     },
                                     this->max_refs_per_page);

    ASSERT_TRUE(page_cache_created.ok());

    this->page_cache = std::move(*page_cache_created);
    batt::Status register_reader_status =
        this->page_cache->register_page_reader(llfs::PageGraphNodeView::page_layout_id(), __FILE__,
                                               __LINE__, llfs::PageGraphNodeView::page_reader());
    ASSERT_TRUE(register_reader_status.ok());
  }

  void reset_logs()
  {
    this->root_log.emplace(2 * kMiB);

    const auto recycler_options =
        llfs::PageRecyclerOptions{}.set_max_refs_per_page(this->max_refs_per_page);

    const u64 recycler_log_size = llfs::PageRecycler::calculate_log_size(recycler_options);

    EXPECT_GE(::llfs::PageRecycler::calculate_max_buffered_page_count(recycler_options,
                                                                      recycler_log_size),
              llfs::PageRecycler::default_max_buffered_page_count(recycler_options));

    this->recycler_log.emplace(recycler_log_size);
  }

  template <typename SlotVisitorFn>
  std::unique_ptr<llfs::Volume> open_volume_or_die(llfs::LogDeviceFactory& root_log,
                                                   llfs::LogDeviceFactory& recycler_log,
                                                   SlotVisitorFn&& slot_visitor_fn)
  {
    llfs::StatusOr<std::unique_ptr<llfs::Volume>> test_volume_recovered = llfs::Volume::recover(
        llfs::VolumeRecoverParams{
            &batt::Runtime::instance().default_scheduler(),
            llfs::VolumeOptions{
                .name = "test_volume",
                .uuid = llfs::None,
                .max_refs_per_page = this->max_refs_per_page,
                .trim_lock_update_interval = llfs::TrimLockUpdateInterval{0u},
                .trim_delay_byte_count = this->trim_delay,
            },
            this->page_cache,
            /*root_log=*/&root_log,
            /*recycler_log=*/&recycler_log,
            nullptr,
        },  //
        BATT_FORWARD(slot_visitor_fn));

    BATT_CHECK(test_volume_recovered.ok()) << BATT_INSPECT(test_volume_recovered.status());

    return std::move(*test_volume_recovered);
  }

  batt::StatusOr<llfs::PageId> build_page_with_refs_to(
      const std::vector<llfs::PageId>& referenced_page_ids, llfs::PageSize page_size,
      llfs::PageCacheJob& job)
  {
    batt::StatusOr<llfs::PageGraphNodeBuilder> page_builder =
        llfs::PageGraphNodeBuilder::from_new_page(job.new_page(
            page_size, batt::WaitForResource::kFalse, llfs::PageGraphNodeView::page_layout_id(),
            /*callers=*/0, /*cancel_token=*/llfs::None));

    BATT_REQUIRE_OK(page_builder);

    for (llfs::PageId page_id : referenced_page_ids) {
      page_builder->add_page(page_id);
    }

    batt::StatusOr<llfs::PinnedPage> pinned_page = std::move(*page_builder).build(job);
    BATT_REQUIRE_OK(pinned_page);

    return pinned_page->page_id();
  }

  batt::StatusOr<llfs::SlotRange> commit_job(llfs::Volume& test_volume,
                                             std::unique_ptr<llfs::PageCacheJob> job,
                                             llfs::PageId page_id)
  {
    auto event = llfs::pack_as_variant<PageTracerTestEvent>(llfs::PackedPageId::from(page_id));

    llfs::StatusOr<llfs::AppendableJob> appendable_job =
        llfs::make_appendable_job(std::move(job), llfs::PackableRef{event});

    BATT_REQUIRE_OK(appendable_job);

    const usize required_size = test_volume.calculate_grant_size(*appendable_job);

    LLFS_VLOG(1) << BATT_INSPECT(required_size);

    llfs::StatusOr<batt::Grant> grant =
        test_volume.reserve(required_size, batt::WaitForResource::kFalse);

    BATT_REQUIRE_OK(grant);

    EXPECT_EQ(grant->size(), required_size);

    return test_volume.append(std::move(*appendable_job), *grant);
  }

  const llfs::MaxRefsPerPage max_refs_per_page{8};

  llfs::TrimDelayByteCount trim_delay{0};

  batt::SharedPtr<llfs::PageCache> page_cache;

  llfs::Optional<llfs::MemoryLogDevice> root_log;

  llfs::Optional<llfs::MemoryLogDevice> recycler_log;
};

TEST_F(PageTracerTest, NoOutgoingRefsCacheDeath)
{
  const std::unique_ptr<llfs::PageDeviceEntry>& page_device1 = this->page_cache->devices_by_id()[0];

  llfs::PageId page = page_device1->arena.device().page_ids().make_page_id(1, 1);

  page_device1->no_outgoing_refs_cache.set_page_bits(1, 1, llfs::HasNoOutgoingRefs{true});
  u64 page_bits = page_device1->no_outgoing_refs_cache.get_page_bits(1, 1);
  EXPECT_EQ(page_bits, 3);

  EXPECT_DEATH(
      page_device1->no_outgoing_refs_cache.set_page_bits(1, 1, llfs::HasNoOutgoingRefs{true}),
      "Assertion failed: generation != old_generation");

  page = page_device1->arena.device().page_ids().make_page_id(2, 1);
  page_device1->no_outgoing_refs_cache.set_page_bits(2, 1, llfs::HasNoOutgoingRefs{true});
  page_device1->no_outgoing_refs_cache.set_page_bits(2, 2, llfs::HasNoOutgoingRefs{true});
  page_bits = page_device1->no_outgoing_refs_cache.get_page_bits(2, 1);
  EXPECT_EQ(page_bits, 0);
}

TEST_F(PageTracerTest, CachingPageTracerTest)
{
  std::unique_ptr<llfs::PageCacheJob> job = this->page_cache->new_job();

  std::unordered_set<llfs::PageId, llfs::PageId::Hash> not_traced_set;
  std::vector<llfs::PageId> leaves;
  std::unordered_map<llfs::PageId, llfs::OutgoingRefsStatus, llfs::PageId::Hash>
      expected_outgoing_refs_status;

  for (usize i = 0; i < 8; ++i) {
    batt::StatusOr<llfs::PageId> leaf =
        this->build_page_with_refs_to({}, llfs::PageSize{256}, *job);
    ASSERT_TRUE(leaf.ok()) << BATT_INSPECT(leaf.status());
    not_traced_set.insert(*leaf);
    leaves.emplace_back(*leaf);
    expected_outgoing_refs_status[*leaf] = llfs::OutgoingRefsStatus::kNoOutgoingRefs;
  }

  u64 num_pages_with_outgoing_refs = 0;
  for (usize i = 1; i < 8; ++i) {
    std::mt19937 rng{i};
    std::vector<llfs::PageId> outgoing_refs;
    std::sample(leaves.begin(), leaves.end(), std::back_inserter(outgoing_refs), i, rng);
    batt::StatusOr<llfs::PageId> root =
        this->build_page_with_refs_to(outgoing_refs, llfs::PageSize{256}, *job);
    job->new_root(*root);
    expected_outgoing_refs_status[*root] = llfs::OutgoingRefsStatus::kHasOutgoingRefs;
    ++num_pages_with_outgoing_refs;

    for (usize j = 0; j < outgoing_refs.size(); ++j) {
      not_traced_set.erase(outgoing_refs[j]);
    }
  }

  batt::StatusOr<llfs::PageId> island =
      this->build_page_with_refs_to({}, llfs::PageSize{256}, *job);
  not_traced_set.insert(*island);
  for (const llfs::PageId& id : not_traced_set) {
    expected_outgoing_refs_status[id] = llfs::OutgoingRefsStatus::kNotTraced;
  }

  batt::Status trace_status = job->trace_new_roots(/*page_loader=*/*job, /*page_id_fn=*/
                                                   []([[maybe_unused]] llfs::PageId page_id) {

                                                   });
  ASSERT_TRUE(trace_status.ok());

  // Verify the OutgoingRefsStatus for each page.
  //
  const std::vector<std::unique_ptr<llfs::PageDeviceEntry>>& page_devices =
      this->page_cache->devices_by_id();
  for (const auto& [page_id, expected_status] : expected_outgoing_refs_status) {
    llfs::page_device_id_int device = llfs::PageIdFactory::get_device_id(page_id);
    u64 physical_page = page_devices[device]->arena.device().page_ids().get_physical_page(page_id);
    llfs::page_generation_int generation =
        page_devices[device]->arena.device().page_ids().get_generation(page_id);
    llfs::OutgoingRefsStatus actual_status = static_cast<llfs::OutgoingRefsStatus>(
        page_devices[device]->no_outgoing_refs_cache.get_page_bits(physical_page, generation));
    EXPECT_EQ(actual_status, expected_status);
  }

  // Perform another call to trace_refs_recursively, this time with the PageCache itself as the
  // PageLoader being passed in. This way, we can compare the get_count metric of PageCache to make
  // sure we aren't doing unecessary loads in this function.
  //
  int initial_get_count = this->page_cache->metrics().get_count.load();
  trace_status = job->trace_new_roots(/*page_loader=*/*(this->page_cache), /*page_id_fn=*/
                                      []([[maybe_unused]] llfs::PageId page_id) {

                                      });
  ASSERT_TRUE(trace_status.ok());

  int expected_post_trace_get_count = initial_get_count + num_pages_with_outgoing_refs;
  EXPECT_EQ(expected_post_trace_get_count, this->page_cache->metrics().get_count.load());
}

}  // namespace
