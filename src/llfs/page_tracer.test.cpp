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

#include <llfs/testing/fake_log_device.hpp>
#include <llfs/testing/test_config.hpp>

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
        llfs::make_memory_page_cache(
            batt::Runtime::instance().default_scheduler(),
            /*arena_sizes=*/
            {
                {llfs::PageCount{this->page_device_capacity}, llfs::PageSize{256}},
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
        llfs::PageGraphNodeBuilder::from_new_page(
            job.new_page(page_size, batt::WaitForResource::kFalse,
                         llfs::PageGraphNodeView::page_layout_id(), llfs::LruPriority{1},
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

  /** \brief Get the generation of the given page with id `page_id`.
   */
  llfs::page_generation_int get_generation(llfs::PageId page_id)
  {
    const std::vector<std::unique_ptr<llfs::PageDeviceEntry>>& page_devices =
        this->page_cache->devices_by_id();
    llfs::page_device_id_int device = llfs::PageIdFactory::get_device_id(page_id);
    llfs::page_generation_int generation =
        page_devices[device]->arena.device().page_ids().get_generation(page_id);
    return generation;
  }

  /** \brief Get the outgoing reference status of the given page with id `page_id`.
   *
   * \return An u64 value telling the caller if the page has outgoing references to
   * other pages, does not have any outgoing references, or has never been traced yet.
   */
  batt::BoolStatus get_outgoing_refs_status(llfs::PageId page_id)
  {
    const std::vector<std::unique_ptr<llfs::PageDeviceEntry>>& page_devices =
        this->page_cache->devices_by_id();
    llfs::page_device_id_int device = llfs::PageIdFactory::get_device_id(page_id);

    batt::BoolStatus has_outgoing_refs =
        page_devices[device]->no_outgoing_refs_cache.has_outgoing_refs(page_id);
    return has_outgoing_refs;
  }

  /** \brief A utility function that triggers a call to `trace_refs_recursive` with the PageCache as
   * the PageLoader being used. This way, a caller can test how many calls to PageCache's `get_page`
   * method occurred.
   *
   * \return The difference between the number of `get_page` calls before and after the execution of
   * `trace_refs_recursive`.
   */
  batt::StatusOr<u64> test_cache_get_count_delta(llfs::PageCacheJob& job)
  {
    int initial_get_count = this->page_cache->metrics().get_count.load();
    batt::Status trace_status =
        job.trace_new_roots(/*page_loader=*/*(this->page_cache), /*page_id_fn=*/
                            []([[maybe_unused]] llfs::PageId page_id) {

                            });
    BATT_REQUIRE_OK(trace_status);
    return this->page_cache->metrics().get_count.load() - initial_get_count;
  }

  const u64 page_device_capacity{16};

  const llfs::MaxRefsPerPage max_refs_per_page{8};

  llfs::TrimDelayByteCount trim_delay{0};

  batt::SharedPtr<llfs::PageCache> page_cache;

  llfs::Optional<llfs::MemoryLogDevice> root_log;

  llfs::Optional<llfs::MemoryLogDevice> recycler_log;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// Test Plan:
//  1. Test the basic operations on an instance of a NoOutgoingRefsCache using the PageCache in the
//  test fixture.
//
//  2. Test using just a PageCache and a PageCacheJob (without committing the job) to
//  test CachingPageTracer's usage in the `trace_refs_recursive` function . This will make sure we
//  are getting and setting the OutgoingRefsStatus correctly, as well as ensure that we are
//  performing loads from the PageCache only when need be.
//
//  3. Create a more complete set up with a Volume (which will also include its PageRecycler and the
//  PageCache) to test the full workflow of setting, getting, and invalidating the
//  OutgoingRefsStatus.

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
TEST_F(PageTracerTest, NoOutgoingRefsCacheDeath)
{
  const std::unique_ptr<llfs::PageDeviceEntry>& page_device1 = this->page_cache->devices_by_id()[0];

  llfs::HasOutgoingRefs no_outgoing_refs{false};
  llfs::PageId page = page_device1->arena.device().page_ids().make_page_id(1, 1);

  // Create a page with physical page id 1 and generation 1, then set and get its page bits in the
  // PageDevice's NoOutgoingRefsCache. The first get will return an unknown status.
  //
  EXPECT_EQ(page_device1->no_outgoing_refs_cache.has_outgoing_refs(page),
            batt::BoolStatus::kUnknown);
  page_device1->no_outgoing_refs_cache.set_page_state(page, no_outgoing_refs);
  batt::BoolStatus has_outgoing_refs = page_device1->no_outgoing_refs_cache.has_outgoing_refs(page);

  // When a page has no outgoing refs, its BoolStatus will be kFalse.
  //
  EXPECT_EQ(has_outgoing_refs, batt::BoolStatus::kFalse);

  // We can't set different outgoing refs bits for the same generation of a page!
  //
  EXPECT_DEATH(
      page_device1->no_outgoing_refs_cache.set_page_state(page, llfs::HasOutgoingRefs{true}),
      "Assertion failed: new_cache_entry == old_cache_entry");

  page = page_device1->arena.device().page_ids().make_page_id(2, 1);
  page_device1->no_outgoing_refs_cache.set_page_state(page, no_outgoing_refs);

  llfs::PageId new_generation_page =
      page_device1->arena.device().page_ids().advance_generation(page);
  page_device1->no_outgoing_refs_cache.set_page_state(new_generation_page, no_outgoing_refs);
  has_outgoing_refs = page_device1->no_outgoing_refs_cache.has_outgoing_refs(page);

  // Since we just queried for a previous generation's page state bits, we will be returned a
  // BoolStatus of value kUnknown.
  //
  EXPECT_EQ(has_outgoing_refs, batt::BoolStatus::kUnknown);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
TEST_F(PageTracerTest, CachingPageTracerTest)
{
  std::unique_ptr<llfs::PageCacheJob> job = this->page_cache->new_job();

  // Maintain a set of PageIds for pages that won't be traced when `trace_refs_recursive` is called,
  // since it has no incoming refs.
  //
  std::unordered_set<llfs::PageId, llfs::PageId::Hash> not_traced_set;
  std::vector<llfs::PageId> leaves;

  // BoolStatus kFalse: page has no outgoing refs.
  // BoolStatus kTrue: page has outgoing refs.
  //
  std::unordered_map<llfs::PageId, batt::BoolStatus, llfs::PageId::Hash>
      expected_outgoing_refs_status;

  // Use half of the PageDevice's capacity to make leaf pages, i.e. pages with no outgoing refs.
  //
  for (usize i = 0; i < (this->page_device_capacity / 2); ++i) {
    batt::StatusOr<llfs::PageId> leaf =
        this->build_page_with_refs_to({}, llfs::PageSize{256}, *job);
    ASSERT_TRUE(leaf.ok()) << BATT_INSPECT(leaf.status());
    not_traced_set.insert(*leaf);
    leaves.emplace_back(*leaf);
    expected_outgoing_refs_status[*leaf] = batt::BoolStatus::kFalse;
  }

  // Use (half of PageDevice's capacity - 1) number of pages for pages with outgoing refs.
  //
  u64 num_pages_with_outgoing_refs = 0;
  for (usize i = 1; i < (this->page_device_capacity / 2); ++i) {
    std::mt19937 rng{i};
    std::vector<llfs::PageId> outgoing_refs;
    // Randomly generate outoing refs to the leaves we created above.
    //
    std::sample(leaves.begin(), leaves.end(), std::back_inserter(outgoing_refs), i, rng);
    batt::StatusOr<llfs::PageId> root =
        this->build_page_with_refs_to(outgoing_refs, llfs::PageSize{256}, *job);
    job->new_root(*root);
    expected_outgoing_refs_status[*root] = batt::BoolStatus::kTrue;
    ++num_pages_with_outgoing_refs;

    // Those leaves that were initially placed in the not_traced set are now traceable since they
    // now have incoming refs.
    //
    for (usize j = 0; j < outgoing_refs.size(); ++j) {
      not_traced_set.erase(outgoing_refs[j]);
    }
  }

  // Since we have 1 page left to allocate in the PageDevice, use that to guarentee we have at least
  // one untraceable page.
  //
  batt::StatusOr<llfs::PageId> island =
      this->build_page_with_refs_to({}, llfs::PageSize{256}, *job);
  not_traced_set.insert(*island);
  for (const llfs::PageId& id : not_traced_set) {
    // All non traceable pages will never has their outgoing refs information set, so their status
    // will be kUnknown.
    //
    expected_outgoing_refs_status[id] = batt::BoolStatus::kUnknown;
  }

  batt::Status trace_status = job->trace_new_roots(/*page_loader=*/*job, /*page_id_fn=*/
                                                   []([[maybe_unused]] llfs::PageId page_id) {

                                                   });
  ASSERT_TRUE(trace_status.ok());

  // Verify the OutgoingRefsStatus for each page.
  //
  for (const auto& [page_id, expected_status] : expected_outgoing_refs_status) {
    batt::BoolStatus actual_status = this->get_outgoing_refs_status(page_id);
    EXPECT_EQ(actual_status, expected_status);
  }

  // Perform another call to trace_new_roots, this time with the PageCache itself as the
  // PageLoader being passed in. This way, we can compare the get_count metric of PageCache to make
  // sure we aren't doing unecessary loads in this function.
  //
  batt::StatusOr<u64> get_count_delta = this->test_cache_get_count_delta(*job);
  ASSERT_TRUE(get_count_delta.ok()) << BATT_INSPECT(get_count_delta.status());
  EXPECT_EQ(*get_count_delta, num_pages_with_outgoing_refs);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
TEST_F(PageTracerTest, VolumeAndRecyclerTest)
{
  auto fake_root_log = llfs::testing::make_fake_log_device_factory(*this->root_log);
  auto fake_recycler_log = llfs::testing::make_fake_log_device_factory(*this->recycler_log);

  std::unique_ptr<llfs::Volume> test_volume = this->open_volume_or_die(
      fake_root_log, fake_recycler_log,
      /*slot_visitor_fn=*/[](const llfs::SlotParse&, const auto& /*payload*/) {
        return llfs::OkStatus();
      });

  // BoolStatus kFalse: page has no outgoing refs.
  // BoolStatus kTrue: page has outgoing refs.
  //
  std::unordered_map<llfs::PageId, batt::BoolStatus, llfs::PageId::Hash> expected_outgoing_refs;
  u64 num_pages_created = 0;

  // Create the first job with some new pages and commit it.
  //
  std::unique_ptr<llfs::PageCacheJob> job1 = test_volume->new_job();

  batt::StatusOr<llfs::PageId> leaf1 =
      this->build_page_with_refs_to({}, llfs::PageSize{256}, *job1);
  ASSERT_TRUE(leaf1.ok()) << BATT_INSPECT(leaf1.status());
  ++num_pages_created;

  batt::StatusOr<llfs::PageId> leaf2 =
      this->build_page_with_refs_to({}, llfs::PageSize{256}, *job1);
  ASSERT_TRUE(leaf2.ok()) << BATT_INSPECT(leaf2.status());
  ++num_pages_created;

  batt::StatusOr<llfs::PageId> root1 =
      this->build_page_with_refs_to({*leaf1, *leaf2}, llfs::PageSize{256}, *job1);
  ASSERT_TRUE(root1.ok()) << BATT_INSPECT(root1.status());
  ++num_pages_created;

  llfs::StatusOr<llfs::SlotRange> commit_root1 =
      this->commit_job(*test_volume, std::move(job1), *root1);
  ASSERT_TRUE(commit_root1.ok()) << BATT_INSPECT(commit_root1.status());

  // Create a second job and commit it. This gives the first leaf page two incoming references from
  // other pages, so a Volume trim wouldn't recycle it right away.
  //
  std::unique_ptr<llfs::PageCacheJob> job2 = test_volume->new_job();
  batt::StatusOr<llfs::PageId> root2 =
      this->build_page_with_refs_to({*leaf1}, llfs::PageSize{256}, *job2);
  ASSERT_TRUE(root2.ok()) << BATT_INSPECT(root2.status());
  ++num_pages_created;

  llfs::StatusOr<llfs::SlotRange> commit_root2 =
      this->commit_job(*test_volume, std::move(job2), *root2);
  ASSERT_TRUE(commit_root2.ok()) << BATT_INSPECT(commit_root2.status());

  expected_outgoing_refs[*leaf1] = batt::BoolStatus::kFalse;
  expected_outgoing_refs[*leaf2] = batt::BoolStatus::kFalse;
  expected_outgoing_refs[*root1] = batt::BoolStatus::kTrue;
  expected_outgoing_refs[*root2] = batt::BoolStatus::kTrue;

  // Create the last job to fill up the PageDevice.
  //
  std::unique_ptr<llfs::PageCacheJob> job3 = test_volume->new_job();
  batt::StatusOr<llfs::PageId> page = this->build_page_with_refs_to({}, llfs::PageSize{256}, *job3);
  ++num_pages_created;
  expected_outgoing_refs[*page] = batt::BoolStatus::kFalse;
  for (usize i = 0; i < (this->page_device_capacity - num_pages_created); ++i) {
    llfs::PageId prev_page{*page};
    page = this->build_page_with_refs_to({prev_page}, llfs::PageSize{256}, *job3);
    expected_outgoing_refs[*page] = batt::BoolStatus::kTrue;
  }
  llfs::StatusOr<llfs::SlotRange> commit_remaining =
      this->commit_job(*test_volume, std::move(job3), *page);
  ASSERT_TRUE(commit_remaining.ok()) << BATT_INSPECT(commit_remaining.status());

  llfs::StatusOr<llfs::SlotRange> flushed = test_volume->sync(
      llfs::LogReadMode::kDurable, llfs::SlotUpperBoundAt{commit_remaining->upper_bound});
  ASSERT_TRUE(flushed.ok());

  // Verify the outgoing refs status.
  //
  for (const auto& [page_id, expected_status] : expected_outgoing_refs) {
    batt::BoolStatus actual_status = this->get_outgoing_refs_status(page_id);
    EXPECT_EQ(actual_status, expected_status);
  }

  // Trim the Volume. This should cause root1 and leaf2 to be recycled.
  //
  llfs::Status trim_set = test_volume->trim(commit_root1->upper_bound);
  ASSERT_TRUE(trim_set.ok());
  llfs::Status trimmed = test_volume->await_trim(commit_root1->upper_bound);
  ASSERT_TRUE(trimmed.ok());

  ASSERT_TRUE(this->page_cache->arena_for_page_id(*root1).allocator().await_ref_count(*root1, 0));
  ASSERT_TRUE(this->page_cache->arena_for_page_id(*leaf2).allocator().await_ref_count(*leaf2, 0));

  // Now, create a new job to create a new page. This new page should be the second generation of a
  // physical page due to the recycling that just happened.
  //
  std::unique_ptr<llfs::PageCacheJob> job4 = test_volume->new_job();
  batt::StatusOr<llfs::PageId> new_leaf =
      this->build_page_with_refs_to({}, llfs::PageSize{256}, *job4);
  ASSERT_TRUE(new_leaf.ok()) << BATT_INSPECT(new_leaf.status());
  EXPECT_GT(this->get_generation(*new_leaf), 1);
  job4->new_root(*new_leaf);

  // Test to make sure we DON'T use the existing NoOutgoingRefsCache cache entry for this physical
  // page since we are tracing a new generation of the physical page, and that we fall back on our
  // wrapped loading tracer to trace the outgoing refs for the page.
  //
  batt::StatusOr<u64> get_count_delta = this->test_cache_get_count_delta(*job4);
  ASSERT_TRUE(get_count_delta.ok()) << BATT_INSPECT(get_count_delta.status());
  EXPECT_EQ(*get_count_delta, 1);
}

}  // namespace
