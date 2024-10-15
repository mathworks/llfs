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
#include <llfs/volume.hpp>

#include <llfs/testing/fake_log_device.hpp>
#include <llfs/testing/test_config.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace {
using namespace llfs::constants;
using namespace llfs::int_types;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// Test Plan:
//  1. Create an instance of NoOutgoingRefsCache and then set and get some values.
//  2. Expect failure when setting bits twice in a row.

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

TEST(NoOutgoingRefsCacheTest, CacheOperationsDeath)
{
  llfs::NoOutgoingRefsCache cache{33};

  EXPECT_EQ(cache.get_page_bits(3), 0);

  cache.set_page_bits(33, llfs::OutgoingRefsStatus::kNoOutgoingRefs);
  EXPECT_EQ(cache.get_page_bits(33), 3);

  cache.clear_page_bits(33);
  EXPECT_EQ(cache.get_page_bits(33), 0);

  cache.set_page_bits(32, llfs::OutgoingRefsStatus::kHasOutgoingRefs);
  EXPECT_EQ(cache.get_page_bits(32), 2);
  // Can't set the page bits again without clearing them first!
  //
  EXPECT_DEATH(cache.set_page_bits(32, llfs::OutgoingRefsStatus::kNoOutgoingRefs),
               "Assertion failed: \\(old_value >> bit_offset\\) & this->bit_mask_ == 0");
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//

// TODO [vsilai 2024-09-29] much of this is duplicated from volume.test.cpp infrastructure. This
// probably needs be avoided?
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
                                     {{llfs::PageCount{8}, llfs::PageSize{1 * kKiB}},
                                      {llfs::PageCount{4}, llfs::PageSize{2 * kKiB}}},
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

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// Test Plan:
//  1. Create a set up with just a PageCache (and its arenas with PageDevices and PageAllocators) to
//  test the `trace_refs_recursively` function in PageTracer. This will make sure we are getting and
//  setting the OutgoingRefsStatus correctly, as well as ensure that we are performing loads from
//  the PageCache only when need be.
//
//  2. Create a more complete set up with a Volume (which will also include its PageRecycler and the
//  PageCache) to test the full workflow of setting, getting, and clearing the OutgoingRefsStatus.

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
TEST_F(PageTracerTest, TraceRefsRecursive)
{
  std::unique_ptr<llfs::PageCacheJob> job = this->page_cache->new_job();

  // Create a "graph" of pages across the two PageDevices available. We should have relationships
  // like this:
  //
  // For the first PageDevice:
  // {root1} -> {leaf1, leaf2, leaf3}
  // {root2} -> {leaf1, leaf2}
  // {leaf1} -> {}
  // {leaf2} -> {}
  // {leaf3} -> {}
  //
  // For the second PageDevice:
  // {root} -> {leaf1}
  // {leaf1} -> {}
  // {leaf2} -> {}
  //
  batt::StatusOr<llfs::PageId> device1_leaf1 =
      this->build_page_with_refs_to({}, llfs::PageSize{1 * kKiB}, *job);
  ASSERT_TRUE(device1_leaf1.ok()) << BATT_INSPECT(device1_leaf1.status());

  batt::StatusOr<llfs::PageId> device1_leaf2 =
      this->build_page_with_refs_to({}, llfs::PageSize{1 * kKiB}, *job);
  ASSERT_TRUE(device1_leaf2.ok()) << BATT_INSPECT(device1_leaf2.status());

  batt::StatusOr<llfs::PageId> device1_leaf3 =
      this->build_page_with_refs_to({}, llfs::PageSize{1 * kKiB}, *job);
  ASSERT_TRUE(device1_leaf3.ok()) << BATT_INSPECT(device1_leaf3.status());

  batt::StatusOr<llfs::PageId> device1_root1 = this->build_page_with_refs_to(
      {*device1_leaf1, *device1_leaf2, *device1_leaf3}, llfs::PageSize{1 * kKiB}, *job);
  ASSERT_TRUE(device1_root1.ok()) << BATT_INSPECT(device1_root1.status());
  job->new_root(*device1_root1);

  batt::StatusOr<llfs::PageId> device1_root2 = this->build_page_with_refs_to(
      {*device1_leaf1, *device1_leaf2}, llfs::PageSize{1 * kKiB}, *job);
  ASSERT_TRUE(device1_root2.ok()) << BATT_INSPECT(device1_root2.status());
  job->new_root(*device1_root2);

  batt::StatusOr<llfs::PageId> device2_leaf1 =
      this->build_page_with_refs_to({}, llfs::PageSize{2 * kKiB}, *job);
  ASSERT_TRUE(device2_leaf1.ok()) << BATT_INSPECT(device2_leaf1.status());

  // This page will be left untraced since no other page references it.
  //
  batt::StatusOr<llfs::PageId> device2_leaf2 =
      this->build_page_with_refs_to({}, llfs::PageSize{2 * kKiB}, *job);
  ASSERT_TRUE(device2_leaf2.ok()) << BATT_INSPECT(device2_leaf2.status());

  batt::StatusOr<llfs::PageId> device2_root =
      this->build_page_with_refs_to({*device2_leaf1}, llfs::PageSize{2 * kKiB}, *job);
  ASSERT_TRUE(device2_root.ok()) << BATT_INSPECT(device2_root.status());
  job->new_root(*device2_root);

  // First call to trace_refs_recursively. Use the hash table to trace incoming edges to leaves in
  // order to make sure we are tracing outgoing refs correctly with the function we pass in.
  //
  std::unordered_map<llfs::PageId, u64, llfs::PageId::Hash> incoming_refs;
  batt::Status trace_status = job->trace_new_roots(/*page_loader=*/*job, /*page_id_fn=*/
                                                   [&incoming_refs](llfs::PageId page_id) {
                                                     incoming_refs[page_id]++;
                                                   });
  ASSERT_TRUE(trace_status.ok());

  std::unordered_map<llfs::PageId, u64, llfs::PageId::Hash> expected_incoming_refs;
  expected_incoming_refs[*device1_leaf1] = 2;
  expected_incoming_refs[*device1_leaf2] = 2;
  expected_incoming_refs[*device1_leaf3] = 1;
  expected_incoming_refs[*device2_leaf1] = 1;
  EXPECT_EQ(incoming_refs, expected_incoming_refs);

  std::unordered_map<llfs::PageId, llfs::OutgoingRefsStatus, llfs::PageId::Hash>
      expected_outgoing_refs;
  expected_outgoing_refs[*device1_leaf1] = llfs::OutgoingRefsStatus::kNoOutgoingRefs;
  expected_outgoing_refs[*device1_leaf2] = llfs::OutgoingRefsStatus::kNoOutgoingRefs;
  expected_outgoing_refs[*device1_leaf3] = llfs::OutgoingRefsStatus::kNoOutgoingRefs;
  expected_outgoing_refs[*device1_root1] = llfs::OutgoingRefsStatus::kHasOutgoingRefs;
  expected_outgoing_refs[*device1_root2] = llfs::OutgoingRefsStatus::kHasOutgoingRefs;
  expected_outgoing_refs[*device2_leaf1] = llfs::OutgoingRefsStatus::kNoOutgoingRefs;
  expected_outgoing_refs[*device2_leaf2] = llfs::OutgoingRefsStatus::kNotTraced;
  expected_outgoing_refs[*device2_root] = llfs::OutgoingRefsStatus::kHasOutgoingRefs;

  // Verify the OutgoingRefsStatus for each page.
  //
  for (const auto& [page_id, expected_status] : expected_outgoing_refs) {
    llfs::OutgoingRefsStatus actual_status = this->page_cache->get_outgoing_refs_info(page_id);
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
  // 3 pages have outgoing references, they should be loaded again.
  //
  int expected_post_trace_get_count = initial_get_count + 3;
  EXPECT_EQ(expected_post_trace_get_count, this->page_cache->metrics().get_count.load());
}

TEST_F(PageTracerTest, VolumeAndRecycler)
{
  auto fake_root_log = llfs::testing::make_fake_log_device_factory(*this->root_log);
  auto fake_recycler_log = llfs::testing::make_fake_log_device_factory(*this->recycler_log);

  std::unique_ptr<llfs::Volume> test_volume = this->open_volume_or_die(
      fake_root_log, fake_recycler_log,
      /*slot_visitor_fn=*/[](const llfs::SlotParse&, const auto& /*payload*/) {
        return llfs::OkStatus();
      });

  // Create the first job and commit it.
  //
  std::unique_ptr<llfs::PageCacheJob> job = test_volume->new_job();

  batt::StatusOr<llfs::PageId> leaf1 =
      this->build_page_with_refs_to({}, llfs::PageSize{1 * kKiB}, *job);
  ASSERT_TRUE(leaf1.ok()) << BATT_INSPECT(leaf1.status());

  batt::StatusOr<llfs::PageId> leaf2 =
      this->build_page_with_refs_to({}, llfs::PageSize{1 * kKiB}, *job);
  ASSERT_TRUE(leaf2.ok()) << BATT_INSPECT(leaf2.status());

  batt::StatusOr<llfs::PageId> root1 =
      this->build_page_with_refs_to({*leaf1, *leaf2}, llfs::PageSize{1 * kKiB}, *job);
  ASSERT_TRUE(root1.ok()) << BATT_INSPECT(root1.status());
  llfs::StatusOr<llfs::SlotRange> commit_root1 =
      this->commit_job(*test_volume, std::move(job), *root1);
  ASSERT_TRUE(commit_root1.ok()) << BATT_INSPECT(commit_root1.status());

  // Create another job and commit it.
  //
  std::unique_ptr<llfs::PageCacheJob> job2 = test_volume->new_job();
  batt::StatusOr<llfs::PageId> root2 =
      this->build_page_with_refs_to({*leaf1}, llfs::PageSize{1 * kKiB}, *job2);
  ASSERT_TRUE(root2.ok()) << BATT_INSPECT(root2.status());
  llfs::StatusOr<llfs::SlotRange> commit_root2 =
      this->commit_job(*test_volume, std::move(job2), *root2);
  ASSERT_TRUE(commit_root2.ok()) << BATT_INSPECT(commit_root2.status());

  llfs::StatusOr<llfs::SlotRange> flushed = test_volume->sync(
      llfs::LogReadMode::kDurable, llfs::SlotUpperBoundAt{commit_root2->upper_bound});
  ASSERT_TRUE(flushed.ok());

  // Verify the outgoing refs status for each page.
  //
  std::unordered_map<llfs::PageId, llfs::OutgoingRefsStatus, llfs::PageId::Hash>
      expected_outgoing_refs;
  expected_outgoing_refs[*leaf1] = llfs::OutgoingRefsStatus::kNoOutgoingRefs;
  expected_outgoing_refs[*leaf2] = llfs::OutgoingRefsStatus::kNoOutgoingRefs;
  expected_outgoing_refs[*root1] = llfs::OutgoingRefsStatus::kHasOutgoingRefs;
  expected_outgoing_refs[*root2] = llfs::OutgoingRefsStatus::kHasOutgoingRefs;

  for (const auto& [page_id, expected_status] : expected_outgoing_refs) {
    llfs::OutgoingRefsStatus actual_status = this->page_cache->get_outgoing_refs_info(page_id);
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

  // Verify that these two pages had their outgoing refs status bits cleared.
  //
  expected_outgoing_refs[*leaf2] = llfs::OutgoingRefsStatus::kNotTraced;
  expected_outgoing_refs[*root1] = llfs::OutgoingRefsStatus::kNotTraced;

  for (const auto& [page_id, expected_status] : expected_outgoing_refs) {
    llfs::OutgoingRefsStatus actual_status = this->page_cache->get_outgoing_refs_info(page_id);
    EXPECT_EQ(actual_status, expected_status);
  }
}

}  // namespace
