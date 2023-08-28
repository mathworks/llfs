//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/appendable_job.hpp>
//

#include <llfs/volume_events.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<AppendableJob> make_appendable_job(std::unique_ptr<PageCacheJob>&& job,
                                            PackableRef&& user_data)
{
  trace_refs(user_data) | seq::for_each([&job](PageId page_id) {
    job->new_root(page_id);
  });

  StatusOr<CommittablePageCacheJob> committable =
      CommittablePageCacheJob::from(std::move(job), /*callers=*/Caller::Unknown);

  BATT_REQUIRE_OK(committable);

  return AppendableJob{
      .job = std::move(*committable),
      .user_data = std::move(user_data),
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PrepareJob prepare(const AppendableJob& appendable)
{
  return PrepareJob{
      .new_page_ids = appendable.job.new_page_ids(),
      .deleted_page_ids = appendable.job.deleted_page_ids(),
      .page_device_ids = appendable.job.page_device_ids(),
      .user_data = appendable.user_data,
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
u64 AppendableJob::calculate_grant_size() const noexcept
{
  const usize user_data_size = packed_sizeof(this->user_data);
  const usize root_refs_size = packed_array_size<PackedPageId>(this->job.root_count());

  return                                                                                 //
      packed_sizeof_slot_with_payload_size(                                              //
          sizeof(PackedPrepareJob)                                                       //
          + user_data_size                                                               //
          + root_refs_size                                                               //
          + packed_array_size<PackedPageId>(this->job.new_page_count())                  //
          + packed_array_size<PackedPageId>(this->job.deleted_page_count())              //
          + packed_array_size<little_page_device_id_int>(this->job.page_device_count())  //
          )                                                                              //
      + packed_sizeof_slot_with_payload_size(                                            //
            sizeof(PackedCommitJob)                                                      //
            + user_data_size                                                             //
            + root_refs_size                                                             //
        );
}

}  // namespace llfs
