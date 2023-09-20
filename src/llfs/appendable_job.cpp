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
  return JobSizeSpec::from_job(*this).calculate_grant_size();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ auto JobSizeSpec::from_job(const AppendableJob& appendable) noexcept -> Self
{
  return JobSizeSpec{
      .user_data_size = packed_sizeof(appendable.user_data),
      .root_page_ref_count = appendable.job.root_count(),
      .new_page_count = appendable.job.new_page_count(),
      .deleted_page_count = appendable.job.deleted_page_count(),
      .page_device_count = appendable.job.page_device_count(),
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize JobSizeSpec::calculate_grant_size() const noexcept
{
  const usize user_data_size = this->user_data_size;
  const usize root_ids_size = packed_array_size<PackedPageId>(this->root_page_ref_count);
  const usize new_ids_size = packed_array_size<PackedPageId>(this->new_page_count);
  const usize deleted_ids_size = packed_array_size<PackedPageId>(this->deleted_page_count);
  const usize device_ids_size =
      packed_array_size<little_page_device_id_int>(this->page_device_count);

  const usize prepare_slot_size = packed_sizeof_slot_with_payload_size(  //
      sizeof(PackedPrepareJob)                                           //
      + user_data_size                                                   //
      + root_ids_size                                                    //
      + new_ids_size                                                     //
      + deleted_ids_size                                                 //
      + device_ids_size                                                  //
  );

  const usize commit_slot_size =             //
      packed_sizeof_slot_with_payload_size(  //
          sizeof(PackedCommitJob)            //
          + user_data_size                   //
          + root_ids_size                    //
      );

  LLFS_DVLOG(1) << BATT_INSPECT(user_data_size) << BATT_INSPECT(root_ids_size)
                << BATT_INSPECT(new_ids_size) << BATT_INSPECT(deleted_ids_size)
                << BATT_INSPECT(device_ids_size) << BATT_INSPECT(prepare_slot_size)
                << BATT_INSPECT(commit_slot_size);

  return prepare_slot_size + commit_slot_size;
}

}  // namespace llfs
