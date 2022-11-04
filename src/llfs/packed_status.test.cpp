//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/packed_status.hpp>
//
#include <llfs/packed_status.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstring>

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

using namespace batt::int_types;
using namespace batt::constants;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

namespace llfs {
namespace packed_status_test {

enum struct CustomCodes {
  ZERO = 0,
  ONE = 1,
  TWO = 2,
};

}
}  // namespace llfs

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

namespace {

const char* kMessage0 = "Zero is the loneliest number";
const char* kMessage1 = "Oh wait, no, that dubious distinction belongs to one";
const char* kMessage2 = "But two can be just as bad";

void register_custom_codes()
{
  using ::llfs::packed_status_test::CustomCodes;

  batt::Status::register_codes<CustomCodes>({
      {CustomCodes::ZERO, kMessage0},
      {CustomCodes::ONE, kMessage1},
      {CustomCodes::TWO, kMessage2},
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(PackedStatusTest, Test)
{
  register_custom_codes();

  batt::Status status{llfs::packed_status_test::CustomCodes::ONE};

  const usize required_size = llfs::packed_sizeof(status);

  EXPECT_EQ(required_size, 20 + std::strlen(kMessage1) +
                               std::strlen(batt::name_of<llfs::packed_status_test::CustomCodes>()));

  std::vector<char> buffer(required_size);

  {
    llfs::DataPacker packer{llfs::MutableBuffer{buffer.data(), buffer.size()}};

    llfs::PackedStatus* packed_status = llfs::pack_object(status, &packer);
    ASSERT_NE(packed_status, nullptr);

    EXPECT_EQ(llfs::packed_sizeof(*packed_status), required_size);

    EXPECT_THAT(
        batt::to_string(*packed_status),
        ::testing::StrEq("llfs::packed_status_test::CustomCodes{1}:Oh wait, no, that dubious "
                         "distinction belongs to one"));

    EXPECT_EQ(packed_status->code, 1u);
    EXPECT_THAT(packed_status->group.as_str(),
                ::testing::StrEq("llfs::packed_status_test::CustomCodes"));
    EXPECT_THAT(packed_status->message.as_str(), ::testing::StrEq(kMessage1));
  }

  // unpack_cast - Success case
  {
    batt::StatusOr<const llfs::PackedStatus*> unpacked =
        llfs::unpack_cast<llfs::PackedStatus>(buffer);

    ASSERT_TRUE(unpacked.ok());

    EXPECT_THAT(
        batt::to_string(**unpacked),
        ::testing::StrEq("llfs::packed_status_test::CustomCodes{1}:Oh wait, no, that dubious "
                         "distinction belongs to one"));
  }

  // unpack_cast - clip the message/group name
  //
  for (usize n_to_clip = 1; n_to_clip < (required_size - sizeof(llfs::PackedStatus)); ++n_to_clip) {
    batt::StatusOr<const llfs::PackedStatus*> unpacked = llfs::unpack_cast<llfs::PackedStatus>(
        llfs::ConstBuffer{buffer.data(), buffer.size() - n_to_clip});

    EXPECT_EQ(unpacked.status(),
              llfs::make_status(llfs::StatusCode::kUnpackCastPackedBytesDataOver));
  }

  // unpack_cast - clip the PackedStatus struct
  //
  batt::StatusOr<const llfs::PackedStatus*> unpacked = llfs::unpack_cast<llfs::PackedStatus>(
      llfs::ConstBuffer{buffer.data(), sizeof(llfs::PackedStatus) - 1});

  EXPECT_EQ(unpacked.status(), llfs::make_status(llfs::StatusCode::kUnpackCastStructOver));
}

}  // namespace
