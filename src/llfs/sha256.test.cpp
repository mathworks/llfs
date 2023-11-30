//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/sha256.hpp>
//
#include <llfs/sha256.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <boost/functional/hash.hpp>

#include <unordered_set>

namespace {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief Test fixture for Sha256 tests.
 */
class Sha256Test : public ::testing::Test
{
 public:
  /** \brief A hash set of Sha256 values.
   */
  std::unordered_set<llfs::Sha256, boost::hash<llfs::Sha256>> sha_set;

  /** \brief Calculate the SHA-256 of a known string from multiple parts.
   */
  llfs::Sha256 sha_a1 = llfs::compute_sha256(batt::as_seq(std::vector<batt::ConstBuffer>{
      boost::asio::buffer("hello,"),
      boost::asio::buffer(" world"),
  }));

  /** \brief Calculate the SHA-256 of a known string from a single part.
   */
  llfs::Sha256 sha_a2 = llfs::compute_sha256(boost::asio::buffer("hello,\0 world"));

  /** \brief Calculate the SHA-256 of a different known string.
   */
  llfs::Sha256 sha_b = llfs::compute_sha256(boost::asio::buffer("adios, amigos!"));
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(Sha256Test, ToString)
{
  EXPECT_THAT(batt::to_string(this->sha_a1),
              ::testing::StrEq("7bf81346f91f572c21001dfab42a8471842e3c32f16ce699253c18ef7e9986a6"));

  EXPECT_THAT(batt::to_string(this->sha_a2),
              ::testing::StrEq("7bf81346f91f572c21001dfab42a8471842e3c32f16ce699253c18ef7e9986a6"));

  EXPECT_THAT(batt::to_string(this->sha_b),
              ::testing::StrEq("22891753903ba82d35cb14e4de73b1f6b6a49dd75676c12e1568e652e1f80a01"));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(Sha256Test, Equality)
{
  EXPECT_EQ(sha_a1, sha_a2);
  EXPECT_EQ(sha_a2, sha_a1);
  EXPECT_NE(sha_b, sha_a1);
  EXPECT_NE(sha_a2, sha_b);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(Sha256Test, Ordering)
{
  EXPECT_LE(sha_a1, sha_a2);
  EXPECT_LE(sha_a2, sha_a1);
  EXPECT_GE(sha_a1, sha_a2);
  EXPECT_GE(sha_a2, sha_a1);
  EXPECT_LT(sha_b, sha_a1);
  EXPECT_LE(sha_b, sha_a1);
  EXPECT_GT(sha_a1, sha_b);
  EXPECT_GE(sha_a1, sha_b);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(Sha256Test, HashValue)
{
  // Insert sha_a1 into the set.
  //
  EXPECT_EQ(this->sha_set.count(sha_a1), 0u);

  this->sha_set.insert(sha_a1);

  EXPECT_EQ(this->sha_set.size(), 1u);
  EXPECT_EQ(this->sha_set.count(sha_a1), 1u);

  // Insert sha_a2 into the set; because it is the same as sha_a1, expect no changes.
  //
  this->sha_set.insert(sha_a2);

  EXPECT_EQ(this->sha_set.size(), 1u);
  EXPECT_EQ(this->sha_set.count(sha_a2), 1u);

  // Insert sha_b into the set.
  //
  EXPECT_EQ(this->sha_set.count(sha_b), 0u);

  this->sha_set.insert(sha_b);

  EXPECT_EQ(this->sha_set.size(), 2u);
  EXPECT_EQ(this->sha_set.count(sha_b), 1u);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(Sha256Test, FromString)
{
  batt::Optional<llfs::Sha256> maybe_sha_a3 = llfs::Sha256::from_str(batt::to_string(sha_a1));

  ASSERT_TRUE(maybe_sha_a3);

  const llfs::Sha256& sha_a3 = *maybe_sha_a3;

  EXPECT_EQ(sha_a3, sha_a1);

  // From upper case.
  //
  batt::Optional<llfs::Sha256> maybe_sha_b2 =
      llfs::Sha256::from_str("22891753903BA82D35CB14E4DE73B1F6B6A49DD75676C12E1568E652E1F80A01");

  const llfs::Sha256& sha_b2 = *maybe_sha_b2;

  EXPECT_EQ(sha_b2, sha_b);

  // Negative case 1: bad character.
  //
  batt::Optional<llfs::Sha256> bad_1 =
      llfs::Sha256::from_str("22891753903ba82d35cb14e4de73b1f6b6a49dd75676_12e1568e652e1f80a01");

  EXPECT_FALSE(bad_1);

  // Negative case 2a: too short, one char.
  //
  batt::Optional<llfs::Sha256> bad_2 =
      llfs::Sha256::from_str("22891753903ba82d35cb14e4de73b1f6b6a49dd75676c12e1568e652e1f80a0");

  EXPECT_FALSE(bad_2);

  // Negative case 2b: too short, one digit.
  //
  batt::Optional<llfs::Sha256> bad_3 =
      llfs::Sha256::from_str("22891753903ba82d35cb14e4de73b1f6b6a49dd75676c12e1568e652e1f80a");

  EXPECT_FALSE(bad_3);

  // Negative case 2b: too short, empty string
  //
  batt::Optional<llfs::Sha256> bad_4 = llfs::Sha256::from_str("");

  EXPECT_FALSE(bad_4);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(Sha256Test, AsKey)
{
  std::string_view key = this->sha_a1.as_key();

  EXPECT_EQ(key.size(), 32u);
  EXPECT_EQ(std::memcmp(key.data(), &this->sha_a1, key.size()), 0);
  EXPECT_EQ((const void*)key.data(), (const void*)(&this->sha_a1));
}

}  // namespace
