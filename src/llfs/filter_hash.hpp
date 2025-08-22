//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once

#include <llfs/config.hpp>
//

#include <llfs/int_types.hpp>
#include <llfs/key.hpp>

#include <batteries/math.hpp>
#include <batteries/static_assert.hpp>

#include <xxhash.h>

#include <array>
#include <functional>
#include <type_traits>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// Implementation details - NOT FOR EXTERNAL USE
//
namespace detail {

/** \brief A set of randomly chosen (by hardware entropy generator) seeds for up to 64 different
 * hash functions to use for building and querying  filters.
 */
inline constexpr std::array<u64, 64> kFilterHashSeeds = {
    0xce3a9eb8b885d5afull, 0x33d9975b8a739ac6ull, 0xe65d0fff49425f03ull, 0x10bb3a132ec4fabcull,
    0x88d476f6e7f2c53cull, 0xcb4905c588217f44ull, 0x54eb7b8b55ac05d6ull, 0xac0de731d7f3f97cull,
    0x998963e5d908c156ull, 0x0bdf939d3b7c1cd6ull, 0x2cf7007c36b2c966ull, 0xb53c35171f25ccceull,
    0x7d6d2ad5e3ef7ae3ull, 0xe3aaa3bf1dbffd08ull, 0xa81f70b4f8dc0f80ull, 0x1f4887ce81cdf25aull,
    0x6433a69ba9e9d9b1ull, 0xf859167265201651ull, 0xe48c6589be0ff660ull, 0xadd5250ba0e7ac09ull,
    0x833f55b86dee015full, 0xae3b000feb85dceaull, 0x0110cfeb4fe23291ull, 0xf3a5d699ab2ce23cull,
    0x7c3a2b8a1c43942cull, 0x8cb3fb6783724d25ull, 0xe3619c66bf3aa139ull, 0x3fdf358be099c7d9ull,
    0x0c38ccabc94a487full, 0x43e19e80ee4fe6edull, 0x22699c9fc26f20eeull, 0xa559cbafff2cea37ull,
    0xfbed4777b17fb16dull, 0x7197788291858011ull, 0xa9325a240f0d996eull, 0x6782b2e3766f2f76ull,
    0xbc3aca45c9d9dc36ull, 0x7b687762afe92061ull, 0x7b2a7cb985790bcfull, 0xf244ed1bc2b06f7dull,
    0x29acd54ff9cb3809ull, 0xe1926523e6f67949ull, 0x98f964fbc223bb91ull, 0xaab5ee47827c5506ull,
    0x0dab726106a4c8ddull, 0xa88bb10b8e57cdd9ull, 0xbef7ede281a687afull, 0x0e2a6b9bc5b7d6e3ull,
    0x5b6f250b605200c8ull, 0xafe46bbd0e81722full, 0xb5d978e72ac594daull, 0x8c4362498b85fff9ull,
    0xce8cd0d29a933471ull, 0x9c2a28aabd1e71cbull, 0x572c8c1d4ea24d86ull, 0x8fc7dff3afb5fbf7ull,
    0xf378bc6c41606bf9ull, 0xa4c36401cf7a557full, 0x0b0a5bdd27f682afull, 0x3fbe0f66ef4777c1ull,
    0x0ed678ccbd246356ull, 0xc2d3489afc4edcd6ull, 0xc482a884240966c6ull, 0x19b952db37267518ull,
};

// Validate assumption that the number of seeds above is a power of 2.
//
namespace {

BATT_STATIC_ASSERT_EQ(u64{1} << (batt::log2_ceil(kFilterHashSeeds.size())),
                      kFilterHashSeeds.size());

}  //namespace
}  //namespace detail
//
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

/** \brief Returns the n-th hash function for the given integer value.
 */
inline u64 get_nth_hash_for_filter(usize int_value, usize n)
{
  return XXH64(&int_value, sizeof(int_value),
               detail::kFilterHashSeeds[n & (detail::kFilterHashSeeds.size() - 1)]);
}

/** \brief Returns the n-th hash function for the given string value.
 */
inline u64 get_nth_hash_for_filter(const KeyView& str, usize n)
{
  return XXH64(str.data(), str.size(),
               detail::kFilterHashSeeds[n & (detail::kFilterHashSeeds.size() - 1)]);
}

/** \brief Returns the n-th hash function for the given value.
 *
 * This is the generic overload of this function; it uses std::hash<T> to calculate a hash value,
 * then hashes that value again using xxhash to obtain the n-th hash function (for  filters).
 */
template <typename T, typename = std::enable_if_t<!std::is_convertible_v<const T&, usize> &&
                                                  !std::is_convertible_v<const T&, const KeyView&>>>
inline u64 get_nth_hash_for_filter(const T& item, usize n)
{
  return get_nth_hash_for_filter(std::hash<T>{}(item), n);
}

}  //namespace llfs

