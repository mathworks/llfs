//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_SHA256_HPP
#define LLFS_SHA256_HPP

#include <llfs/config.hpp>
//
#include <llfs/int_types.hpp>
#include <llfs/unpack_cast.hpp>

#include <batteries/buffer.hpp>
#include <batteries/operators.hpp>
#include <batteries/optional.hpp>
#include <batteries/seq.hpp>
#include <batteries/seq/decay.hpp>
#include <batteries/shared_ptr.hpp>
#include <batteries/static_assert.hpp>
#include <batteries/suppress.hpp>
#include <batteries/type_traits.hpp>

#include <boost/container_hash/hash.hpp>

#include <openssl/sha.h>

#include <cstring>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief A packable SHA-256 (32-byte) hash.
 */
struct Sha256 {
  /** \brief Parses a 64-character hex string and returns the resulting Sha256 value.
   *
   * \return the parsed Sha256 on success; None if a parsing error occurred.
   */
  static batt::Optional<Sha256> from_str(const std::string_view& s);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief The bytes of the SHA hash.
   */
  std::array<u8, SHA256_DIGEST_LENGTH> bytes;

  /** \brief Default initializes a Sha256 object; this does NOT set the initial contents of
   * `this->bytes`!
   */
  Sha256() = default;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns the raw SHA hash bytes as a binary string; this is NOT the inverse of the
   * Sha256::from_str function, but rather just a simple type conversion.
   */
  std::string_view as_key() const noexcept
  {
    return std::string_view{(const char*)bytes.data(), bytes.size()};
  }
};

// Sanity check: nothing else but the SHA-256 (256-bits == 32-bytes) in class Sha256!
//
BATT_STATIC_ASSERT_EQ(sizeof(Sha256), 32);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/** \brief Prints a Sha256 as a hex string; this is the inverse of Sha256::from_str.
 */
std::ostream& operator<<(std::ostream& out, const Sha256& t);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/** \brief Computes a machine-word-sized hash integer based on the passed Sha256; the returned value
 * is suitable for use in containers like std::unordered_map.
 */
inline usize hash_value(const Sha256& sha)
{
  usize seed = 0xf345f9e32e60e535ull;
  boost::hash_range(seed, sha.bytes.begin(), sha.bytes.end());
  return seed;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/** \brief Equality-comparison of Sha256 objects.
 */
inline bool operator==(const Sha256& l, const Sha256& r)
{
  return !std::memcmp(l.bytes.data(), r.bytes.data(), l.bytes.size());
}

BATT_EQUALITY_COMPARABLE((inline), Sha256, Sha256)

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/** \brief Order-comparison of Sha256 objects.  Uses the byte-wise lexicographical ordering given by
 * std::memcpy (i.e., binary dictionary order).
 */
inline bool operator<(const Sha256& l, const Sha256& r)
{
  return std::memcmp(l.bytes.data(), r.bytes.data(), l.bytes.size()) < 0;
}

BATT_TOTALLY_ORDERED((inline), Sha256, Sha256)

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/** \brief Performs bounds-checking prior to casting a raw pointer to a (packed) Sha256.
 */
inline batt::Status validate_packed_value(const Sha256& packed, const void* buffer_data,
                                          usize buffer_size)
{
  BATT_REQUIRE_OK(llfs::validate_packed_struct(packed, buffer_data, buffer_size));

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BATT_SUPPRESS_IF_GCC("-Wdeprecated-declarations")

/** \brief Computes and returns the SHA-256 hash of the data contained in the passed Seq of
 * ConstBuffer.
 */
template <typename ConstBufferSeq,
          typename = std::enable_if_t<!std::is_convertible_v<ConstBufferSeq&&, batt::ConstBuffer>>>
Sha256 compute_sha256(ConstBufferSeq&& buffers)
{
  Sha256 hash;
  SHA256_CTX ctx;

  SHA256_Init(&ctx);

  for (;;) {
    batt::Optional<batt::ConstBuffer> buffer = buffers.next();
    if (!buffer) {
      break;
    }
    SHA256_Update(&ctx, buffer->data(), buffer->size());
  }

  SHA256_Final(hash.bytes.data(), &ctx);
  return hash;
}

/** \brief Computes and returns the SHA-256 hash of the data contained in the passed buffer.
 */
inline Sha256 compute_sha256(const batt::ConstBuffer& single_buffer)
{
  return compute_sha256(batt::seq::single_item(single_buffer)  //
                        | batt::seq::decayed());
}

BATT_UNSUPPRESS_IF_GCC()

}  // namespace llfs

#endif  // LLFS_SHA256_HPP
