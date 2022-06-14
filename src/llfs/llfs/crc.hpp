#pragma once
#ifndef LLFS_CRC_HPP
#define LLFS_CRC_HPP

#include <boost/crc.hpp>

namespace llfs {

boost::crc_basic<64> make_crc64();

}  // namespace llfs

#endif  // LLFS_CRC_HPP
