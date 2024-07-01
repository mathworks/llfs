//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_TRACK_FDS_HPP
#define LLFS_TRACK_FDS_HPP

#include <llfs/config.hpp>
//
#include <llfs/optional.hpp>
#include <llfs/status.hpp>

#include <boost/stacktrace/stacktrace.hpp>

#include <set>

namespace llfs {

/** \brief Returns the set of currently open file descriptors for this process.
 */
StatusOr<std::set<int>> get_open_fds();

/** \brief If tracking is enabled and fd is not invalid, adds it to the tracked set.
 * \return the passed fd
 */
int maybe_track_fd(int fd);

/** \brief Returns whether fd tracking is currently enabled for this process.
 */
bool is_fd_tracking_enabled();

/** \brief Sets whether fd tracking is currently enabled for this process.
 * \return the previous enabled status
 */
bool set_fd_tracking_enabled(bool on);

/** \brief Returns the stack trace for the most recent open of the passed file descriptor, or None
 * if no such trace exists.
 */
Optional<boost::stacktrace::stacktrace> get_trace_for_fd(int fd);

/** \brief Sets the stack trace for the given file descriptor.
 */
void set_trace_for_fd(int fd, Optional<boost::stacktrace::stacktrace> trace);

}  //namespace llfs

#endif  // LLFS_TRACK_FDS_HPP
