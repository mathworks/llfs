//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -
//
#pragma once
#ifndef LLFS_LOGGING_HPP
#define LLFS_LOGGING_HPP

#include <llfs/config.hpp>

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
#if defined(LLFS_DISABLE_LOGGING)

// Nothing to include!

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
#elif defined(LLFS_USE_GLOG)

#include <glog/logging.h>

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
#elif defined(LLFS_USE_BOOST_LOG)

#include <batteries/assert.hpp>
#include <batteries/suppress.hpp>

BATT_SUPPRESS_IF_GCC("-Wdeprecated-copy")

#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/trivial.hpp>

BATT_UNSUPPRESS_IF_GCC()

#include <boost/preprocessor/cat.hpp>

#include <atomic>
#include <iostream>

#include <errno.h>

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
#elif defined(LLFS_USE_SELF_LOGGING)

#include <chrono>
#include <iostream>

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
#else

#error No Logging Impl Selected!

#endif

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

namespace llfs {

namespace detail {
struct NullStream {
  template <typename Arg>
  const NullStream& operator<<(Arg&&) const noexcept
  {
    return *this;
  }

  const NullStream& operator<<(std::ostream& (*)(std::ostream&)) const noexcept
  {
    return *this;
  }
};
}  // namespace detail

#define LLFS_LOG_NO_OUTPUT()                                                                       \
  if (false)                                                                                       \
  (::llfs::detail::NullStream{})

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
#if defined(LLFS_DISABLE_LOGGING)

#define LLFS_LOG_ERROR() LLFS_LOG_NO_OUTPUT()
#define LLFS_LOG_WARNING() LLFS_LOG_NO_OUTPUT()
#define LLFS_LOG_INFO() LLFS_LOG_NO_OUTPUT()
#define LLFS_LOG_INFO_EVERY_N(n) LLFS_LOG_NO_OUTPUT()
#define LLFS_LOG_INFO_EVERY_T(t) LLFS_LOG_NO_OUTPUT()
#define LLFS_LOG_INFO_IF(condition) LLFS_LOG_NO_OUTPUT()
#define LLFS_VLOG(verbosity) LLFS_LOG_NO_OUTPUT()
#define LLFS_PLOG_ERROR() LLFS_LOG_NO_OUTPUT()
#define LLFS_PLOG_WARNING() LLFS_LOG_NO_OUTPUT()
#define LLFS_PLOG_INFO() LLFS_LOG_NO_OUTPUT()
#define LLFS_LOG_WARNING_IF(condition) LLFS_LOG_NO_OUTPUT()
#define LLFS_LOG_WARNING_FIRST_N(n) LLFS_LOG_NO_OUTPUT()
#define LLFS_LOG_INFO_FIRST_N(n) LLFS_LOG_NO_OUTPUT()
#define LLFS_VLOG_EVERY_N(verbosity, n) LLFS_LOG_NO_OUTPUT()
#define LLFS_VLOG_IF(verbosity, condition) LLFS_LOG_NO_OUTPUT()

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
#elif defined(LLFS_USE_GLOG)

#define LLFS_LOG_ERROR() LOG(ERROR)
#define LLFS_LOG_WARNING() LOG(WARNING)
#define LLFS_LOG_INFO() LOG(INFO)
#define LLFS_LOG_INFO_EVERY_N(n) LOG_EVERY_N(INFO, (n))
#define LLFS_LOG_INFO_EVERY_T(t) LOG_EVERY_T(INFO, (t))
#define LLFS_LOG_INFO_IF(condition) LOG_IF(INFO, (condition))
#define LLFS_VLOG(verbosity) VLOG((verbosity))
#define LLFS_PLOG_ERROR() PLOG(ERROR)
#define LLFS_PLOG_WARNING() PLOG(WARNING)
#define LLFS_PLOG_INFO() PLOG(INFO)
#define LLFS_LOG_WARNING_IF(condition) LOG_IF(WARNING, (condition))
#define LLFS_LOG_WARNING_FIRST_N(n) LOG_FIRST_N(WARNING, (n))
#define LLFS_LOG_INFO_FIRST_N(n) LOG_FIRST_N(INFO, (n))
#define LLFS_VLOG_EVERY_N(verbosity, n) VLOG_EVERY_N((verbosity), (n))
#define LLFS_VLOG_IF(verbosity, condition) VLOG_IF((verbosity), (condition))

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
#elif defined(LLFS_USE_BOOST_LOG)

#define LLFS_LOG_ERROR() BOOST_LOG_TRIVIAL(error)
#define LLFS_LOG_WARNING() BOOST_LOG_TRIVIAL(warning)
#define LLFS_LOG_INFO() BOOST_LOG_TRIVIAL(info)
#define LLFS_VLOG(verbosity) BOOST_LOG_TRIVIAL(debug)

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

inline const bool kBoostLoggingInitialized = [] {
  boost::log::core::get()->set_filter(boost::log::trivial::severity >= boost::log::trivial::info);
  return true;
}();

#elif defined(LLFS_USE_SELF_LOGGING)

inline std::atomic<int> LogSeverityFilter{2};

#define LLFS_LOG_OUTPUT(level_name)                                                                \
  for (bool LlFs_LoG_LooP_FLaG = true; LlFs_LoG_LooP_FLaG;                                         \
       LlFs_LoG_LooP_FLaG = false, std::cerr << std::endl)                                         \
  std::cerr << "["                                                                                 \
            << (std::chrono::duration_cast<std::chrono::microseconds>(                             \
                    std::chrono::steady_clock::now().time_since_epoch())                           \
                    .count())                                                                      \
            << "] " << (level_name) << " "

#define LLFS_LOG_SEVERITY(level, level_name)                                                       \
  if (::llfs::LogSeverityFilter >= (level))                                                        \
  LLFS_LOG_OUTPUT((level_name))

#define LLFS_LOG_ERROR() LLFS_LOG_SEVERITY(0, "ERROR")
#define LLFS_LOG_WARNING() LLFS_LOG_SEVERITY(1, "WARNING")
#define LLFS_LOG_INFO() LLFS_LOG_SEVERITY(2, "INFO")
#define LLFS_VLOG(verbosity) LLFS_LOG_SEVERITY(2 + (verbosity), "INFO")

#endif

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

#if defined(LLFS_USE_SELF_LOGGING) || defined(LLFS_USE_BOOST_LOG)

#define LLFS_LOG_WARNING_IF(condition)                                                             \
  if (condition)                                                                                   \
  LLFS_LOG_WARNING()

//+++++++++++-+-+--+----- --- -- -  -  -   -

#define LLFS_LOG_INFO_FIRST_N(n)                                                                   \
  static std::atomic<int> BOOST_PP_CAT(llfs_log_counter_, __LINE__){(n)};                          \
  if (BOOST_PP_CAT(llfs_log_counter_, __LINE__).fetch_sub(1) > 1)                                  \
  LLFS_LOG_INFO()

#define LLFS_VLOG_EVERY_N(verbosity, n)                                                            \
  static std::atomic<int> BOOST_PP_CAT(llfs_log_counter_, __LINE__){(n)};                          \
  for (; BOOST_PP_CAT(llfs_log_counter_, __LINE__).fetch_sub(1) <= 1;                              \
       BOOST_PP_CAT(llfs_log_counter_, __LINE__).fetch_add((n)))                                   \
  LLFS_VLOG(verbosity)

//+++++++++++-+-+--+----- --- -- -  -  -   -

#define LLFS_PLOG_ERROR() LLFS_LOG_ERROR() << BATT_INSPECT(errno)
#define LLFS_PLOG_WARNING() LLFS_LOG_WARNING() << BATT_INSPECT(errno)
#define LLFS_PLOG_INFO() LLFS_LOG_INFO() << BATT_INSPECT(errno)

#endif  // defined(LLFS_USE_SELF_LOGGING) || defined(LLFS_USE_BOOST_LOG)

//+++++++++++-+-+--+----- --- -- -  -  -   -

#define LLFS_DLOG_ERROR()                                                                          \
  if (::llfs::kDebugBuild)                                                                         \
  LLFS_LOG_ERROR()

#define LLFS_DLOG_WARNING()                                                                        \
  if (::llfs::kDebugBuild)                                                                         \
  LLFS_LOG_WARNING()

#define LLFS_DLOG_INFO()                                                                           \
  if (::llfs::kDebugBuild)                                                                         \
  LLFS_LOG_INFO()

#define LLFS_DVLOG(verbosity)                                                                      \
  if (::llfs::kDebugBuild)                                                                         \
  LLFS_VLOG((verbosity))

}  // namespace llfs

#endif  // LLFS_LOGGING_HPP
