//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_CALLER_HPP
#define LLFS_CALLER_HPP

#include <llfs/int_types.hpp>

#include <boost/preprocessor/cat.hpp>
#include <boost/preprocessor/stringize.hpp>

#include <vector>

namespace llfs {

struct Caller {
#define LLFS_FOR_EACH_CALLER(macro)                                                                \
  macro(EditBatch_get_page_id)                             /*                                 */   \
      macro(EditBatch_into_filtered_leaf)                  /*                                 */   \
      macro(EditBatch_into_leaf)                           /*                                 */   \
      macro(EmptyTreeView_batch_update)                    /*                                 */   \
      macro(LeafView_batch_update)                         /*                                 */   \
      macro(NodeBuilder_flush_update_buffer)               /*                                 */   \
      macro(NodeBuilder_flush_update_buffer_if_necessary)  /*                                 */   \
      macro(NodeBuilder_from)                              /*                                 */   \
      macro(NodeBuilder_insert_batch_to_update_buffer)     /*                                 */   \
      macro(NodeBuilder_merge_disjoint_nodes)              /*                                 */   \
      macro(NodeBuilder_merge_subtree_and_splice)          /*                                 */   \
      macro(NodeBuilder_splice)                            /*                                 */   \
      macro(NodeBuilder_split_if_necessary)                /*                                 */   \
      macro(NodeBuilder_split_pivot_into_subtree_children) /*                                 */   \
      macro(NodeView_batch_update)                         /*                                 */   \
      macro(NodeView_clone_node)                           /*                                 */   \
      macro(NodeView_into_node_builder)                    /*                                 */   \
      macro(NodeView_modify_child_subtree)                 /*                                 */   \
      macro(NodeView_split)                                /*                                 */   \
      macro(NodeView_split_at_pivots)                      /*                                 */   \
      macro(NodeView_split_evenly)                         /*                                 */   \
      macro(PageCacheJob_commit_0)                         /*                                 */   \
      macro(PageCacheJob_commit_1)                         /*                                 */   \
      macro(PageCacheJob_commit_2)                         /*                                 */   \
      macro(PageCacheJob_finalize)                         /*                                 */   \
      macro(PageCacheJob_get_page_ref_count_updates)       /*                                 */   \
      macro(PageCacheJob_new_page)                         /*                                 */   \
      macro(PageCacheJob_pin_new)                          /*                                 */   \
      macro(PageCacheJob_prune)                            /*                                 */   \
      macro(PageCache_allocate_page_of_size)               /*                                 */   \
      macro(PageCache_deallocate_page)                     /*                                 */   \
      macro(PageCache_recycle_page)                        /*                                 */   \
      macro(PageRecycler_recycle_task_main)                /*                                 */   \
      macro(Tablet_checkpoint_committer)                   /*                                 */   \
      macro(Tablet_checkpoint_generator)                   /*                                 */   \
      macro(Tablet_checkpoint_generator_0)                 /*                                 */   \
      macro(Tablet_checkpoint_generator_1)                 /*                                 */   \
      macro(Tablet_checkpoint_reaper_task_main)            /*                                 */   \
      macro(TreeView_from_edits)                           /*                                 */   \
      macro(UpdateBufferLevel_append_batch)                /*                                 */   \
      macro(UpdateBufferLevel_from_batch)                  /*                                 */   \
      macro(UpdateBufferLevel_from_batch_seq)              /*                                 */   \
      macro(UpdateBufferSegmentViewEditBatch_into_leaf)    /*                                 */   \
      macro(UpdateBuffer_insert_batch)                     /*                                 */   \
      macro(merge_disjoint_subtrees)                       /*                                 */   \
      macro(parallel_pack_leaf_pages)                      /*                                 */   \
      macro(EditBatch_get_cached_leaf)                     /*                                 */

  static constexpr u64 Unknown = 0ull;

#define LLFS_CALLER_INDEX(name) BOOST_PP_CAT(name, _INDEX),

  enum { LLFS_FOR_EACH_CALLER(LLFS_CALLER_INDEX) kMaxBits };

#define LLFS_CALLER_MASK(name) static constexpr u64 name = 1ull << BOOST_PP_CAT(name, _INDEX);

  LLFS_FOR_EACH_CALLER(LLFS_CALLER_MASK)

  static std::vector<const char*> get_strings(u64 callers)
  {
    std::vector<const char*> result;
#define LLFS_DUMP_CALLER(name)                                                                     \
  if (callers & name) {                                                                            \
    result.emplace_back(BOOST_PP_STRINGIZE(name));                                                 \
  }

    LLFS_FOR_EACH_CALLER(LLFS_DUMP_CALLER)

    return result;
  }
};

}  // namespace llfs

#endif  // LLFS_CALLER_HPP
