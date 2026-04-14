// :bustub-keep-private:
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// arc_replacer.cpp
//
// Identification: src/buffer/arc_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/arc_replacer.h"
#include <optional>
#include "common/config.h"

namespace bustub {

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new ArcReplacer, with lists initialized to be empty and target size to 0
 * @param num_frames the maximum number of frames the ArcReplacer will be required to cache
 */
ArcReplacer::ArcReplacer(size_t num_frames) : replacer_size_(num_frames) { replacer_size_ = num_frames; }

/**
 * TODO(P1): Add implementation
 *
 * @brief Performs the Replace operation as described by the writeup
 * that evicts from either mfu_ or mru_ into its corresponding ghost list
 * according to balancing policy.
 *
 * If you wish to refer to the original ARC paper, please note that there are
 * two changes in our implementation:
 * 1. When the size of mru_ equals the target size, we don't check
 * the last access as the paper did when deciding which list to evict from.
 * This is fine since the original decision is stated to be arbitrary.
 * 2. Entries that are not evictable are skipped. If all entries from the desired side
 * (mru_ / mfu_) are pinned, we instead try victimize the other side (mfu_ / mru_),
 * and move it to its corresponding ghost list (mfu_ghost_ / mru_ghost_).
 *
 * @return frame id of the evicted frame, or std::nullopt if cannot evict
 */
auto ArcReplacer::Evict() -> std::optional<frame_id_t> {
  std::lock_guard<std::mutex> lock(latch_);

  if (mru_.size() >= mru_target_size_) {
    if (auto v = TryEvict(mru_, mru_ghost_, mru_ghost_map)) return v;
    return TryEvict(mfu_, mfu_ghost_, mfu_ghost_map);
  } else {
    if (auto v = TryEvict(mfu_, mfu_ghost_, mfu_ghost_map)) return v;
    return TryEvict(mru_, mru_ghost_, mru_ghost_map);
  }
}

std::optional<frame_id_t> ArcReplacer::TryEvict(
    std::list<frame_id_t> &list, std::list<page_id_t> &ghost_list,
    std::unordered_map<page_id_t, std::list<page_id_t>::iterator> &ghost_map) {
  for (auto it = list.rbegin(); it != list.rend(); it++) {
    auto map_it = alive_map_.find(*it);
    if (map_it != alive_map_.end() && map_it->second->evictable_) {
      frame_id_t fid = map_it->second->frame_id_;
      page_id_t pid = map_it->second->page_id_;
      list.erase(std::next(it).base());
      ghost_list.push_front(pid);
      ghost_map[pid] = ghost_list.begin();
      alive_map_.erase(fid);
      curr_size_--;
      return fid;
    }
  }
  return std::nullopt;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Record access to a frame, adjusting ARC bookkeeping accordingly
 * by bring the accessed page to the front of mfu_ if it exists in any of the lists
 * or the front of mru_ if it does not.
 *
 * Performs the operations EXCEPT REPLACE described in original paper, which is
 * handled by `Evict()`.
 *
 * Consider the following four cases, handle accordingly:
 * 1. Access hits mru_ or mfu_
 * 2/3. Access hits mru_ghost_ / mfu_ghost_
 * 4. Access misses all the lists
 *
 * This routine performs all changes to the four lists as preperation
 * for `Evict()` to simply find and evict a victim into ghost lists.
 *
 * Note that frame_id is used as identifier for alive pages and
 * page_id is used as identifier for the ghost pages, since page_id is
 * the unique identifier to the page after it's dead.
 * Using page_id for alive pages should be the same since it's one to one mapping,
 * but using frame_id is slightly more intuitive.
 *
 * @param frame_id id of frame that received a new access.
 * @param page_id id of page that is mapped to the frame.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void ArcReplacer::RecordAccess(frame_id_t frame_id, page_id_t page_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);
  auto it = alive_map_.find(frame_id);
  auto mru_g_it = mru_ghost_map.find(page_id);
  auto mfu_g_it = mfu_ghost_map.find(page_id);
  if (it != alive_map_.end()) {
    if (it->second->arc_status_ == ArcStatus::MRU) {
      mfu_.splice(mfu_.begin(), mru_, it->second->iter_);
      it->second->arc_status_ = ArcStatus::MFU;
    } else {
      mfu_.splice(mfu_.begin(), mfu_, it->second->iter_);
    }
    return;
  } else if (mru_g_it != mru_ghost_map.end() || mfu_g_it != mfu_ghost_map.end()) {
    if (mru_g_it != mru_ghost_map.end()) {
      if (mru_ghost_.size() >= mfu_ghost_.size()) {
        mru_target_size_++;
        if (mru_target_size_ > replacer_size_) mru_target_size_ = replacer_size_;
      } else {
        mru_target_size_ += mfu_ghost_.size() / mru_ghost_.size();
        if (mru_target_size_ > replacer_size_) mru_target_size_ = replacer_size_;
      }
      mru_ghost_.erase(mru_g_it->second);
      mfu_.push_front(frame_id);
      alive_map_[frame_id] = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MFU);
      alive_map_[frame_id]->iter_ = mfu_.begin();
      mru_ghost_map.erase(mru_g_it);
      return;
    } else {
      size_t delta = (mfu_ghost_.size() >= mru_ghost_.size()) ? 1 : (mru_ghost_.size() / mfu_ghost_.size());
      if (mru_target_size_ < delta) {
        mru_target_size_ = 0;
      } else {
        mru_target_size_ -= delta;
      }
      mfu_ghost_.erase(mfu_g_it->second);
      mfu_.push_front(frame_id);
      alive_map_[frame_id] = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MFU);  // 修复 3
      alive_map_[frame_id]->iter_ = mfu_.begin();
      mfu_ghost_map.erase(mfu_g_it);
      return;
    }
  } else {
    if (mru_.size() + mru_ghost_.size() == replacer_size_) {
      mru_ghost_map.erase(mru_ghost_.back());
      mru_ghost_.pop_back();
    } else if (mru_.size() + mru_ghost_.size() + mfu_.size() + mfu_ghost_.size() >= 2 * replacer_size_) {
      mfu_ghost_map.erase(mfu_ghost_.back());
      mfu_ghost_.pop_back();
    }
    mru_.push_front(frame_id);
    alive_map_[frame_id] = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MRU);
    alive_map_[frame_id]->iter_ = mru_.begin();
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void ArcReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);

  auto it = alive_map_.find(frame_id);
  if (it == alive_map_.end()) {
    abort();
  }

  auto fs = it->second;
  if (fs->evictable_ && !set_evictable) {
    curr_size_--;
  } else if (!fs->evictable_ && set_evictable) {
    curr_size_++;
  }

  fs->evictable_ = set_evictable;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from replacer.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * decided by the ARC algorithm.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void ArcReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  auto it = alive_map_.find(frame_id);
  if (it == alive_map_.end()) return;

  // TODO: clean mru/mfu/ghost;
  auto &fs = it->second;
  if (!fs->evictable_) {
    abort();
  } else {
    curr_size_--;
    if (fs->arc_status_ == ArcStatus::MRU)
      mru_.erase(fs->iter_);
    else
      mfu_.erase(fs->iter_);
    alive_map_.erase(it);
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto ArcReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
