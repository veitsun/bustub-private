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
#include <memory>
#include <optional>
#include "common/config.h"
#include "common/exception.h"
#include "libfort/lib/fort.h"

namespace bustub {

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new ArcReplacer, with lists initialized to be empty and target size to 0
 * @param num_frames the maximum number of frames the ArcReplacer will be required to cache
 */
ArcReplacer::ArcReplacer(size_t num_frames) : replacer_size_(num_frames) {}

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
	// 这里实现的是 ARC 的 REPLACE 操作
	// 从 mru_ 或者 mfu_ 中找到一个 evictable 的 frame 淘汰出去，并把它移入到对应的 ghost list
	if(mru_.size() >= mru_target_size_) {
		// 优先从 mru_ 尾部找 evictable frame 淘汰 → 移入 mru_ghost_ 头部
		auto rit = mru_.rbegin();
		for(; rit != mru_.rend(); ++rit) {
			// 这个mru链表里存的是 frameid
			auto node = alive_map_.find(*rit);
			if(node->second->evictable_) {
				// 找到尾部方向上的第一个可淘汰的节点
				// 执行淘汰
				frame_id_t result =*rit;
				Dieout(node, mru_, mru_ghost_);
				return result;

				// break;
			}
		}
		if(rit == mru_.rend()) {
			// 说明整个 mru_ 都没有 evictable，那么转向到另一侧尝试淘汰，移入对应那侧的 ghost list
			auto rrit = mfu_.rbegin();
			for(; rrit != mfu_.rend(); ++rrit) {
				auto node = alive_map_.find(*rrit);
				if(node->second->evictable_) {
					// 在另一侧找到可淘汰的节点
					// 执行淘汰
					frame_id_t result = *rrit;
					Dieout(node, mfu_, mfu_ghost_);
					return result;

					// break;
				}
			}
		}
		// 到这里说明两侧都被 pin 住
		return std::nullopt;
	}
	else {
		// 从 mfu_ 尾部找 evictable frame 淘汰 → 移入 mfu_ghost_ 头部
		auto rit = mfu_.rbegin();
		for(; rit!= mfu_.rend(); ++rit) {
			// 这个 mfu 链表里存的是 frameid
			auto node = alive_map_.find(*rit);
			if(node->second->evictable_) {
				// 找到尾部方向上的一个可淘汰的节点
				// 执行淘汰
				frame_id_t result = *rit;
				Dieout(node, mfu_, mfu_ghost_);
				return result;

				// break;
			}
		}
		if(rit == mfu_.rend()) {
			// 说明整个 mfu_ 都没有 evictable，那么转向到另一侧尝试淘汰，移入对应那侧的 ghost list
			auto rrit = mru_.rbegin();
			for(; rrit != mru_.rend(); ++rrit) {
				auto node = alive_map_.find(*rrit);
				if(node->second->evictable_) {
					// 在另一侧找到可淘汰的节点
					// 执行淘汰
					frame_id_t result = *rrit;
					Dieout(node, mru_, mru_ghost_);
					return result;

					// break;
				}
			}
		}
		// 到这里说明两侧都被 pin 住了
		return std::nullopt;
	}


	// 如果两侧都全部 pinned，返回 std::nullopt
	// return std::nullopt; 
}



void ArcReplacer::Dieout(std::unordered_map<frame_id_t, std::shared_ptr<FrameStatus>>::const_iterator it, std::list<frame_id_t>&list_, std::list<page_id_t>&list_ghost_) {
	
	list_.erase(it->second->it_);
	list_ghost_.emplace_front(it->second->page_id_);
	// 在 ghost_map_ 中注册
	if(it->second->arc_status_ == ArcStatus::MFU) {
		it->second->arc_status_ = ArcStatus::MFU_GHOST;
	}
	else {
		it->second->arc_status_ = ArcStatus::MRU_GHOST;
	}
	auto ptr = std::make_shared<FrameStatus>(it->second->page_id_, it->second->frame_id_, false, it->second->arc_status_);
	ptr->it_ = list_ghost_.begin();
	// ghost_map_.insert({it->second->page_id_, ptr});
	ghost_map_[it->second->page_id_] = ptr;

	
	alive_map_.erase(it);
	curr_size_ --;

	if(list_ghost_.size() > replacer_size_) {
		// 超出时从尾部删除最旧的条目
		page_id_t pid = list_ghost_.back();
		ghost_map_.erase(pid);
		list_ghost_.pop_back();
		
	}
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
	// printf("mru_.size=%zu mfu_.size=%zu target=%zu\n", mru_.size(), mfu_.size(), mru_target_size_);
	// for (auto f : mfu_) printf("mfu frame=%d\n", f);
	/*
		在 ARC 里，一个页可能处于四种状态。
		这个函数的任务是根据这个 frame_id 当前所处的位置，做一次状态转移
	*/
	// unordered_map 是一个键值对容器，底层通常基于哈希表实现
	auto it = alive_map_.find(frame_id);
	if(it != alive_map_.end()) {
		// 说明这个 alive_map_ 表里有这个目录
		// 将该 frame 从当前列表移除
		// 移到 mfu_ 头部（晋升为"频繁使用"),分两种情况
		if(it->second->arc_status_ == ArcStatus::MFU) {
			mfu_.erase(it->second->it_);
			mfu_.emplace_front(frame_id);
			it->second->it_ = mfu_.begin();
		}
		else if(it->second->arc_status_ == ArcStatus::MRU) {
			mru_.erase(it->second->it_);
			mfu_.emplace_front(frame_id);
			it->second->it_ = mfu_.begin();
			it->second->arc_status_ = ArcStatus::MFU;
		}
		
	}
	else {
		// 这是第一次访问
		// 那么也分三种情况，命中哪个历史,还有就是完全未命中
		auto it_ghost = ghost_map_.find(page_id);
		if(it_ghost == ghost_map_.end()) {
			// 这是完全未命中的情况（新页）
			// Case 4A: mru 侧已满，裁剪 mru_ghost_ 尾部
			if(mru_.size() + mru_ghost_.size() == replacer_size_) {
				if(!mru_ghost_.empty()) {
					ghost_map_.erase(mru_ghost_.back());
					mru_ghost_.pop_back();
				}
			// Case 4B: 四个列表总大小 >= 2c，裁剪 mfu_ghost_ 尾部
			} else if(mru_.size() + mfu_.size() + mru_ghost_.size() + mfu_ghost_.size() >= 2 * replacer_size_) {
				if(!mfu_ghost_.empty()) {
					ghost_map_.erase(mfu_ghost_.back());
					mfu_ghost_.pop_back();
				}
			}
			// 将 frame 加入 mru_ 头部（第一次访问，算"最近使用"）
			mru_.emplace_front(frame_id);
			auto ptr = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MRU);
			ptr->it_ = mru_.begin();
			alive_map_[frame_id] = ptr;
		}
		else {
			auto status = it_ghost->second->arc_status_;
			if(status == ArcStatus::MFU_GHOST) {
				// 频繁使用模式的 页被淘汰太亏啊了，应该扩大 mfu_ 的目标大小
				mru_target_size_ = mru_target_size_ - 1 > 0 ? mru_target_size_ - 1 : 0;
				// 从 mfu_ghost_ 和 ghost_map_ 中删除该页
				mfu_ghost_.erase(it_ghost->second->it_);
				ghost_map_.erase(page_id);
				mfu_.emplace_front(frame_id);

				auto ptr = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MFU);
				// ptr->arc_status_  = ArcStatus::MFU;
				// ptr->frame_id_ = frame_id;
				ptr->it_ = mfu_.begin();
				// ptr->page_id_ = page_id;

				// alive_map_.insert({frame_id, ptr});
				alive_map_[frame_id] = ptr;
			}
			else if (status == ArcStatus::MRU_GHOST) {
				// 说明最近使用模式的页淘汰太快了，应该扩大 mru_ 的目标大小
				mru_target_size_ = mru_target_size_ + 1 < replacer_size_ ? mru_target_size_ + 1 : replacer_size_;
				// 从 mru_ghost_ 和 ghost_map_ 中删除该页
				mru_ghost_.erase(it_ghost->second->it_);
				ghost_map_.erase(page_id);
				mfu_.emplace_front(frame_id);

				auto ptr = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MFU);
				// ptr->arc_status_ = ArcStatus::MFU;
				// ptr->frame_id_ = frame_id;
				ptr->it_ = mfu_.begin();
				// ptr->page_id_ = page_id;

				// alive_map_.insert({frame_id, ptr});
				alive_map_[frame_id] = ptr;
			}
		}

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
	// 设置可淘汰状态
	// 在 alive_map_ 中查找 frame_id ，找不到则说明 frame_id 无效
	auto it = alive_map_.find(frame_id);
	if(it == alive_map_.end()) {
		throw Exception("SetEvictable: frame_id not found");
	}

	auto status = it->second;
	
	// 看 evictable 状态是否发生变化
	if(status->evictable_ == set_evictable) {
		// 没有变化，直接返回
		return ;
	}
	if(set_evictable) {
		curr_size_ ++;
	}
	else {
		curr_size_ --;
	}
	status->evictable_ = set_evictable;
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
	// 强制移除指定 frame
	auto it = alive_map_.find(frame_id);
	if (it == alive_map_.end()) {
		return ;
	}
	if(it->second->evictable_ == false) {
		throw Exception("Remove: this frame is non-evictable");
	}

	if(it->second->arc_status_ == ArcStatus::MRU) {
		mru_.erase(it->second->it_);
	}
	else if(it->second->arc_status_ == ArcStatus::MFU) {
		mfu_.erase(it->second->it_);
	}
	alive_map_.erase(frame_id);
	curr_size_ --;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto ArcReplacer::Size() -> size_t {
	// return 0; 
	return curr_size_;
}

}  // namespace bustub
