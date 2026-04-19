//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <mutex>
#include <optional>
#include <utility>
#include <vector>
#include "buffer/arc_replacer.h"
#include "common/config.h"
#include "common/macros.h"
#include "storage/disk/disk_scheduler.h"
#include "storage/page/page_guard.h"

namespace bustub {

/**
 * @brief The constructor for a `FrameHeader` that initializes all fields to default values.
 *
 * See the documentation for `FrameHeader` in "buffer/buffer_pool_manager.h" for more information.
 *
 * @param frame_id The frame ID / index of the frame we are creating a header for.
 */
FrameHeader::FrameHeader(frame_id_t frame_id) : frame_id_(frame_id), data_(BUSTUB_PAGE_SIZE, 0) { Reset(); }

/**
 * @brief Get a raw const pointer to the frame's data.
 *
 * @return const char* A pointer to immutable data that the frame stores.
 */
auto FrameHeader::GetData() const -> const char * { return data_.data(); }

/**
 * @brief Get a raw mutable pointer to the frame's data.
 *
 * @return char* A pointer to mutable data that the frame stores.
 */
auto FrameHeader::GetDataMut() -> char * { return data_.data(); }

/**
 * @brief Resets a `FrameHeader`'s member fields.
 */
void FrameHeader::Reset() {
  std::fill(data_.begin(), data_.end(), 0);
  pin_count_.store(0);
  is_dirty_ = false;
}

/**
 * @brief Creates a new `BufferPoolManager` instance and initializes all fields.
 *
 * See the documentation for `BufferPoolManager` in "buffer/buffer_pool_manager.h" for more information.
 *
 * ### Implementation
 *
 * We have implemented the constructor for you in a way that makes sense with our reference solution. You are free to
 * change anything you would like here if it doesn't fit with you implementation.
 *
 * Be warned, though! If you stray too far away from our guidance, it will be much harder for us to help you. Our
 * recommendation would be to first implement the buffer pool manager using the stepping stones we have provided.
 *
 * Once you have a fully working solution (all Gradescope test cases pass), then you can try more interesting things!
 *
 * @param num_frames The size of the buffer pool.
 * @param disk_manager The disk manager.
 * @param log_manager The log manager. Please ignore this for P1.
 */
BufferPoolManager::BufferPoolManager(size_t num_frames, DiskManager *disk_manager, LogManager *log_manager)
    : num_frames_(num_frames),
      next_page_id_(0),
      bpm_latch_(std::make_shared<std::mutex>()),
      replacer_(std::make_shared<ArcReplacer>(num_frames)),
      disk_scheduler_(std::make_shared<DiskScheduler>(disk_manager)),
      log_manager_(log_manager) {
  // Not strictly necessary...
  std::scoped_lock latch(*bpm_latch_);

  // Initialize the monotonically increasing counter at 0.
  next_page_id_.store(0);

  // Allocate all of the in-memory frames up front.
  frames_.reserve(num_frames_);

  // The page table should have exactly `num_frames_` slots, corresponding to exactly `num_frames_` frames.
  page_table_.reserve(num_frames_);

  // Initialize all of the frame headers, and fill the free frame list with all possible frame IDs (since all frames are
  // initially free).
  for (size_t i = 0; i < num_frames_; i++) {
    frames_.push_back(std::make_shared<FrameHeader>(i));
    free_frames_.push_back(static_cast<int>(i));
  }
}

/**
 * @brief Destroys the `BufferPoolManager`, freeing up all memory that the buffer pool was using.
 */
BufferPoolManager::~BufferPoolManager() = default;

/**
 * @brief Returns the number of frames that this buffer pool manages.
 */
auto BufferPoolManager::Size() const -> size_t { return num_frames_; }

/**
 * @brief Allocates a new page on disk.
 *
 * ### Implementation
 *
 * You will maintain a thread-safe, monotonically increasing counter in the form of a `std::atomic<page_id_t>`.
 * See the documentation on [atomics](https://en.cppreference.com/w/cpp/atomic/atomic) for more information.
 *
 * TODO(P1): Add implementation.
 *
 * @return The page ID of the newly allocated page.
 */
auto BufferPoolManager::NewPage() -> page_id_t {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  // 在磁盘上新加一个页
  // next_page_id_ 表示的是下一个还没有使用的 id
  // 我要创建新的页，请给我一个从来没有被分配过的唯一编号
  return next_page_id_.fetch_add(1);
}

/**
 * @brief Removes a page from the database, both on disk and in memory.
 *
 * If the page is pinned in the buffer pool, this function does nothing and returns `false`. Otherwise, this function
 * removes the page from both disk and memory (if it is still in the buffer pool), returning `true`.
 *
 * ### Implementation
 *
 * Think about all of the places that a page or a page's metadata could be, and use that to guide you on implementing
 * this function. You will probably want to implement this function _after_ you have implemented `CheckedReadPage` and
 * `CheckedWritePage`.
 *
 * You should call `DeallocatePage` in the disk scheduler to make the space available for new pages.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to delete.
 * @return `false` if the page exists but could not be deleted, `true` if the page didn't exist or deletion succeeded.
 */
auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  // std::lock_guard<std::mutex> lk(*bpm_latch_);
  bpm_latch_->lock();
  auto it = page_table_.find(page_id);
  if(it == page_table_.end()) {
    // 不在缓存里
    // 说明在磁盘里，直接通过磁盘调用器，删除磁盘空间
    disk_scheduler_->DeallocatePage(page_id);
    bpm_latch_->unlock();
    return true;
  }
  // 否则是在 buffer pool 中的
  auto frame_id = it->second;
  // 那么我要删掉在 buffer pool 中的页，我就需要考虑这个页能不能删了
  // 什么样的情况下不能删，被别人占用了
  // 我也不需要判断是否是脏页了，是脏页也不需要写回磁盘，直接删掉就好
  auto frame = frames_[frame_id];

  if(frame->pin_count_.load() > 0 ) {
    // 说明有人占用
    bpm_latch_->unlock();
    return false;
  }

  // 他没有人占用
  page_table_.erase(it);
  replacer_->Remove(frame_id);
  frame->Reset();
  free_frames_.push_back(frame_id);

  bpm_latch_->unlock();
  // 释放锁后再做磁盘操作，
  disk_scheduler_->DeallocatePage(page_id);
  return true;

}

/**
 * @brief Acquires an optional write-locked guard over a page of data. The user can specify an `AccessType` if needed.
 *
 * If it is not possible to bring the page of data into memory, this function will return a `std::nullopt`.
 *
 * Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are expected to acquire either a
 * `ReadPageGuard` or a `WritePageGuard` depending on the mode in which they would like to access the data, which
 * ensures that any access of data is thread-safe.
 *
 * There can only be 1 `WritePageGuard` reading/writing a page at a time. This allows data access to be both immutable
 * and mutable, meaning the thread that owns the `WritePageGuard` is allowed to manipulate the page's data however they
 * want. If a user wants to have multiple threads reading the page at the same time, they must acquire a `ReadPageGuard`
 * with `CheckedReadPage` instead.
 *
 * ### Implementation
 *
 * There are three main cases that you will have to implement. The first two are relatively simple: one is when there is
 * plenty of available memory, and the other is when we don't actually need to perform any additional I/O. Think about
 * what exactly these two cases entail.
 *
 * The third case is the trickiest, and it is when we do not have any _easily_ available memory at our disposal. The
 * buffer pool is tasked with finding memory that it can use to bring in a page of memory, using the replacement
 * algorithm you implemented previously to find candidate frames for eviction.
 *
 * Once the buffer pool has identified a frame for eviction, several I/O operations may be necessary to bring in the
 * page of data we want into the frame.
 *
 * There is likely going to be a lot of shared code with `CheckedReadPage`, so you may find creating helper functions
 * useful.
 *
 * These two functions are the crux of this project, so we won't give you more hints than this. Good luck!
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The ID of the page we want to write to.
 * @param access_type The type of page access.
 * @return std::optional<WritePageGuard> An optional latch guard where if there are no more free frames (out of memory)
 * returns `std::nullopt`; otherwise, returns a `WritePageGuard` ensuring exclusive and mutable access to a page's data.
 */
auto BufferPoolManager::CheckedWritePage(page_id_t page_id, AccessType access_type) -> std::optional<WritePageGuard> {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  // 这里分为三种情况

  // 先加个锁
  bpm_latch_->lock();

  auto table_node = page_table_.find(page_id);
  if(table_node != page_table_.end()) {
    // 说明 page 已经在 buffer 中
    auto frame_id = table_node->second;
    auto frame = frames_[frame_id];
    frame->page_id_ = page_id;
    frame->pin_count_.fetch_add(1);
    replacer_->SetEvictable(frame_id, false);
    replacer_->RecordAccess(frame_id, page_id, access_type);

    bpm_latch_->unlock();
    // 构造并返回 guard ()， WritePageGuard 构造函数内部会对 frame->rwlatch_ 加写锁
    // Guard 内部存的 bpm_latch_ 和 BPM 里的 bpm_latch_ 指向的是同一个 std::mutex 对象，只是引用计数加了1
    return WritePageGuard(page_id, frames_[frame_id], replacer_, bpm_latch_, disk_scheduler_);

  }
  // page 不在 buffer中，但有空闲frame ， free_frames_ 非空
  if(!free_frames_.empty()) {
    auto frame_id = free_frames_.front();
    free_frames_.pop_front();

    // 建立新映射
    auto frame = frames_[frame_id];
    frame->Reset();

    frame->page_id_ = page_id;
    page_table_[page_id] = frame_id; // 建立起新的映射
    frame->pin_count_.store(1);
    replacer_->SetEvictable(frame_id, false);
    replacer_->RecordAccess(frame_id, page_id, access_type);

    bpm_latch_->unlock(); // 释放锁，再做磁盘 IO
    // 发起磁盘读请求，从磁盘读入页面数据
    auto promise = disk_scheduler_->CreatePromise(); // pomise 是后台线程做完某件事情后，通知前台线程的工具
    auto future = promise.get_future();
    // 构造一个磁盘请求
    std::vector<DiskRequest> requests;
    requests.push_back({false, frame->GetDataMut(), page_id, std::move(promise)}); 
    // 将这个请求交给 disk_scheduler_
    disk_scheduler_->Schedule(requests); 

    future.get(); // 阻塞等待，直到磁盘读取真的完成。

    // 数据已经在 frame 里了，这时返回一个 WritePageGuard
    return WritePageGuard(page_id, frame, replacer_, bpm_latch_, disk_scheduler_);
  }
  // else {
  // 没有空闲 frame，需要驱逐
  auto victim = replacer_->Evict();
  if(!victim.has_value()) {
    return std::nullopt; // 内存不足
  }
  auto frame_id = victim.value();


  // if 被驱逐的页是脏页，需要先写回磁盘
  auto frame = frames_[frame_id];
  if(frame->is_dirty_) {
    // 找到旧的 page_id 
    auto old_page_id = frame->page_id_;

    frame->is_dirty_ = false; // 先清标记，防止并发重复写
    bpm_latch_->unlock(); // 释放锁再做 IO

    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();

    std::vector<DiskRequest> requests;
    requests.push_back({true, frame->GetDataMut(), old_page_id, std::move(promise)});

    disk_scheduler_->Schedule(requests);
    future.get(); // 等待写完成

    bpm_latch_->lock(); // 这里需要重新拿锁
  }
  // 删除旧的 page_table_ 映射
  page_table_.erase(frame->page_id_);

  // 建立新的 page_table_ 映射
  // auto frame
  frame->Reset();
  frame->page_id_ = page_id;
  page_table_[page_id] = frame_id;

  frame->pin_count_.store(1);
  replacer_->RecordAccess(frame_id, page_id, access_type);
  replacer_->SetEvictable(frame_id, false);

  bpm_latch_->unlock();
  // 从磁盘读入页面数据
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  std::vector<DiskRequest> requests;
  requests.push_back({false, frame->GetDataMut(), page_id, std::move(promise)});

  disk_scheduler_->Schedule(requests);
  future.get(); // 等待读完成

  return WritePageGuard(page_id, frame, replacer_, bpm_latch_, disk_scheduler_);
  // }

  // bpm_latch_->unlock();
}

/**
 * @brief Acquires an optional read-locked guard over a page of data. The user can specify an `AccessType` if needed.
 *
 * If it is not possible to bring the page of data into memory, this function will return a `std::nullopt`.
 *
 * Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are expected to acquire either a
 * `ReadPageGuard` or a `WritePageGuard` depending on the mode in which they would like to access the data, which
 * ensures that any access of data is thread-safe.
 *
 * There can be any number of `ReadPageGuard`s reading the same page of data at a time across different threads.
 * However, all data access must be immutable. If a user wants to mutate the page's data, they must acquire a
 * `WritePageGuard` with `CheckedWritePage` instead.
 *
 * ### Implementation
 *
 * See the implementation details of `CheckedWritePage`.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return std::optional<ReadPageGuard> An optional latch guard where if there are no more free frames (out of memory)
 * returns `std::nullopt`; otherwise, returns a `ReadPageGuard` ensuring shared and read-only access to a page's data.
 */
auto BufferPoolManager::CheckedReadPage(page_id_t page_id, AccessType access_type) -> std::optional<ReadPageGuard> {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  // 这里分为三种情况

  // 先加个锁
  bpm_latch_->lock();

  auto table_node = page_table_.find(page_id);
  if(table_node != page_table_.end()) {
    // 说明 page 已经在 buffer 中
    auto frame_id = table_node->second;
    auto frame = frames_[frame_id];
    frame->page_id_ = page_id;
    frame->pin_count_.fetch_add(1);
    replacer_->SetEvictable(frame_id, false);
    replacer_->RecordAccess(frame_id, page_id, access_type);

    bpm_latch_->unlock();
    // 构造并返回 guard ()， WritePageGuard 构造函数内部会对 frame->rwlatch_ 加写锁
    // Guard 内部存的 bpm_latch_ 和 BPM 里的 bpm_latch_ 指向的是同一个 std::mutex 对象，只是引用计数加了1
    return ReadPageGuard(page_id, frame, replacer_, bpm_latch_, disk_scheduler_);

  }
  // page 不在 buffer中，但有空闲frame ， free_frames_ 非空
  if(!free_frames_.empty()) {
    auto frame_id = free_frames_.front();
    free_frames_.pop_front();

    // 建立新映射
    auto frame = frames_[frame_id];
    frame->Reset();

    frame->page_id_ = page_id;
    page_table_[page_id] = frame_id; // 建立起新的映射
    frame->pin_count_.store(1);
    replacer_->SetEvictable(frame_id, false);
    replacer_->RecordAccess(frame_id, page_id, access_type);

    bpm_latch_->unlock(); // 释放锁，再做磁盘 IO
    // 发起磁盘读请求，从磁盘读入页面数据
    auto promise = disk_scheduler_->CreatePromise(); // pomise 是后台线程做完某件事情后，通知前台线程的工具
    auto future = promise.get_future();
    // 构造一个磁盘请求
    std::vector<DiskRequest> requests;
    requests.push_back({false, frame->GetDataMut(), page_id, std::move(promise)}); 
    // 将这个请求交给 disk_scheduler_
    disk_scheduler_->Schedule(requests); 

    future.get(); // 阻塞等待，直到磁盘读取真的完成。

    // 数据已经在 frame 里了，这时返回一个 WritePageGuard
    return ReadPageGuard(page_id, frame, replacer_, bpm_latch_, disk_scheduler_);
  }
  // else {
  // 没有空闲 frame，需要驱逐
  auto victim = replacer_->Evict();
  if(!victim.has_value()) {
    return std::nullopt; // 内存不足
  }
  auto frame_id = victim.value();


  // if 被驱逐的页是脏页，需要先写回磁盘
  auto frame = frames_[frame_id];
  if(frame->is_dirty_) {
    // 找到旧的 page_id 
    auto old_page_id = frame->page_id_;

    frame->is_dirty_ = false; // 先清标记，防止并发重复写
    bpm_latch_->unlock(); // 释放锁再做 IO

    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();

    std::vector<DiskRequest> requests;
    requests.push_back({true, frame->GetDataMut(), old_page_id, std::move(promise)});

    disk_scheduler_->Schedule(requests);
    future.get(); // 等待写完成

    bpm_latch_->lock(); // 这里需要重新拿锁
  }
  // 删除旧的 page_table_ 映射
  page_table_.erase(frame->page_id_);

  // 建立新的 page_table_ 映射
  // auto frame
  frame->Reset();
  frame->page_id_ = page_id;
  page_table_[page_id] = frame_id;

  frame->pin_count_.store(1);
  replacer_->RecordAccess(frame_id, page_id, access_type);
  replacer_->SetEvictable(frame_id, false);

  bpm_latch_->unlock();
  // 从磁盘读入页面数据
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  std::vector<DiskRequest> requests;
  requests.push_back({false, frame->GetDataMut(), page_id, std::move(promise)});

  disk_scheduler_->Schedule(requests);
  future.get(); // 等待读完成

  return ReadPageGuard(page_id, frame, replacer_, bpm_latch_, disk_scheduler_);
  // }

  // bpm_latch_->unlock();
}

/**
 * @brief A wrapper around `CheckedWritePage` that unwraps the inner value if it exists.
 *
 * If `CheckedWritePage` returns a `std::nullopt`, **this function aborts the entire process.**
 *
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer pool
 * manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
 *
 * See the documentation for `CheckedPageWrite` for more information about implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return WritePageGuard A page guard ensuring exclusive and mutable access to a page's data.
 */
auto BufferPoolManager::WritePage(page_id_t page_id, AccessType access_type) -> WritePageGuard {
  auto guard_opt = CheckedWritePage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedWritePage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief A wrapper around `CheckedReadPage` that unwraps the inner value if it exists.
 *
 * If `CheckedReadPage` returns a `std::nullopt`, **this function aborts the entire process.**
 *
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer pool
 * manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
 *
 * See the documentation for `CheckedPageRead` for more information about implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return ReadPageGuard A page guard ensuring shared and read-only access to a page's data.
 */
auto BufferPoolManager::ReadPage(page_id_t page_id, AccessType access_type) -> ReadPageGuard {
  auto guard_opt = CheckedReadPage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedReadPage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief Flushes a page's data out to disk unsafely.
 *
 * This function will write out a page's data to disk if it has been modified. If the given page is not in memory, this
 * function will return `false`.
 *
 * You should not take a lock on the page in this function.
 * This means that you should carefully consider when to toggle the `is_dirty_` bit.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage` and
 * `CheckedWritePage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page to be flushed.
 * @return `false` if the page could not be found in the page table; otherwise, `true`.
 */
auto BufferPoolManager::FlushPageUnsafe(page_id_t page_id) -> bool {
  // 把某个在内存里的页“尽力刷回磁盘” （通常只刷脏页），  但不对该页
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  // std::lock_guard<std::mutex> lk(*bpm_latch_);
  auto it = page_table_.find(page_id);
  if(it == page_table_.end() ) { return false; }

  auto frame = frames_[it->second];
  if(frame->is_dirty_) {
    frame->is_dirty_ = false;
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    std::vector<DiskRequest> requests;
    requests.push_back({true, frame->GetDataMut(), page_id, std::move(promise)});

    disk_scheduler_->Schedule(requests);
    future.get();

  }
  return true;
}

/**
 * @brief Flushes a page's data out to disk safely.
 *
 * This function will write out a page's data to disk if it has been modified. If the given page is not in memory, this
 * function will return `false`.
 *
 * You should take a lock on the page in this function to ensure that a consistent state is flushed to disk.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `Flush` in the page guards, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page to be flushed.
 * @return `false` if the page could not be found in the page table; otherwise, `true`.
 */
auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  std::lock_guard<std::mutex> lk(*bpm_latch_);
  auto it = page_table_.find(page_id);
  if(it == page_table_.end() ) { return false; }

  auto frame = frames_[it->second];
  if(frame->is_dirty_) {
    frame->is_dirty_ = false;
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    std::vector<DiskRequest> requests;
    requests.push_back({true, frame->GetDataMut(), page_id, std::move(promise)});

    disk_scheduler_->Schedule(requests);
    future.get();

  }
  return true;
}

/**
 * @brief Flushes all page data that is in memory to disk unsafely.
 *
 * You should not take locks on the pages in this function.
 * This means that you should carefully consider when to toggle the `is_dirty_` bit.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `FlushPage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 */
void BufferPoolManager::FlushAllPagesUnsafe() {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  std::vector<page_id_t> pages;
  {
    std::lock_guard<std::mutex> lk(*bpm_latch_ );
    for(auto &[page_id, _] : page_table_) {
      pages.emplace_back(page_id);
    }
  }
  for(auto page_id : pages) {
    FlushPageUnsafe(page_id);
  }
}

/**
 * @brief Flushes all page data that is in memory to disk safely.
 *
 * You should take locks on the pages in this function to ensure that a consistent state is flushed to disk.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `FlushPage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 */
void BufferPoolManager::FlushAllPages() {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  std::vector<page_id_t> pages;
  {
    std::lock_guard<std::mutex> lk(*bpm_latch_);
    for(auto &[page_id, _] : page_table_) {
      pages.emplace_back(page_id);
    }
  }
  for(auto page_id : pages) {
    FlushPage(page_id);
  }
}

/**
 * @brief Retrieves the pin count of a page. If the page does not exist in memory, return `std::nullopt`.
 *
 * This function is thread safe. Callers may invoke this function in a multi-threaded environment where multiple threads
 * access the same page.
 *
 * This function is intended for testing purposes. If this function is implemented incorrectly, it will definitely cause
 * problems with the test suite and autograder.
 *
 * # Implementation
 *
 * We will use this function to test if your buffer pool manager is managing pin counts correctly. Since the
 * `pin_count_` field in `FrameHeader` is an atomic type, you do not need to take the latch on the frame that holds the
 * page we want to look at. Instead, you can simply use an atomic `load` to safely load the value stored. You will still
 * need to take the buffer pool latch, however.
 *
 * Again, if you are unfamiliar with atomic types, see the official C++ docs
 * [here](https://en.cppreference.com/w/cpp/atomic/atomic).
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page we want to get the pin count of.
 * @return std::optional<size_t> The pin count if the page exists; otherwise, `std::nullopt`.
 */
auto BufferPoolManager::GetPinCount(page_id_t page_id) -> std::optional<size_t> {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  // 查找对应 page_id 的 pin_count_
  // bpm_latch_
  bpm_latch_->lock();
  auto it = page_table_.find(page_id);
  if(it == page_table_.end()) {
    // 说明这个映射表里没有这个 page_id
    bpm_latch_->unlock();
    return std::nullopt;
  }
  bpm_latch_->unlock();

  return frames_[it->second]->pin_count_.load();
  

}

}  // namespace bustub
