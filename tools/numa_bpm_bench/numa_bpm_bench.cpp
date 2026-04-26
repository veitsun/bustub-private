//===----------------------------------------------------------------------===//
//
// numa_bpm_bench.cpp
//
// Benchmark comparing NUMA INTERLEAVE vs BIND allocation policies for the
// BusTub buffer pool manager.
//
// Usage:
//   ./bustub-numa-bpm-bench [--duration <ms>] [--frames <n>]
//                           [--threads <n>] [--pages <n>]
//
// The benchmark runs two back-to-back phases (INTERLEAVE then BIND) and
// reports throughput (page accesses / second) for each.
//
//===----------------------------------------------------------------------===//

#include <numa.h>
#include <sched.h>
#include <sys/time.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "argparse/argparse.hpp"
#include "buffer/numa_buffer_pool_manager.h"
#include "fmt/core.h"
#include "storage/disk/disk_manager_memory.h"

using namespace bustub;  // NOLINT

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

static auto NowMs() -> uint64_t {
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  return static_cast<uint64_t>(tv.tv_sec) * 1000ULL + static_cast<uint64_t>(tv.tv_usec) / 1000ULL;
}

// ─────────────────────────────────────────────────────────────────────────────
// Benchmark runner
// ─────────────────────────────────────────────────────────────────────────────

struct BenchResult {
  std::string policy_name;
  double ops_per_sec;
};

/**
 * Run a mixed read/write workload against a NumaBufferPoolManager.
 *
 * Each worker thread repeatedly picks a random page_id in [0, num_pages),
 * acquires a ReadPageGuard (80 % of the time) or WritePageGuard (20 %), and
 * releases it immediately.  We count total page accesses over the duration.
 *
 * @param policy       INTERLEAVE or BIND
 * @param bind_node    NUMA node for BIND policy (ignored for INTERLEAVE)
 * @param num_frames   Buffer pool size in pages
 * @param num_pages    Working-set size (pages on disk)
 * @param num_threads  Worker thread count
 * @param duration_ms  How long to run (milliseconds)
 */
static auto RunBench(NumaPolicy policy, int bind_node, size_t num_frames, size_t num_pages, int num_threads,
                     uint64_t duration_ms) -> BenchResult {
  const std::string policy_name = (policy == NumaPolicy::INTERLEAVE) ? "INTERLEAVE" : "BIND";

  auto disk = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<NumaBufferPoolManager>(num_frames, disk.get(), policy, bind_node);

  // Pre-allocate pages so reads don't always hit cold disk.
  for (size_t i = 0; i < std::min(num_pages, num_frames); ++i) {
    auto pid = bpm->NewPage();
    auto guard = bpm->CheckedWritePage(pid);
    (void)guard;
  }

  std::atomic<uint64_t> total_ops{0};
  std::atomic<bool> stop{false};

  auto worker = [&](int /*tid*/) {
    std::mt19937_64 rng(std::random_device{}());
    std::uniform_int_distribution<page_id_t> page_dist(0, static_cast<page_id_t>(num_pages) - 1);
    std::uniform_int_distribution<int> rw_dist(0, 4);  // 0 = write, 1-4 = read

    uint64_t local_ops = 0;
    while (!stop.load(std::memory_order_relaxed)) {
      auto pid = page_dist(rng);
      if (rw_dist(rng) == 0) {
        auto guard = bpm->CheckedWritePage(pid);
        if (guard.has_value()) {
          ++local_ops;
        }
      } else {
        auto guard = bpm->CheckedReadPage(pid);
        if (guard.has_value()) {
          ++local_ops;
        }
      }
    }
    total_ops.fetch_add(local_ops, std::memory_order_relaxed);
  };

  std::vector<std::thread> threads;
  threads.reserve(num_threads);

  auto t0 = NowMs();
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back(worker, i);
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(duration_ms));
  stop.store(true, std::memory_order_relaxed);

  for (auto &t : threads) {
    t.join();
  }
  auto elapsed_ms = NowMs() - t0;

  double ops_per_sec = static_cast<double>(total_ops.load()) / (static_cast<double>(elapsed_ms) / 1000.0);

  return {policy_name, ops_per_sec};
}

// ─────────────────────────────────────────────────────────────────────────────
// main
// ─────────────────────────────────────────────────────────────────────────────

auto main(int argc, char *argv[]) -> int {
  if (numa_available() < 0) {
    fmt::print(stderr, "NUMA is not available on this system.\n");
    return 1;
  }

  int num_nodes = numa_num_configured_nodes();
  fmt::print("NUMA nodes detected: {}\n", num_nodes);

  argparse::ArgumentParser args("bustub-numa-bpm-bench");
  args.add_argument("--duration").default_value(uint64_t{5000}).scan<'u', uint64_t>().help("duration per phase (ms)");
  args.add_argument("--frames").default_value(size_t{4096}).scan<'u', size_t>().help("buffer pool frames");
  args.add_argument("--pages").default_value(size_t{8192}).scan<'u', size_t>().help("working-set pages");
  args.add_argument("--threads").default_value(int{8}).scan<'i', int>().help("worker threads");
  args.add_argument("--bind-node").default_value(int{0}).scan<'i', int>().help("NUMA node for BIND policy");

  try {
    args.parse_args(argc, argv);
  } catch (const std::exception &e) {
    fmt::print(stderr, "Argument error: {}\n", e.what());
    return 1;
  }

  auto duration_ms = args.get<uint64_t>("--duration");
  auto num_frames = args.get<size_t>("--frames");
  auto num_pages = args.get<size_t>("--pages");
  auto num_threads = args.get<int>("--threads");
  auto bind_node = args.get<int>("--bind-node");

  fmt::print("Config: frames={}, pages={}, threads={}, duration={}ms, bind_node={}\n", num_frames, num_pages,
             num_threads, duration_ms, bind_node);
  fmt::print("\n");

  // ── Phase 1: INTERLEAVE ──────────────────────────────────────────────────
  fmt::print("[1/2] Running INTERLEAVE policy ...\n");
  auto r1 = RunBench(NumaPolicy::INTERLEAVE, bind_node, num_frames, num_pages, num_threads, duration_ms);
  fmt::print("  INTERLEAVE throughput: {:.0f} ops/sec\n\n", r1.ops_per_sec);

  // ── Phase 2: BIND ────────────────────────────────────────────────────────
  fmt::print("[2/2] Running BIND policy (node {}) ...\n", bind_node);
  auto r2 = RunBench(NumaPolicy::BIND, bind_node, num_frames, num_pages, num_threads, duration_ms);
  fmt::print("  BIND throughput:        {:.0f} ops/sec\n\n", r2.ops_per_sec);

  // ── Summary ──────────────────────────────────────────────────────────────
  fmt::print("=== Summary ===\n");
  fmt::print("  INTERLEAVE : {:.0f} ops/sec\n", r1.ops_per_sec);
  fmt::print("  BIND       : {:.0f} ops/sec\n", r2.ops_per_sec);

  double ratio = r1.ops_per_sec / r2.ops_per_sec;
  if (ratio > 1.0) {
    fmt::print("  INTERLEAVE is {:.2f}x faster than BIND\n", ratio);
  } else {
    fmt::print("  BIND is {:.2f}x faster than INTERLEAVE\n", 1.0 / ratio);
  }

  return 0;
}
