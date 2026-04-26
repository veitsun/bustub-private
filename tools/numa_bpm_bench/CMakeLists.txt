set(NUMA_BPM_BENCH_SOURCES numa_bpm_bench.cpp)
add_executable(numa-bpm-bench ${NUMA_BPM_BENCH_SOURCES} "${PROJECT_SOURCE_DIR}/tools/backtrace.cpp")
add_backward(numa-bpm-bench)

target_link_libraries(numa-bpm-bench bustub numa)
set_target_properties(numa-bpm-bench PROPERTIES OUTPUT_NAME bustub-numa-bpm-bench)
