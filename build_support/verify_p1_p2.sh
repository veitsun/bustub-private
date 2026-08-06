#!/usr/bin/env bash
#一键编译并验证 P1 (Buffer Pool)与 P2 (B+Tree) 的全部测试。
#
# 用法：
#   build_support/verify_p1_p2.sh              # 增量编译 + 跑全部
#   build_support/verify_p1_p2.sh --clean      # 删掉 build/ 重新配置
#   build_support/verify_p1_p2.sh p1           # 只跑 P1
#   build_support/verify_p1_p2.sh p2           # 只跑 P2
#
# 说明：
#1. 必须用 clang 工具链。本机 gcc 8.5 没有 libasan 运行库，Debug 模式默认开
#    -fsanitize=address 会在链接期报 "cannot find /usr/lib64/libasan.so.5.0.0"。
#    clang 20 自带 libclang_rt.asan.a，可以正常链接。
#    若一定要用 gcc，就得加-DBUSTUB_SANITIZER=（关掉 sanitizer，见CMakeLists.txt:116）。
# 2. BusTub 的测试默认带 DISABLED_ 前缀，必须加 --gtest_also_run_disabled_tests 才会跑。

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${REPO_ROOT}/build"
JOBS="$(nproc)"

P1_TESTS=(
  disk_manager_test
  disk_scheduler_test
  arc_replacer_test
  page_guard_test
  buffer_pool_manager_test
)

P2_TESTS=(
  b_plus_tree_insert_test
  b_plus_tree_delete_test
  b_plus_tree_tombstone_test
  b_plus_tree_sequential_scale_test
  b_plus_tree_concurrent_test
)

WHICH="all"
for arg in "$@"; do
  case "$arg" in
    --clean) rm -rf "${BUILD_DIR}" ;;
    p1) WHICH="p1" ;;
    p2) WHICH="p2" ;;
    *) echo "unknown arg: $arg" >&2; exit 2 ;;
  esac
done

TARGETS=()
case "$WHICH" in
  p1)  TARGETS=("${P1_TESTS[@]}") ;;
  p2)  TARGETS=("${P2_TESTS[@]}") ;;
  all) TARGETS=("${P1_TESTS[@]}" "${P2_TESTS[@]}") ;;
esac

# ── 配置 ──
if [[ ! -f "${BUILD_DIR}/build.ninja" ]]; then
  echo "==> 配置 CMake (clang + Ninja + Debug/ASan)"
  mkdir -p "${BUILD_DIR}"
  ( cd "${BUILD_DIR}" && CC=clang CXX=clang++ cmake -G Ninja -DCMAKE_BUILD_TYPE=Debug .. ) \
    || { echo "cmake 配置失败"; exit 1; }
fi

# ── 编译 ──
echo "==> 编译 ${#TARGETS[@]} 个目标 (-j ${JOBS})"
( cd "${BUILD_DIR}" && ninja -j "${JOBS}" "${TARGETS[@]}" ) \
  || { echo "编译失败"; exit 1; }

# ── 运行 ──
cd "${BUILD_DIR}/test"
pass_total=0
fail_total=0
declare -a summary

run_group() {
  local label="$1"; shift
  echo
  echo "########## ${label} ##########"
  for t in "$@"; do
    echo "──────── ${t} ────────"
    local log="/tmp/${t}.runlog"
    timeout 900 "./${t}" --gtest_also_run_disabled_tests --gtest_brief=1 > "${log}" 2>&1
    local rc=$?
    # 从 gtest 输出里抓通过/失败数
    local p f
    p=$(grep -oE '\[  PASSED  \] [0-9]+' "${log}" | tail -1 | grep -oE '[0-9]+')
    f=$(grep -oE '\[  FAILED  \] [0-9]+ test' "${log}" | tail -1 | grep -oE '[0-9]+')
    p=${p:-0}; f=${f:-0}
    if [[ $rc -eq 124 ]]; then
      echo "  ⏱  TIMEOUT (900s) —— 可能死锁"
      summary+=("${t}: TIMEOUT")
      fail_total=$((fail_total + 1))
      continue
    fi
    pass_total=$((pass_total + p))
    fail_total=$((fail_total + f))
    grep -E '^\[  (PASSED|FAILED)' "${log}" | tail -5
    grep -E '^\[  FAILED  \] [A-Za-z]' "${log}" | sed 's/^/     /'
    summary+=("${t}: ${p} passed, ${f} failed")
  done
}

[[ "$WHICH" == "all" || "$WHICH" == "p1" ]] && run_group "P1Buffer Pool Manager" "${P1_TESTS[@]}"
[[ "$WHICH" == "all" || "$WHICH" == "p2" ]] && run_group "P2  B+Tree Index" "${P2_TESTS[@]}"

echo
echo "================== 汇总 =================="
for line in "${summary[@]}"; do echo "  ${line}"; done
echo "------------------------------------------"
echo "  合计: ${pass_total} passed, ${fail_total} failed"
echo "  单个测试的完整日志: /tmp/<target>.runlog"
[[ ${fail_total} -eq 0 ]] && echo "  ✅ 全部通过" || echo "  ❌ 存在失败"
exit $(( fail_total == 0 ? 0 : 1 ))
