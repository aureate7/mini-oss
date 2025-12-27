#!/usr/bin/env bash
set -euo pipefail

# 脚本所在目录：service_proto
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# proto 根目录：service_proto/proto
PROTO_ROOT="${SCRIPT_DIR}/proto"

# 输出目录：service_proto/pb
OUT_DIR="${SCRIPT_DIR}/pb"

# 依赖检查
command -v protoc >/dev/null 2>&1 || { echo "❌ protoc 未安装或不在 PATH"; exit 1; }
command -v protoc-gen-go >/dev/null 2>&1 || { echo "❌ protoc-gen-go 未安装：go install google.golang.org/protobuf/cmd/protoc-gen-go@latest"; exit 1; }
command -v protoc-gen-go-grpc >/dev/null 2>&1 || { echo "❌ protoc-gen-go-grpc 未安装：go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest"; exit 1; }

mkdir -p "${OUT_DIR}"

# 额外 include（解决 google/protobuf/timestamp.proto 在某些环境找不到）
EXTRA_INCLUDES=()
for d in \
  "$(brew --prefix protobuf 2>/dev/null)/include" \
  "/opt/homebrew/include" \
  "/usr/local/include" \
  "/usr/include"
do
  if [[ -n "${d}" && -d "${d}" ]]; then
    EXTRA_INCLUDES+=("-I" "${d}")
  fi
done

echo "📦 编译 proto 文件..."

find "${PROTO_ROOT}" -name "*.proto" -print0 | xargs -0 protoc \
  -I "${PROTO_ROOT}" \
  "${EXTRA_INCLUDES[@]}" \
  --go_out="${OUT_DIR}" --go_opt=paths=source_relative \
  --go-grpc_out="${OUT_DIR}" --go-grpc_opt=paths=source_relative

echo "✅ 生成完毕：${OUT_DIR}"