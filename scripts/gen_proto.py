#!/usr/bin/env python3
"""
Compile protobuf files into Go and Python stubs.
Ensures zero-warning generation and fixes Python grpc relative imports for package packaging.
"""
import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

def run_cmd(cmd: list[str], cwd: Path | None = None) -> None:
    res = subprocess.run(cmd, cwd=cwd or ROOT, text=True, capture_output=True)
    if res.returncode != 0:
        print(f"Command failed: {' '.join(cmd)}", file=sys.stderr)
        if res.stdout:
            print(res.stdout, file=sys.stderr)
        if res.stderr:
            print(res.stderr, file=sys.stderr)
        sys.exit(res.returncode)
    if res.stderr:
        print(res.stderr, file=sys.stderr)

def main() -> None:
    proto_dir = ROOT / "proto" / "switchyard" / "v1"
    proto_file = proto_dir / "broker.proto"

    go_out_dir = ROOT / "services" / "broker" / "pkg" / "pb"
    py_out_dir = ROOT / "services" / "worker" / "app" / "pb"

    go_out_dir.mkdir(parents=True, exist_ok=True)
    py_out_dir.mkdir(parents=True, exist_ok=True)

    # Compile Go stubs
    print("Compiling Go stubs into services/broker/pkg/pb...")
    run_cmd([
        "protoc",
        f"-I{proto_dir}",
        f"--go_out={go_out_dir}",
        "--go_opt=paths=source_relative",
        f"--go-grpc_out={go_out_dir}",
        "--go-grpc_opt=paths=source_relative",
        "--fatal_warnings",
        str(proto_file)
    ])

    # Compile Python stubs
    print("Compiling Python stubs into services/worker/app/pb...")
    run_cmd([
        sys.executable,
        "-m", "grpc_tools.protoc",
        f"-I{proto_dir}",
        f"--python_out={py_out_dir}",
        f"--grpc_python_out={py_out_dir}",
        "--fatal_warnings",
        str(proto_file)
    ])

    # Ensure services/worker/app and pb are packages
    (ROOT / "services" / "worker" / "app" / "__init__.py").touch(exist_ok=True)
    (py_out_dir / "__init__.py").touch(exist_ok=True)

    # Fix grpc_tools relative import bug for package-internal imports
    grpc_file = py_out_dir / "broker_pb2_grpc.py"
    if grpc_file.exists():
        content = grpc_file.read_text(encoding="utf-8")
        patched = re.sub(r"^import broker_pb2 as", "from . import broker_pb2 as", content, flags=re.MULTILINE)
        grpc_file.write_text(patched, encoding="utf-8")

    print("Protobuf compilation completed successfully with zero warnings.")

if __name__ == "__main__":
    main()
