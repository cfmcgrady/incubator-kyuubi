#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Reproduce full tiny/SF1 and bounded native comparisons against a packaged core JAR.

Run on Linux. The output directory must be new, outside the source checkout.
Build the pinned native reference with the flags documented in README.md first.
"""
import argparse
import concurrent.futures
import hashlib
import json
import os
import select
import time
from pathlib import Path
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--dbgen-dir", type=Path, required=True)
parser.add_argument("--core-jar", type=Path, required=True)
parser.add_argument("--output-dir", type=Path, required=True)
parser.add_argument(
    "--scales", type=float, nargs="+", default=[0.01, 1, 10, 10000, 30000, 100000]
)
parser.add_argument("--workers", type=int, default=2)
parser.add_argument("--skip-full", action="store_true")
args = parser.parse_args()
native = args.dbgen_dir.resolve()
root = args.output_dir.resolve()
jar = args.core_jar.resolve()
expected = "852ad0a5ee31ebefeed884cea4188781dd9613a3"
revision = subprocess.check_output(
    ["git", "-C", str(native), "rev-parse", "HEAD"], text=True
).strip()
if revision != expected:
    raise SystemExit("Reference commit mismatch: " + revision)
if not jar.is_file():
    raise SystemExit("Core JAR is missing")
root.mkdir(parents=True, exist_ok=False)
out = root / "samples"
out.mkdir()
tables = {
    "customer": (150000, "c"),
    "orders": (1500000, "o"),
    "lineitem": (1500000, "L"),
    "part": (200000, "P"),
    "partsupp": (200000, "S"),
    "supplier": (10000, "s"),
    "nation": (25, "n"),
    "region": (5, "r"),
}


def capture(case):
    table, scale, partitions, child = case
    base, code = tables[table]
    total = (
        base
        if table in ("nation", "region")
        else (
            max(1, int(scale * 1000) * base // 1000) if scale < 1 else int(scale) * base
        )
    )
    each = total // partitions
    start = each * (child - 1)
    end = min(start + 100, total if child == partitions else start + each)
    name = f"{table}-{scale}-{partitions}-{child}"
    work = out / name
    work.mkdir(exist_ok=True)
    fifo = work / (table + ".tbl" + (f".{child}" if partitions > 1 else ""))
    os.mkfifo(fifo)
    cmd = [str(native / "dbgen"), "-s", str(scale), "-T", code, "-f"]
    if partitions > 1:
        cmd += ["-C", str(partitions), "-S", str(child)]
    env = os.environ.copy()
    env["DSS_PATH"] = str(work)
    with (work / "stderr.log").open("wb") as err:
        descriptor = os.open(fifo, os.O_RDONLY | os.O_NONBLOCK)
        process = subprocess.Popen(
            cmd, cwd=native, env=env, stdout=subprocess.DEVNULL, stderr=err
        )
        data = []
        try:
            with os.fdopen(descriptor, "rb") as stream:
                deadline = time.monotonic() + 60
                while not select.select([stream], [], [], 1)[0]:
                    if process.poll() is not None:
                        raise RuntimeError((case, (work / "stderr.log").read_text()))
                    if time.monotonic() > deadline:
                        raise TimeoutError(
                            f"Native generator produced no output: {case}"
                        )
                os.set_blocking(stream.fileno(), True)
                for line in stream:
                    key = int(line.split(b"|", 1)[0])
                    parent = (
                        ((key >> 5) << 3) + (key & 7)
                        if table in ("orders", "lineitem")
                        else key + 1 if table in ("nation", "region") else key
                    )
                    if parent > end:
                        break
                    # Native PR_END seeks back over the final delimiter; FIFOs cannot seek.
                    if line.endswith(b"|\n"):
                        line = line[:-2] + b"\n"
                    data.append(line)
            status = process.wait(timeout=30)
            if status not in (0, -13):
                raise RuntimeError((case, status, (work / "stderr.log").read_text()))
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()
    fifo.unlink()
    payload = b"".join(data)
    target = out / (name + ".tbl")
    target.write_bytes(payload)
    record = dict(
        table=table,
        scale=scale,
        partitions=partitions,
        partition=child,
        start=start,
        end=end,
        rows=len(data),
        sha256=hashlib.sha256(payload).hexdigest(),
        file=str(target),
        command=cmd,
    )
    assert data, case
    return record


records = []
if not args.skip_full:
    for scale in (0.01, 1):
        folder = root / ("full-" + str(scale))
        folder.mkdir()
        env = os.environ.copy()
        env["DSS_PATH"] = str(folder)
        cmd = [str(native / "dbgen"), "-s", str(scale), "-f"]
        subprocess.run(cmd, cwd=native, env=env, check=True)
        for table, (base, _) in tables.items():
            count = base if table in ("nation", "region") else int(base * scale)
            file = folder / (table + ".tbl")
            digest = hashlib.sha256()
            rows = 0
            with file.open("rb") as data:
                for chunk in iter(lambda: data.read(1024 * 1024), b""):
                    digest.update(chunk)
                    rows += chunk.count(b"\n")
            records.append(
                dict(
                    table=table,
                    scale=scale,
                    start=0,
                    end=count,
                    rows=rows,
                    file=str(file),
                    sha256=digest.hexdigest(),
                    command=cmd,
                )
            )
cases = []
for scale in args.scales:
    for table in tables:
        for p, c in (
            [(1, 1)] if table in ("nation", "region") else [(3, 1), (3, 2), (7, 7)]
        ):
            cases.append((table, scale, p, c))
with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
    for record in pool.map(capture, cases):
        records.append(record)
        print(
            record["table"],
            record["scale"],
            record["start"],
            record["rows"],
            flush=True,
        )
manifest = root / "matrix.tsv"
manifest.write_text(
    "".join(
        "\t".join(str(r[k]) for k in ("table", "scale", "start", "end", "file")) + "\n"
        for r in records
    )
)
(root / "reference.jsonl").write_text("".join(json.dumps(r) + "\n" for r in records))
classes = root / "classes"
classes.mkdir()
source = Path(__file__).with_name("NativeComparison.java")
subprocess.run(["javac", "-cp", str(jar), "-d", str(classes), str(source)], check=True)
subprocess.run(
    [
        "java",
        "-Xmx1g",
        "-cp",
        str(classes) + os.pathsep + str(jar),
        "NativeComparison",
        str(manifest),
    ],
    check=True,
)
print("PASS:", len(records), "comparisons,", sum(r["rows"] for r in records), "rows")
