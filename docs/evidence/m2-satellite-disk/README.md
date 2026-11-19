# Synthetic disk-working-state measurements

These are local regression measurements, not official-feed acceptance or
quorum-storage throughput. No upstream token, official exam session, cluster
fault, remote push or external service was used.

The Linux test process runs with a 512MiB container cap, a 384MiB Go soft limit,
two CPUs, a read-only root and no network. SQLite state and temporary files use
a native Linux Docker volume. The test binary is mounted read-only from the
workspace. A Windows bind-mounted database failed SQLite truncation before
input; native local disk is required for this experiment and matches k3s's
intended working volume.

`TestDiskProcessorMemoryEnvelope` processes spatially independent detections
through the disk processor, writes a streaming snapshot, deletes the old
working cache, restores the snapshot and verifies exact active/component
counts. It never creates a graph-sized Go map or input slice.

`TestDiskDenseCorrectionMemoryEnvelope` seeds one valid component directly in
its derived cache, then retracts one point through the real processor and
verifies one revised fire with unchanged identity and exact membership. This
isolates large-record/correction memory; seeding is not a measurement of the
complete input-ingestion path or a database write.

Build from services/aggregator with the usual serial Go limits and:

```powershell
$env:CGO_ENABLED='0'
$env:GOOS='linux'
$env:GOARCH='amd64'
go test -c -p 1 -o '../../.cache/m2-disk-runtime/m2.test' ./internal/m2
```

On a disposable native Linux volume, run the test binary in a 512MiB/2CPU
container with these environment variables and arguments (PowerShell callers
must quote each complete dotted test flag):

```text
GOMEMLIMIT=384MiB GOMAXPROCS=2
TMPDIR=/state SQLITE_TMPDIR=/state
M2_DISK_STRESS=1 M2_DISK_POINTS=200000
'-test.run=TestDisk(ProcessorMemoryEnvelope|DenseCorrectionMemoryEnvelope)'
'-test.v=true' '-test.timeout=20m'
```

Mount the test binary at /tests/m2.test and a native disk volume at /state.
Use --memory 512m --cpus 2 --read-only --network none. Do not mount the SQLite
working directory over Windows file sharing. Each test cleans only its private
files. Fixture/container cleanup must verify the codex.task=m2-gap-fixes label
before deleting the uniquely named test container or volume.

The tests print peak process RSS from /proc/self/status and, where available,
the cgroup's memory.peak (which also includes charged file cache). Record
container exit/OOM state along with the stdout. See report.json for measured
results and the exact implementation commit or working-tree base.
