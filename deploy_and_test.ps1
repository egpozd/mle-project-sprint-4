param(
    [string]$HostName = 'ubuntu@84.201.174.29',
    [string]$RemoteDir = '/home/ubuntu/recsys-data/service-stage4'
)

$ErrorActionPreference = 'Stop'
$LocalDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$SshOptions = @('-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10')

# Work only under the dedicated stage-4 directory on the educational VM.
if ($RemoteDir -ne '/home/ubuntu/recsys-data/service-stage4') {
    throw 'RemoteDir must be /home/ubuntu/recsys-data/service-stage4.'
}
foreach ($file in @('recommendations_service.py', 'test_service.py')) {
    if (-not (Test-Path -LiteralPath (Join-Path $LocalDir $file) -PathType Leaf)) {
        throw "Missing local source: $file"
    }
}

Write-Host '1/4 Checking SSH, VM environment, and real Parquet files...'
$Preflight = @'
set -euo pipefail
BASE=/home/ubuntu/recsys-data
STAGE=/home/ubuntu/recsys-data/service-stage4
PY=/home/ubuntu/venvs/mle-recsys/bin/python
test -d "$BASE"
test -x "$PY"
"$PY" -c 'import fastapi, uvicorn, pyarrow.parquet'
"$PY" - "$BASE" "$STAGE" <<'PY'
import json
import shutil
import sys
from pathlib import Path
import pyarrow.parquet as pq

base, stage = Path(sys.argv[1]), Path(sys.argv[2])
if shutil.disk_usage(base).free < 300 * 1024**2:
    raise SystemExit('PREFLIGHT_FAILED: less than 300 MiB free on the VM')

roots = [p for p in base.iterdir() if p.is_dir() and p.name.startswith('output')]
roots.sort(key=lambda p: (p.name == 'output-v3', p.name), reverse=True)
requirements = {
    'offline': ('recommendations.parquet', {'user_id', 'track_id', 'rank'}),
    'popular': ('top_popular.parquet', {'user_id', 'track_id', 'rank'}),
    'similar': ('similar.parquet', {'track_id', 'similar_track_id', 'rank'}),
    'items': ('items.parquet', {'track_id'}),
}

def select(filename, columns):
    candidates = []
    for root in roots:
        for path in root.rglob(filename):
            if not path.is_file():
                continue
            try:
                pf = pq.ParquetFile(path)
                if columns <= set(pf.schema_arrow.names) and pf.metadata.num_rows > 0:
                    candidates.append((path.stat().st_mtime, str(path), pf.metadata.num_rows))
            except Exception as exc:
                print(f'SKIP_INVALID {path}: {exc}', file=sys.stderr)
    if not candidates:
        return None
    candidates.sort(reverse=True)
    _, path, rows = candidates[0]
    print(f'SCHEMA_OK {filename}: {path} rows={rows}', flush=True)
    return path

resolved = {}
for kind, (filename, columns) in requirements.items():
    path = select(filename, columns)
    if path is None and kind != 'items':
        raise SystemExit(f'PREFLIGHT_FAILED: no suitable {filename} found under {roots}')
    resolved[kind] = path or ''
if not roots:
    raise SystemExit('PREFLIGHT_FAILED: no output directories found')
stage.mkdir(parents=True, exist_ok=True)
with (stage / 'resolved_paths.json').open('w', encoding='utf-8') as file:
    json.dump(resolved, file, indent=2)
print('PREFLIGHT_OK', flush=True)
PY
'@
$PreflightFile = Join-Path $env:TEMP ("recsys-preflight-" + [guid]::NewGuid().ToString('N') + '.sh')
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
$content = $Preflight.Replace("`r", "").TrimEnd("`n") + "`n"
[System.IO.File]::WriteAllText($PreflightFile, $content, $utf8NoBom)
$RemotePreflight = "$RemoteDir/preflight-$([guid]::NewGuid().ToString('N')).sh"
$SftpBatch = @("put `"$PreflightFile`" $RemotePreflight", 'bye') -join "`n"
$SftpBatch | & sftp -b - -oBatchMode=yes -oConnectTimeout=10 $HostName
if ($LASTEXITCODE -ne 0) { Remove-Item $PreflightFile -Force; throw 'SFTP upload of preflight script failed.' }
$PreflightOutput = @()
$PreflightExit = 1
try {
    $PreflightOutput = @(& ssh @SshOptions $HostName "bash $RemotePreflight")
    $PreflightExit = $LASTEXITCODE
} finally {
    Remove-Item -LiteralPath $PreflightFile -Force -ErrorAction SilentlyContinue
    & ssh @SshOptions $HostName "rm -f $RemotePreflight" *> $null
}
$PreflightOutput | ForEach-Object { Write-Host $_ }
if ($PreflightExit -ne 0 -or 'PREFLIGHT_OK' -notin $PreflightOutput) {
    throw "Remote preflight failed (exit $PreflightExit). Nothing started."
}

Write-Host '2/4 Transferring only stage-4 Python sources...'
$PutCommands = @(
    "put `"$(Join-Path $LocalDir 'recommendations_service.py')`" $RemoteDir/recommendations_service.py",
    "put `"$(Join-Path $LocalDir 'test_service.py')`" $RemoteDir/test_service.py",
    'bye'
) -join "`n"
$PutCommands | & sftp -b - -oBatchMode=yes -oConnectTimeout=10 $HostName
if ($LASTEXITCODE -ne 0) {
    throw 'SFTP transfer failed. No HTTP tests were started.'
}

Write-Host '3/4 Starting an isolated service and running actual HTTP tests...'
$RemoteRun = @'
set -euo pipefail
STAGE=/home/ubuntu/recsys-data/service-stage4
PY=/home/ubuntu/venvs/mle-recsys/bin/python
RUN_DIR=$(mktemp -d "$STAGE/run-XXXXXXXX")
echo "RUN_DIR=$RUN_DIR"
cp "$STAGE/recommendations_service.py" "$STAGE/test_service.py" "$RUN_DIR/"
cd "$RUN_DIR"

get_path() {
    "$PY" -c 'import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$STAGE/resolved_paths.json" "$1"
}
export RECSYS_OFFLINE="$(get_path offline)"
export RECSYS_POPULAR="$(get_path popular)"
export RECSYS_SIMILAR="$(get_path similar)"
export RECSYS_ITEMS="$(get_path items)"
export RECSYS_HISTORY_DB="$RUN_DIR/history.sqlite3"
export RECSYS_INSTANCE_ID="$("$PY" -c 'import uuid;print(uuid.uuid4().hex)')"
PORT="$("$PY" -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')"
export SERVICE_URL="http://127.0.0.1:$PORT"
"$PY" -m py_compile recommendations_service.py test_service.py
SERVER_PID=
cleanup() {
    result=$?
    trap - EXIT INT TERM
    if [ -n "${SERVER_PID:-}" ]; then
        kill "$SERVER_PID" 2>/dev/null || true
        for i in $(seq 1 10); do
            if ! kill -0 "$SERVER_PID" 2>/dev/null; then break; fi
            sleep 0.2
        done
        if kill -0 "$SERVER_PID" 2>/dev/null; then
            kill -KILL "$SERVER_PID" 2>/dev/null || true
        fi
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
"$PY" -m uvicorn recommendations_service:app --host 127.0.0.1 --port "$PORT" > "$RUN_DIR/service_runtime.log" 2>&1 < /dev/null &
SERVER_PID=$!
echo "SERVER_PID=$SERVER_PID PORT=$PORT INSTANCE_ID=$RECSYS_INSTANCE_ID"

if ! "$PY" - "$SERVICE_URL" "$RECSYS_INSTANCE_ID" "$SERVER_PID" <<'PY'
import json, os, sys, time, urllib.request
url, expected, pid = sys.argv[1], sys.argv[2], int(sys.argv[3])
for _ in range(360):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        raise SystemExit('STARTUP_FAILED: new service process exited')
    try:
        with urllib.request.urlopen(url + '/health', timeout=2) as response:
            body = json.load(response)
        if body.get('status') == 'ok' and body.get('instance_id') == expected:
            print('READY_OK: exact new instance', flush=True)
            break
    except Exception:
        pass
    time.sleep(1)
else:
    raise SystemExit('STARTUP_FAILED: no response from the new instance after 360 seconds')
PY
then
    tail -n 50 "$RUN_DIR/service_runtime.log" >&2 || true
    exit 1
fi

set +e
"$PY" -u "$RUN_DIR/test_service.py" 2>&1 | tee "$RUN_DIR/test_service.log"
test_exit=${PIPESTATUS[0]}
set -e
if [ "$test_exit" -ne 0 ]; then
    echo "HTTP_TESTS_FAILED exit=$test_exit" >&2
    exit "$test_exit"
fi
echo 'HTTP_TESTS_PASSED'
'@
$RunFile = Join-Path $env:TEMP ("recsys-run-" + [guid]::NewGuid().ToString('N') + '.sh')
$RemoteRunFile = "$RemoteDir/run-$([guid]::NewGuid().ToString('N')).sh"
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
$content = $RemoteRun.Replace("`r", "").TrimEnd("`n") + "`n"
[System.IO.File]::WriteAllText($RunFile, $content, $utf8NoBom)
$SftpBatch = @("put `"$RunFile`" $RemoteRunFile", 'bye') -join "`n"
$SftpBatch | & sftp -b - -oBatchMode=yes -oConnectTimeout=10 $HostName
Remove-Item $RunFile -Force
if ($LASTEXITCODE -ne 0) { throw 'SFTP upload of run script failed.' }
$RunOutput = @()
$RunExit = 1
try {
    $RunOutput = @(& ssh @SshOptions $HostName "bash $RemoteRunFile")
    $RunExit = $LASTEXITCODE
} finally {
    & ssh @SshOptions $HostName "rm -f $RemoteRunFile" *> $null
}
$RunOutput | ForEach-Object { Write-Host $_ }
$RunMarker = @($RunOutput | Where-Object { $_ -match '^RUN_DIR=(/home/ubuntu/recsys-data/service-stage4/run-[A-Za-z0-9]+)$' } | Select-Object -First 1)
$RemoteRunDir = $null
if ($RunMarker.Count -gt 0) {
    $RemoteRunDir = $RunMarker[0].Substring('RUN_DIR='.Length)
}

Write-Host '4/4 Retrieving real test output when available...'
if ($RemoteRunDir) {
    $RemoteLog = "${HostName}:$RemoteRunDir/test_service.log"
    $LogName = if ($RunExit -eq 0 -and 'HTTP_TESTS_PASSED' -in $RunOutput) { 'test_service.log' } else { 'test_service.failed.log' }
    $LocalLog = Join-Path $LocalDir $LogName
    & scp @SshOptions $RemoteLog $LocalLog
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Downloaded test output: $LocalLog"
    } elseif ($RunExit -eq 0) {
        throw 'HTTP tests passed, but retrieval of test_service.log failed.'
    } else {
        Write-Warning 'Remote tests did not complete, or no test log was created.'
    }
}
if ($RunExit -ne 0 -or 'HTTP_TESTS_PASSED' -notin $RunOutput) {
    throw "VM deployment or HTTP tests failed (exit $RunExit). Check the output above."
}
Write-Host "SUCCESS: all VM HTTP tests passed. Log: $(Join-Path $LocalDir 'test_service.log')"
