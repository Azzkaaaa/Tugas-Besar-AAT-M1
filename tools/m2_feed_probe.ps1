param([int]$Seconds = 20, [string]$FromOffset = '0', [string]$Output = '.cache/m2/feed.jsonl')
$ErrorActionPreference = 'Stop'
$feedToken = (Get-Content -LiteralPath '.secrets/satellite-token' -Raw).Trim()
$parent = Split-Path -Parent $Output
New-Item -ItemType Directory -Path $parent -Force | Out-Null
$request = [System.Net.HttpWebRequest]::Create('https://satellite.naufarrel.dev/v1/stream?from=' + [Uri]::EscapeDataString($FromOffset))
$request.Headers['Authorization'] = 'Bearer ' + $feedToken
$request.Accept = 'text/event-stream'
$request.Timeout = 10000
$request.ReadWriteTimeout = 10000
$response = $request.GetResponse()
$reader = [IO.StreamReader]::new($response.GetResponseStream())
$writer = [IO.StreamWriter]::new((Join-Path (Get-Location) $Output), $false, [Text.UTF8Encoding]::new($false))
$watch = [Diagnostics.Stopwatch]::StartNew()
$kind = ''; $eventID = ''; $data = [Collections.Generic.List[string]]::new(); $count = 0; $cursor = $FromOffset
try {
    while ($watch.Elapsed.TotalSeconds -lt $Seconds) {
        $line = $reader.ReadLine()
        if ($null -eq $line) { break }
        if ($line.Length -gt 1048576) { throw 'Oversized SSE line' }
        if ($line -eq '') {
            if ($data.Count -gt 0) {
                $count++
                if ($eventID -ne '') { $cursor = $eventID }
                $payload = $data -join "`n"
                $entry = @{ sequence=$count; kind=$kind; cursor=$cursor; received_at=[DateTime]::UtcNow.ToString('o'); payload=[Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($payload)) }
                $writer.WriteLine(($entry | ConvertTo-Json -Compress)); $writer.Flush()
                if ($count -le 5) { @{event=$kind; id=$eventID; data=$payload} | ConvertTo-Json -Compress }
            }
            $kind=''; $eventID=''; $data.Clear()
            continue
        }
        $parts = $line -split ':', 2
        if ($parts.Count -lt 2) { continue }
        $value = $parts[1]
        if ($value.StartsWith(' ')) { $value = $value.Substring(1) }
        switch ($parts[0]) { 'event' { $kind=$value }; 'id' { $eventID=$value }; 'data' { $data.Add($value) } }
    }
} finally {
    $writer.Dispose(); $reader.Dispose(); $response.Dispose()
}
@{captured=$count;last_cursor=$cursor;path=$Output} | ConvertTo-Json -Compress
