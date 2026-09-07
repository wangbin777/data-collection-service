param(
    [string]$JarPath = "",
    [int]$Port = 19090,
    [string]$Token = "ops-token",
    [int]$StartupTimeoutSeconds = 90,
    [switch]$KeepServer
)

Set-StrictMode -Version 2.0
$ErrorActionPreference = "Stop"

$script:RepoRoot = Split-Path -Parent $PSScriptRoot
if ([string]::IsNullOrWhiteSpace($JarPath)) {
    $JarPath = Join-Path $script:RepoRoot "collector-boot/target/data-collection-service-0.0.1-SNAPSHOT.jar"
}
$script:ResolvedJarPath = [System.IO.Path]::GetFullPath($JarPath)
$script:BaseUrl = "http://127.0.0.1:$Port/collector"
$script:SmokeDeviceId = "smoke-local-http-01"
$script:SmokePointId = "smoke-point-1"
$script:SmokePointCode = "smoke_point_1"
$script:BackendProcess = $null
$script:BackendReady = $false
$script:CleanupStarted = $false
$script:FailureCount = 0
$script:RequestLog = New-Object System.Collections.ArrayList
$script:LogDir = Join-Path (Split-Path -Parent $script:ResolvedJarPath) "real-smoke"
$script:StdoutLog = Join-Path $script:LogDir "backend-stdout.log"
$script:StderrLog = Join-Path $script:LogDir "backend-stderr.log"

function Write-Pass([string]$Name, [string]$Message = "") {
    if ([string]::IsNullOrWhiteSpace($Message)) {
        Write-Host "[PASS] $Name"
    } else {
        Write-Host "[PASS] $Name - $Message"
    }
}

function Write-Degraded([string]$Name, [string]$Message = "") {
    if ([string]::IsNullOrWhiteSpace($Message)) {
        Write-Host "[DEGRADED] $Name"
    } else {
        Write-Host "[DEGRADED] $Name - $Message"
    }
}

function Write-Fail([string]$Name, [string]$Message) {
    $script:FailureCount += 1
    Write-Host "[FAIL] $Name - $Message"
}

function Fail-Smoke([string]$Name, [string]$Message) {
    Write-Fail $Name $Message
    throw "${Name}: $Message"
}

function Assert-True([bool]$Condition, [string]$Name, [string]$Message) {
    if (-not $Condition) {
        Fail-Smoke $Name $Message
    }
}

function Assert-StatusCode($Response, [int]$Expected, [string]$Name) {
    if ($Response.StatusCode -ne $Expected) {
        Fail-Smoke $Name "expected HTTP $Expected, actual HTTP $($Response.StatusCode)"
    }
}

function Assert-StatusCodeIn($Response, [int[]]$Expected, [string]$Name) {
    if ($Expected -notcontains $Response.StatusCode) {
        Fail-Smoke $Name "expected HTTP $($Expected -join '/'), actual HTTP $($Response.StatusCode)"
    }
}

function Get-JsonProperty($Object, [string]$Name) {
    if ($null -eq $Object) {
        return $null
    }
    $property = $Object.PSObject.Properties[$Name]
    if ($null -eq $property) {
        return $null
    }
    return $property.Value
}

function Has-JsonProperty($Object, [string]$Name) {
    if ($null -eq $Object) {
        return $false
    }
    return $null -ne $Object.PSObject.Properties[$Name]
}

function Convert-ToJsonBody($Text) {
    if ([string]::IsNullOrWhiteSpace($Text)) {
        return $null
    }
    try {
        return $Text | ConvertFrom-Json -ErrorAction Stop
    } catch {
        return $null
    }
}

function Test-PortAvailable([int]$CheckPort) {
    $activeListeners = [System.Net.NetworkInformation.IPGlobalProperties]::GetIPGlobalProperties().GetActiveTcpListeners()
    foreach ($endpoint in $activeListeners) {
        if ($endpoint.Port -eq $CheckPort) {
            return $false
        }
    }
    $listener = $null
    try {
        $listener = New-Object System.Net.Sockets.TcpListener([System.Net.IPAddress]::Any, $CheckPort)
        $listener.Start()
        return $true
    } catch {
        return $false
    } finally {
        if ($null -ne $listener) {
            $listener.Stop()
        }
    }
}

function Read-ErrorResponseText($Exception) {
    $response = $Exception.Response
    if ($null -eq $response) {
        return ""
    }
    try {
        $contentProperty = $response.PSObject.Properties["Content"]
        if ($null -ne $contentProperty -and $null -ne $contentProperty.Value) {
            $content = $contentProperty.Value
            $method = $content.GetType().GetMethod("ReadAsStringAsync", [Type[]]@())
            if ($null -ne $method) {
                return $content.ReadAsStringAsync().GetAwaiter().GetResult()
            }
        }
    } catch {
        return ""
    }
    try {
        $stream = $response.GetResponseStream()
        if ($null -ne $stream) {
            $reader = New-Object System.IO.StreamReader($stream)
            return $reader.ReadToEnd()
        }
    } catch {
        return ""
    }
    return ""
}

function Read-ErrorStatusCode($Exception) {
    $response = $Exception.Response
    if ($null -eq $response) {
        return 0
    }
    try {
        $statusProperty = $response.PSObject.Properties["StatusCode"]
        if ($null -ne $statusProperty -and $null -ne $statusProperty.Value) {
            return [int]$statusProperty.Value
        }
    } catch {
        return 0
    }
    return 0
}

function Invoke-SmokeRequest(
    [string]$Name,
    [string]$Path,
    [string]$Method = "GET",
    [AllowNull()][string]$TokenValue = $script:Token,
    [AllowNull()]$Body = $null,
    [int]$TimeoutSeconds = 15,
    [string]$RequestId = ""
) {
    $url = "$($script:BaseUrl)$Path"
    $headers = @{ "Accept" = "application/json" }
    if (-not [string]::IsNullOrWhiteSpace($TokenValue)) {
        $headers["X-Collector-Token"] = $TokenValue
    }
    if ([string]::IsNullOrWhiteSpace($RequestId)) {
        $RequestId = "real-smoke-$Name-$([Guid]::NewGuid().ToString('N'))"
    }
    $headers["X-Request-Id"] = $RequestId

    $invokeParams = @{
        Uri = $url
        Method = $Method
        Headers = $headers
        TimeoutSec = $TimeoutSeconds
        ErrorAction = "Stop"
    }
    if ($PSVersionTable.PSVersion.Major -lt 6) {
        $invokeParams["UseBasicParsing"] = $true
    }
    if ($null -ne $Body) {
        $invokeParams["ContentType"] = "application/json; charset=utf-8"
        $invokeParams["Body"] = ($Body | ConvertTo-Json -Depth 30)
    }

    try {
        $response = Invoke-WebRequest @invokeParams
        $text = [string]$response.Content
        $contentType = ""
        if ($response.Headers) {
            $contentType = [string]$response.Headers["Content-Type"]
        }
        $json = Convert-ToJsonBody $text
        [void]$script:RequestLog.Add([pscustomobject]@{ Name = $Name; Path = $Path; Method = $Method; StatusCode = [int]$response.StatusCode; RequestId = $RequestId })
        return [pscustomobject]@{
            Name = $Name
            Url = $url
            Path = $Path
            Method = $Method
            StatusCode = [int]$response.StatusCode
            BodyText = $text
            Json = $json
            ContentType = $contentType
            RequestId = $RequestId
        }
    } catch {
        $statusCode = Read-ErrorStatusCode $_.Exception
        $text = Read-ErrorResponseText $_.Exception
        if ([string]::IsNullOrWhiteSpace($text)) {
            $text = $_.Exception.Message
        }
        $json = Convert-ToJsonBody $text
        [void]$script:RequestLog.Add([pscustomobject]@{ Name = $Name; Path = $Path; Method = $Method; StatusCode = $statusCode; RequestId = $RequestId })
        return [pscustomobject]@{
            Name = $Name
            Url = $url
            Path = $Path
            Method = $Method
            StatusCode = $statusCode
            BodyText = $text
            Json = $json
            ContentType = ""
            RequestId = $RequestId
        }
    }
}

function Invoke-SmokeProbe([string]$Path) {
    try {
        $response = Invoke-WebRequest -Uri "$($script:BaseUrl)$Path" -Method GET -TimeoutSec 2 -ErrorAction Stop -UseBasicParsing
        return [pscustomobject]@{ StatusCode = [int]$response.StatusCode; BodyText = [string]$response.Content; Json = (Convert-ToJsonBody ([string]$response.Content)) }
    } catch {
        $statusCode = Read-ErrorStatusCode $_.Exception
        $text = Read-ErrorResponseText $_.Exception
        return [pscustomobject]@{ StatusCode = $statusCode; BodyText = $text; Json = (Convert-ToJsonBody $text) }
    }
}

function Assert-Json($Response, [string]$Name) {
    Assert-True ($null -ne $Response.Json) $Name "response body is not valid JSON"
}

function Assert-ApiResultSuccess($Response, [string]$Name, [bool]$RequireCode = $true) {
    Assert-StatusCode $Response 200 $Name
    Assert-Json $Response $Name
    $code = Get-JsonProperty $Response.Json "code"
    $status = [string](Get-JsonProperty $Response.Json "status")
    if ($RequireCode) {
        Assert-True ($code -eq 200) $Name "expected ApiResult.code=200, actual '$code'"
    } else {
        Assert-True (($code -eq 200) -or ($status -eq "success")) $Name "expected ApiResult success, actual code='$code' status='$status'"
    }
    Assert-True (Has-JsonProperty $Response.Json "data") $Name "expected ApiResult.data"
}

function Get-ApiData($Response) {
    return Get-JsonProperty $Response.Json "data"
}

function Get-ArrayCount($Value) {
    if ($null -eq $Value) {
        return 0
    }
    if ($Value -is [System.Array]) {
        return $Value.Count
    }
    if ($Value -is [System.Collections.ICollection]) {
        return $Value.Count
    }
    return 0
}

function To-Array($Value) {
    if ($null -eq $Value) {
        return @()
    }
    if ($Value -is [System.Array]) {
        return @($Value)
    }
    if ($Value -is [System.Collections.IEnumerable] -and -not ($Value -is [string])) {
        return @($Value)
    }
    return @($Value)
}

function Find-DeviceById($Devices, [string]$DeviceId) {
    foreach ($item in (To-Array $Devices)) {
        $candidate = Get-JsonProperty $item "deviceId"
        if ([string]::IsNullOrWhiteSpace($candidate)) {
            $candidate = Get-JsonProperty $item "id"
        }
        if ($candidate -eq $DeviceId) {
            return $item
        }
    }
    return $null
}

function Assert-RawAggregate($Response, [string]$Name) {
    Assert-StatusCode $Response 200 $Name
    Assert-Json $Response $Name
    Assert-True (([string](Get-JsonProperty $Response.Json "status")) -eq "success") $Name "expected raw status=success"
    Assert-True (Has-JsonProperty $Response.Json "deviceCount") $Name "expected raw deviceCount"
    Assert-True (Has-JsonProperty $Response.Json "dataCount") $Name "expected raw dataCount"
    Assert-True (Has-JsonProperty $Response.Json "devices") $Name "expected raw devices"
    Assert-True (Has-JsonProperty $Response.Json "timestamp") $Name "expected raw timestamp"
    Assert-True (-not (Has-JsonProperty $Response.Json "code")) $Name "RAW DTO must not be ApiResult envelope"
}

function Assert-DashboardEndpoint($Response, [string]$Name, [bool]$AllowDegraded = $false) {
    Assert-True ($Response.StatusCode -ne 404) $Name "route returned 404"
    Assert-True ($Response.StatusCode -ne 401 -and $Response.StatusCode -ne 403) $Name "auth failed with HTTP $($Response.StatusCode)"
    Assert-True ($Response.StatusCode -lt 500 -and $Response.StatusCode -ge 200) $Name "unexpected HTTP $($Response.StatusCode)"
    Assert-Json $Response $Name
    $status = [string](Get-JsonProperty $Response.Json "status")
    if ($AllowDegraded -and ($status -eq "disabled" -or $status -eq "error")) {
        Write-Degraded $Name $([string](Get-JsonProperty $Response.Json "message"))
    } else {
        Write-Pass $Name "HTTP $($Response.StatusCode) JSON"
    }
}

function Resolve-AssetUrl([string]$AssetPath) {
    if ($AssetPath -match '^https?://') {
        return $AssetPath
    }
    $baseIndex = [Uri]"$($script:BaseUrl)/desktop/index.html"
    return ([Uri]::new($baseIndex, $AssetPath)).AbsoluteUri
}

function Invoke-AssetRequest([string]$Name, [string]$AssetUrl) {
    $headers = @{ "Accept" = "*/*"; "X-Request-Id" = "real-smoke-$Name-$([Guid]::NewGuid().ToString('N'))" }
    try {
        $params = @{ Uri = $AssetUrl; Method = "GET"; Headers = $headers; TimeoutSec = 15; ErrorAction = "Stop" }
        if ($PSVersionTable.PSVersion.Major -lt 6) {
            $params["UseBasicParsing"] = $true
        }
        $response = Invoke-WebRequest @params
        return [pscustomobject]@{ StatusCode = [int]$response.StatusCode; BodyText = [string]$response.Content; ContentType = [string]$response.Headers["Content-Type"]; Url = $AssetUrl }
    } catch {
        return [pscustomobject]@{ StatusCode = (Read-ErrorStatusCode $_.Exception); BodyText = (Read-ErrorResponseText $_.Exception); ContentType = ""; Url = $AssetUrl }
    }
}

function Build-SmokePayload() {
    return @{
        device = @{
            id = $script:SmokeDeviceId
            deviceId = $script:SmokeDeviceId
            deviceName = "Smoke Local HTTP"
            protocolType = "HTTP"
            connectionType = "HTTP"
            ipAddress = "127.0.0.1"
            port = 9
            collectionInterval = 5000
            status = "OFFLINE"
            configSource = "local"
            temporaryConfig = $true
        }
        connection = @{
            connectionType = "HTTP"
            connectionKey = $script:SmokeDeviceId
            deviceId = $script:SmokeDeviceId
            host = "127.0.0.1"
            port = 9
            url = "http://127.0.0.1:9/smoke"
            connectTimeoutMs = 500
            readTimeoutMs = 500
            writeTimeoutMs = 500
            retries = 0
            extJson = @{
                configSource = "local"
                temporaryConfig = $true
                healthCheckPath = "/smoke"
                method = "GET"
            }
        }
        points = @(
            @{
                pointId = $script:SmokePointId
                pointCode = $script:SmokePointCode
                pointName = "Smoke Point"
                deviceId = $script:SmokeDeviceId
                address = "/value"
                dataType = "DOUBLE"
                readWrite = "R"
                collectionMode = "MANUAL"
                status = 1
                cacheEnabled = 1
                alarmEnabled = 0
                baseCollectionInterval = 5000
                currentCollectionInterval = 5000
                minCollectionInterval = 1000
                maxCollectionInterval = 60000
                pointChangeThreshold = 0.01
                additionalConfig = @{
                    configSource = "local"
                    temporaryConfig = $true
                    reportEnabled = $false
                }
            }
        )
        overwrite = $true
        startAfterSave = $false
    }
}

function Cleanup-SmokeDevice([bool]$VerifyCleanup = $false) {
    if ($script:CleanupStarted) {
        return
    }
    $script:CleanupStarted = $true
    try {
        if ($script:BackendReady -and -not [string]::IsNullOrWhiteSpace($Token)) {
            $delete = Invoke-SmokeRequest "cleanup-smoke-device" "/api/config/local/device/$($script:SmokeDeviceId)" "DELETE" $Token $null 15
            if ($delete.StatusCode -eq 200) {
                Write-Pass "cleanup smoke device" "deleted $($script:SmokeDeviceId)"
            } elseif ($delete.StatusCode -eq 400 -or $delete.StatusCode -eq 404) {
                Write-Pass "cleanup smoke device" "already absent or non-local"
            } else {
                Write-Fail "cleanup smoke device" "unexpected HTTP $($delete.StatusCode)"
            }
            if ($VerifyCleanup) {
                $readAfter = Invoke-SmokeRequest "cleanup-readback" "/api/config/local/device/$($script:SmokeDeviceId)" "GET" $Token $null 15
                Assert-True ($readAfter.StatusCode -ne 200) "cleanup readback" "smoke device still readable"
                Write-Pass "cleanup readback" "device no longer returns success"
                $aggregateAfterCleanup = Invoke-SmokeRequest "aggregate-after-cleanup" "/api/data/realtime" "GET" $Token $null 15
                Assert-RawAggregate $aggregateAfterCleanup "aggregate after cleanup"
                $devices = To-Array (Get-JsonProperty $aggregateAfterCleanup.Json "devices")
                Assert-True ($null -eq (Find-DeviceById $devices $script:SmokeDeviceId)) "aggregate after cleanup" "smoke device still present"
                Write-Pass "aggregate after cleanup" "smoke device absent"
            }
        }
    } catch {
        Write-Fail "cleanup smoke device" $_.Exception.Message
    }
}

function Stop-BackendProcess() {
    if ($null -eq $script:BackendProcess) {
        return
    }
    if ($KeepServer) {
        Write-Pass "backend cleanup" "KeepServer enabled, PID $($script:BackendProcess.Id) left running"
        return
    }
    try {
        if (-not $script:BackendProcess.HasExited) {
            Stop-Process -Id $script:BackendProcess.Id -ErrorAction Stop
            if (-not $script:BackendProcess.WaitForExit(15000)) {
                Write-Fail "backend cleanup" "PID $($script:BackendProcess.Id) did not exit after Stop-Process"
            } else {
                Write-Pass "backend cleanup" "stopped owned PID $($script:BackendProcess.Id)"
            }
        } else {
            Write-Pass "backend cleanup" "owned PID $($script:BackendProcess.Id) already exited"
        }
    } catch {
        Write-Fail "backend cleanup" $_.Exception.Message
    }
}

function Get-FileTail([string]$Path, [int]$LineCount = 80) {
    if (-not (Test-Path $Path)) {
        return "<missing: $Path>"
    }
    try {
        return (Get-Content -Path $Path -Tail $LineCount -ErrorAction Stop) -join "`n"
    } catch {
        return "<failed to read ${Path}: $($_.Exception.Message)>"
    }
}

function Wait-BackendReady() {
    $deadline = (Get-Date).AddSeconds($StartupTimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        if ($null -ne $script:BackendProcess -and $script:BackendProcess.HasExited) {
            Fail-Smoke "backend ready" "Java exited with code $($script:BackendProcess.ExitCode)`nstdout tail:`n$(Get-FileTail $script:StdoutLog)`nstderr tail:`n$(Get-FileTail $script:StderrLog)"
        }
        $probe = Invoke-SmokeProbe "/health"
        if ($probe.StatusCode -ge 200 -and $probe.StatusCode -lt 500 -and $null -ne $probe.Json) {
            $script:BackendReady = $true
            Write-Pass "backend ready" "HTTP $($probe.StatusCode)"
            return
        }
        Start-Sleep -Milliseconds 500
    }
    $exitCode = "running"
    if ($null -ne $script:BackendProcess -and $script:BackendProcess.HasExited) {
        $exitCode = [string]$script:BackendProcess.ExitCode
    }
    Fail-Smoke "backend ready" "startup timeout, Java exit code $exitCode`nstdout tail:`n$(Get-FileTail $script:StdoutLog)`nstderr tail:`n$(Get-FileTail $script:StderrLog)"
}

function Resolve-JavaExecutable() {
    if (-not [string]::IsNullOrWhiteSpace($env:JAVA_HOME)) {
        $candidate = Join-Path $env:JAVA_HOME "bin/java.exe"
        if (Test-Path $candidate) {
            return $candidate
        }
    }
    $command = Get-Command java.exe -ErrorAction Stop
    return $command.Source
}

function Start-Backend() {
    New-Item -ItemType Directory -Force -Path $script:LogDir | Out-Null
    Remove-Item -Path $script:StdoutLog, $script:StderrLog -Force -ErrorAction SilentlyContinue
    $javaExecutable = Resolve-JavaExecutable
    $args = @(
        "-jar", $script:ResolvedJarPath,
        "--spring.profiles.active=dev",
        "--server.port=$Port",
        "--telemetry.tdengine.enabled=false",
        "--collector.report.enabled=false",
        "--collector.report.mqtt.enabled=false",
        "--collector.report.shadow.persistence-enabled=false",
        "--collector.alarm.state.enabled=false",
        "--collector.cache.type=local",
        "--spring.data.redis.stream.enabled=false",
        "--collector.config.loader=file",
        "--logging.level.com.wangbin.collector.api.filter.LogFilter=INFO",
        "--logging.access.include-paths[0]=/api/**",
        "--logging.access.include-paths[1]=/collector/api/**"
    )
    $script:BackendProcess = Start-Process -FilePath $javaExecutable -ArgumentList $args -RedirectStandardOutput $script:StdoutLog -RedirectStandardError $script:StderrLog -PassThru -WindowStyle Hidden
    Write-Pass "backend process started" "PID $($script:BackendProcess.Id)"
}

function Assert-Jar() {
    Assert-True (Test-Path $script:ResolvedJarPath) "executable jar" "jar not found: $script:ResolvedJarPath"
    $name = [System.IO.Path]::GetFileName($script:ResolvedJarPath)
    Assert-True (-not ($name -like "original-*")) "executable jar" "must not start original jar: $name"
    Assert-True (-not ($name -like "*.original")) "executable jar" "must not start *.original jar: $name"
    Write-Pass "executable jar" $name
}

function Assert-AccessLogSingleAggregate([string]$RequestId) {
    $deadline = (Get-Date).AddSeconds(5)
    $matches = @()
    while ((Get-Date) -lt $deadline) {
        if (Test-Path $script:StdoutLog) {
            $lines = Get-Content -Path $script:StdoutLog -ErrorAction SilentlyContinue
            $matches = @($lines | Where-Object { $_ -like "*$RequestId*" -and $_ -like "*/api/data/realtime*" })
            if ($matches.Count -gt 0) {
                break
            }
        }
        Start-Sleep -Milliseconds 250
    }
    Assert-True ($matches.Count -eq 1) "aggregate access log" "expected 1 access log entry for aggregate request, actual $($matches.Count)"
    $deviceRequests = @($script:RequestLog | Where-Object { $_.Name -eq "aggregate-after-create-device-detail" -or $_.Path -like "/api/data/device/*" })
    Assert-True ($deviceRequests.Count -le 2) "aggregate request count" "smoke script issued unexpected per-device aggregate validation requests"
    Write-Pass "aggregate access log" "1 client HTTP GET /api/data/realtime"
}

function Run-Smoke() {
    Assert-Jar
    if (-not (Test-PortAvailable $Port)) {
        Write-Host "Smoke port already in use"
        exit 1
    }
    Write-Pass "smoke port available" $Port
    Start-Backend
    Wait-BackendReady

    $health = Invoke-SmokeRequest "public-health" "/health" "GET" "" $null 15
    Assert-StatusCode $health 200 "public health"
    Assert-Json $health "public health"
    Write-Pass "public health" "JSON status=$(Get-JsonProperty $health.Json 'status')"

    $actuator = Invoke-SmokeRequest "actuator-health" "/actuator/health" "GET" "" $null 15
    Assert-StatusCodeIn $actuator @(200, 503) "actuator health"
    Assert-Json $actuator "actuator health"
    Assert-True (Has-JsonProperty $actuator.Json "status") "actuator health" "expected status field"
    Write-Pass "actuator health" "status=$(Get-JsonProperty $actuator.Json 'status')"

    $index = Invoke-SmokeRequest "desktop-index" "/desktop/index.html" "GET" "" $null 15
    Assert-StatusCode $index 200 "desktop index"
    Assert-True ($index.BodyText.Length -gt 0) "desktop index" "body is empty"
    Assert-True (($index.ContentType -like "*html*" -or $index.BodyText -like "*<html*")) "desktop index" "expected HTML content"
    Assert-True (($index.BodyText -like "*id=`"app`"*" -or $index.BodyText -like "*type=`"module`"*")) "desktop index" "missing Vue app root or module script"
    Write-Pass "desktop index" "HTTP 200 HTML"

    $jsMatch = [regex]::Match($index.BodyText, '<script[^>]+src="([^"]+\.js)"')
    $cssMatch = [regex]::Match($index.BodyText, '<link[^>]+href="([^"]+\.css)"')
    Assert-True $jsMatch.Success "desktop js asset" "index.html did not reference a JS asset"
    Assert-True $cssMatch.Success "desktop css asset" "index.html did not reference a CSS asset"
    $js = Invoke-AssetRequest "desktop-js-asset" (Resolve-AssetUrl $jsMatch.Groups[1].Value)
    Assert-StatusCode $js 200 "desktop js asset"
    Assert-True ($js.BodyText.Length -gt 0) "desktop js asset" "asset body is empty"
    Write-Pass "desktop js asset" "$($js.BodyText.Length) bytes"
    $css = Invoke-AssetRequest "desktop-css-asset" (Resolve-AssetUrl $cssMatch.Groups[1].Value)
    Assert-StatusCode $css 200 "desktop css asset"
    Assert-True ($css.BodyText.Length -gt 0) "desktop css asset" "asset body is empty"
    Write-Pass "desktop css asset" "$($css.BodyText.Length) bytes"

    $noToken = Invoke-SmokeRequest "unauthorized-api" "/api/data/realtime" "GET" "" $null 15
    Assert-StatusCode $noToken 401 "unauthorized API rejected"
    Write-Pass "unauthorized API rejected" "HTTP 401"
    $invalidToken = Invoke-SmokeRequest "invalid-token-api" "/api/data/realtime" "GET" "invalid-smoke-token" $null 15
    Assert-StatusCode $invalidToken 401 "invalid token rejected"
    Write-Pass "invalid token rejected" "HTTP 401"
    $validToken = Invoke-SmokeRequest "valid-token-api" "/api/data/realtime" "GET" $Token $null 15
    Assert-True ($validToken.StatusCode -ne 401 -and $validToken.StatusCode -ne 403) "valid token accepted" "HTTP $($validToken.StatusCode)"
    Assert-RawAggregate $validToken "valid token accepted"
    Write-Pass "valid token accepted" "HTTP $($validToken.StatusCode) raw aggregate"

    $monitorNoToken = Invoke-SmokeRequest "monitor-no-token" "/monitor/runtime" "GET" "" $null 15
    Assert-StatusCode $monitorNoToken 401 "monitor auth without token"
    Write-Pass "monitor auth without token" "HTTP 401"
    $monitorWithToken = Invoke-SmokeRequest "monitor-with-token" "/monitor/runtime" "GET" $Token $null 15
    Assert-DashboardEndpoint $monitorWithToken "monitor auth with token" $false

    $configDevices = Invoke-SmokeRequest "config-devices" "/api/config/devices" "GET" $Token $null 15
    Assert-ApiResultSuccess $configDevices "ApiResult config devices" $true
    Write-Pass "ApiResult config devices" "code=200 data present"

    $aggregateBaseline = Invoke-SmokeRequest "aggregate-baseline" "/api/data/realtime" "GET" $Token $null 15
    Assert-RawAggregate $aggregateBaseline "RAW aggregate baseline"
    $baselineDeviceCount = [int](Get-JsonProperty $aggregateBaseline.Json "deviceCount")
    $baselineDataCount = [int](Get-JsonProperty $aggregateBaseline.Json "dataCount")
    $baselineDevicesCount = Get-ArrayCount (Get-JsonProperty $aggregateBaseline.Json "devices")
    Write-Pass "RAW aggregate baseline" "status=$(Get-JsonProperty $aggregateBaseline.Json 'status') deviceCount=$baselineDeviceCount dataCount=$baselineDataCount devices=$baselineDevicesCount"

    Assert-DashboardEndpoint (Invoke-SmokeRequest "dashboard-config-devices" "/api/config/devices" "GET" $Token $null 15) "dashboard config devices" $false
    Assert-DashboardEndpoint (Invoke-SmokeRequest "dashboard-device-runtime" "/api/device/runtime" "GET" $Token $null 15) "dashboard device runtime" $false
    Assert-DashboardEndpoint (Invoke-SmokeRequest "dashboard-recent-alarms" "/api/data/history/alarms?limit=8" "GET" $Token $null 15) "dashboard recent alarms" $true
    Assert-DashboardEndpoint (Invoke-SmokeRequest "dashboard-report" "/monitor/report" "GET" $Token $null 15) "dashboard report" $false
    Assert-DashboardEndpoint (Invoke-SmokeRequest "dashboard-runtime" "/monitor/runtime" "GET" $Token $null 15) "dashboard runtime" $false
    Assert-DashboardEndpoint (Invoke-SmokeRequest "dashboard-system" "/monitor/system" "GET" $Token $null 15) "dashboard system" $false
    Assert-DashboardEndpoint (Invoke-SmokeRequest "dashboard-cache" "/monitor/cache" "GET" $Token $null 15) "dashboard cache" $false
    Assert-DashboardEndpoint (Invoke-SmokeRequest "dashboard-storage" "/monitor/storage" "GET" $Token $null 15) "dashboard storage" $true
    Assert-DashboardEndpoint (Invoke-SmokeRequest "dashboard-performance" "/monitor/perf/detail" "GET" $Token $null 15) "dashboard performance" $false

    $preCleanup = Invoke-SmokeRequest "cleanup-before-create" "/api/config/local/device/$($script:SmokeDeviceId)" "DELETE" $Token $null 15
    if ($preCleanup.StatusCode -eq 200 -or $preCleanup.StatusCode -eq 400 -or $preCleanup.StatusCode -eq 404) {
        Write-Pass "cleanup before create" "HTTP $($preCleanup.StatusCode)"
    } else {
        Fail-Smoke "cleanup before create" "unexpected HTTP $($preCleanup.StatusCode)"
    }

    $payload = Build-SmokePayload
    $create = Invoke-SmokeRequest "create-local-device" "/api/config/local/devices" "POST" $Token $payload 20
    Assert-ApiResultSuccess $create "create local device" $true
    $createdData = Get-ApiData $create
    Assert-True ((Get-JsonProperty $createdData "deviceId") -eq $script:SmokeDeviceId) "create local device" "wrong deviceId"
    Assert-True ([bool](Get-JsonProperty $createdData "temporaryConfig")) "create local device" "temporaryConfig must be true"
    Assert-True (([int](Get-JsonProperty $createdData "pointCount")) -eq 1) "create local device" "pointCount must be 1"
    Assert-True (-not [bool](Get-JsonProperty $createdData "started")) "create local device" "startAfterSave=false must not start device"
    Write-Pass "create local device" "deviceId=$($script:SmokeDeviceId) pointCount=1 started=false"

    $readLocal = Invoke-SmokeRequest "read-local-device" "/api/config/local/device/$($script:SmokeDeviceId)" "GET" $Token $null 15
    Assert-ApiResultSuccess $readLocal "read local device" $true
    $readData = Get-ApiData $readLocal
    Assert-True ((Get-JsonProperty $readData "deviceId") -eq $script:SmokeDeviceId) "read local device" "wrong deviceId"
    Assert-True ($null -ne (Get-JsonProperty $readData "bundle")) "read local device" "bundle missing"
    Write-Pass "read local device" "bundle present"

    $listAfterCreate = Invoke-SmokeRequest "list-config-devices" "/api/config/devices" "GET" $Token $null 15
    Assert-ApiResultSuccess $listAfterCreate "list config devices" $true
    $listData = Get-ApiData $listAfterCreate
    $configRows = To-Array (Get-JsonProperty $listData "devices")
    Assert-True ($null -ne (Find-DeviceById $configRows $script:SmokeDeviceId)) "list config devices" "smoke device not found"
    Write-Pass "list config devices" "contains $($script:SmokeDeviceId)"

    $points = Invoke-SmokeRequest "point-config" "/api/config/device/$($script:SmokeDeviceId)/points?includeAdaptive=true" "GET" $Token $null 15
    Assert-ApiResultSuccess $points "point config" $true
    $pointsData = Get-ApiData $points
    Assert-True ((Get-JsonProperty $pointsData "deviceId") -eq $script:SmokeDeviceId) "point config" "wrong deviceId"
    Assert-True (([int](Get-JsonProperty $pointsData "count")) -eq 1) "point config" "expected count=1"
    $pointRows = To-Array (Get-JsonProperty $pointsData "points")
    Assert-True (@($pointRows | Where-Object { (Get-JsonProperty $_ "pointId") -eq $script:SmokePointId }).Count -eq 1) "point config" "smoke point not found"
    Write-Pass "point config" "count=1 contains $($script:SmokePointId)"

    $deviceRealtime = Invoke-SmokeRequest "device-realtime" "/api/data/device/$($script:SmokeDeviceId)" "GET" $Token $null 15
    Assert-StatusCode $deviceRealtime 200 "device realtime"
    Assert-Json $deviceRealtime "device realtime"
    Assert-True (([string](Get-JsonProperty $deviceRealtime.Json "status")) -eq "success") "device realtime" "expected status=success"
    Assert-True ((Get-JsonProperty $deviceRealtime.Json "deviceId") -eq $script:SmokeDeviceId) "device realtime" "wrong deviceId"
    Assert-True (([int](Get-JsonProperty $deviceRealtime.Json "dataCount")) -eq 1) "device realtime" "expected dataCount=1"
    $realtimeData = Get-JsonProperty $deviceRealtime.Json "data"
    Assert-True (Has-JsonProperty $realtimeData $script:SmokePointId) "device realtime" "smoke point data missing"
    Write-Pass "device realtime" "RAW DTO dataCount=1 value may be null"

    $singlePoint = Invoke-SmokeRequest "single-point-realtime" "/api/data/device/$($script:SmokeDeviceId)/point/$($script:SmokePointId)" "GET" $Token $null 15
    Assert-StatusCode $singlePoint 200 "single point realtime"
    Assert-Json $singlePoint "single point realtime"
    Assert-True (([string](Get-JsonProperty $singlePoint.Json "status")) -eq "success") "single point realtime" "expected status=success"
    Assert-True ((Get-JsonProperty $singlePoint.Json "deviceId") -eq $script:SmokeDeviceId) "single point realtime" "wrong deviceId"
    Assert-True ((Get-JsonProperty $singlePoint.Json "pointId") -eq $script:SmokePointId) "single point realtime" "wrong pointId"
    Assert-True ($null -ne (Get-JsonProperty $singlePoint.Json "data")) "single point realtime" "data missing"
    Write-Pass "single point realtime" "RAW DTO point exists, value may be null"

    $aggregateRequestId = "real-smoke-aggregate-after-create"
    $aggregateAfterCreate = Invoke-SmokeRequest "aggregate-after-create" "/api/data/realtime" "GET" $Token $null 15 $aggregateRequestId
    Assert-RawAggregate $aggregateAfterCreate "aggregate after create"
    $afterDeviceCount = [int](Get-JsonProperty $aggregateAfterCreate.Json "deviceCount")
    $afterDataCount = [int](Get-JsonProperty $aggregateAfterCreate.Json "dataCount")
    $aggregateDevices = To-Array (Get-JsonProperty $aggregateAfterCreate.Json "devices")
    $smokeAggregateDevice = Find-DeviceById $aggregateDevices $script:SmokeDeviceId
    Assert-True ($null -ne $smokeAggregateDevice) "aggregate after create" "smoke device missing"
    Assert-True (([string](Get-JsonProperty $smokeAggregateDevice "status")) -eq "success") "aggregate after create" "inner device status must be success"
    Assert-True (([int](Get-JsonProperty $smokeAggregateDevice "dataCount")) -eq 1) "aggregate after create" "inner dataCount must be 1"
    Assert-True ($afterDeviceCount -ge $baselineDeviceCount) "aggregate after create" "deviceCount decreased from baseline"
    Assert-True ($afterDataCount -ge $baselineDataCount) "aggregate after create" "dataCount decreased from baseline"
    Write-Pass "aggregate after create" "contains smoke device; baseline deviceCount=$baselineDeviceCount after=$afterDeviceCount"
    Assert-AccessLogSingleAggregate $aggregateRequestId

    $summary = Invoke-SmokeRequest "device-summary" "/api/data/devices" "GET" $Token $null 15
    Assert-StatusCode $summary 200 "device summary"
    Assert-Json $summary "device summary"
    Assert-True (([string](Get-JsonProperty $summary.Json "status")) -eq "success") "device summary" "expected status=success"
    $summaryDevice = Find-DeviceById (Get-JsonProperty $summary.Json "devices") $script:SmokeDeviceId
    Assert-True ($null -ne $summaryDevice) "device summary" "smoke device missing"
    Assert-True (([int](Get-JsonProperty $summaryDevice "pointCount")) -eq 1) "device summary" "expected pointCount=1"
    Write-Pass "device summary" "contains smoke device pointCount=1"

    $opsLogs = Invoke-SmokeRequest "ops-logs" "/api/ops/logs?limit=20" "GET" $Token $null 15
    Assert-ApiResultSuccess $opsLogs "ops logs" $true
    Write-Pass "ops logs" "ApiResult data contract valid"

    Cleanup-SmokeDevice $true
}

try {
    Run-Smoke
} catch {
    Write-Host "REAL BACKEND SMOKE FAILED"
    Write-Host $_.Exception.Message
    Write-Fail "real backend smoke" $_.Exception.Message
} finally {
    Cleanup-SmokeDevice $false
    Stop-BackendProcess
}

if ($script:FailureCount -eq 0) {
    Write-Host "REAL BACKEND SMOKE PASSED"
    exit 0
}
Write-Host "REAL BACKEND SMOKE FAILED"
exit 1
