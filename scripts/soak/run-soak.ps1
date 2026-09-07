param(
  [int]$Points = 10000,
  [int]$Devices = 0,
  [int]$DurationSeconds = 300,
  [int]$CollectionIntervalMs = 1000,
  [string]$Profile = "soak",
  [string]$Scenario = "normal",
  [string]$MetricsOutput = "",
  [string]$MavenRepoLocal = ".m2-local",
  [string]$RedisHost = "127.0.0.1",
  [int]$RedisPort = 6379,
  [string]$RedisPassword = "",
  [string]$TdengineUrl = "jdbc:TAOS-RS://127.0.0.1:6041/wangbin_collector",
  [string]$TdengineUsername = "root",
  [string]$TdenginePassword = "taosdata",
  [string]$MqttBrokerUrl = "tcp://127.0.0.1:1883",
  [bool]$SpreadWithinInterval = $true,
  [string]$IngressMode = "point",
  [int]$HistoryCoreSize = 0,
  [int]$HistoryMaxSize = 0,
  [int]$HistoryQueueCapacity = 0,
  [string]$RedisContainer = "data-collection-redis",
  [string]$TdengineContainer = "data-collection-tdengine",
  [string]$MqttContainer = "data-collection-mqtt",
  [int]$OutageStartSeconds = 60,
  [int]$OutageDurationSeconds = 120,
  [switch]$AllowServiceControl
)

$ErrorActionPreference = "Stop"

function New-RunId {
  return (Get-Date).ToString("yyyyMMdd-HHmmss")
}

function Resolve-Devices {
  param([int]$PointCount, [int]$DeviceCount)
  if ($DeviceCount -gt 0) {
    return $DeviceCount
  }
  return [Math]::Max(1, [Math]::Floor($PointCount / 1000))
}

function Start-OutageJob {
  param(
    [string]$ScenarioName,
    [int]$StartSeconds,
    [int]$Duration,
    [string]$RedisName,
    [string]$TdName,
    [string]$MqttName
  )
  if (-not $AllowServiceControl) {
    Write-Host "AllowServiceControl is disabled; running soak only without dependency outage actions."
    return $null
  }
  if ($ScenarioName -eq "normal") {
    return $null
  }
  return Start-Job -ScriptBlock {
    param($ScenarioName, $StartSeconds, $Duration, $RedisName, $TdName, $MqttName)
    Start-Sleep -Seconds $StartSeconds
    $targets = @()
    if ($ScenarioName -match "redis") { $targets += $RedisName }
    if ($ScenarioName -match "tdengine") { $targets += $TdName }
    if ($ScenarioName -match "cloud|mqtt") { $targets += $MqttName }
    foreach ($target in $targets) {
      if (-not [string]::IsNullOrWhiteSpace($target)) {
        Write-Host "Stopping test container $target"
        docker stop $target | Out-Host
      }
    }
    Start-Sleep -Seconds $Duration
    foreach ($target in $targets) {
      if (-not [string]::IsNullOrWhiteSpace($target)) {
        Write-Host "Starting test container $target"
        docker start $target | Out-Host
      }
    }
  } -ArgumentList $ScenarioName, $StartSeconds, $Duration, $RedisName, $TdName, $MqttName
}

$runId = New-RunId
$resolvedDevices = Resolve-Devices -PointCount $Points -DeviceCount $Devices
if ([string]::IsNullOrWhiteSpace($MetricsOutput)) {
  $MetricsOutput = "target/soak-results/$runId"
}
$redisKeyPrefix = "collector:soak:$runId"

New-Item -ItemType Directory -Force -Path $MetricsOutput | Out-Null

$repoLocal = Resolve-Path -Path $MavenRepoLocal -ErrorAction SilentlyContinue
if ($null -eq $repoLocal) {
  New-Item -ItemType Directory -Force -Path $MavenRepoLocal | Out-Null
  $repoLocal = Resolve-Path -Path $MavenRepoLocal
}

$outageJob = Start-OutageJob `
  -ScenarioName $Scenario `
  -StartSeconds $OutageStartSeconds `
  -Duration $OutageDurationSeconds `
  -RedisName $RedisContainer `
  -TdName $TdengineContainer `
  -MqttName $MqttContainer

try {
  $arguments = @(
    "-B",
    "-ntp",
    "-Dmaven.repo.local=$($repoLocal.Path)",
    "-Dtest=RealEnvironmentSoakIT",
    "-Dspring.profiles.active=$Profile",
    "-Dcollector.config.loader=file",
    "-Dspring.data.redis.host=$RedisHost",
    "-Dspring.data.redis.port=$RedisPort",
    "-Dspring.data.redis.password=$RedisPassword",
    "-Dspring.datasource.url=$TdengineUrl",
    "-Dspring.datasource.username=$TdengineUsername",
    "-Dspring.datasource.password=$TdenginePassword",
    "-Dtelemetry.tdengine.enabled=true",
    "-Dtelemetry.tdengine.buffer.pending-key=$redisKeyPrefix:history:pending:v1",
    "-Dtelemetry.tdengine.buffer.processing-key=$redisKeyPrefix:history:processing:v1",
    "-Dtelemetry.tdengine.buffer.dead-letter-key=$redisKeyPrefix:history:dead:v1",
    "-Dcollector.telemetry-ingress-buffer.pending-key=$redisKeyPrefix:telemetry:ingress:pending:v1",
    "-Dcollector.telemetry-ingress-buffer.processing-key=$redisKeyPrefix:telemetry:ingress:processing:v1",
    "-Dcollector.telemetry-ingress-buffer.dead-letter-key=$redisKeyPrefix:telemetry:ingress:dead:v1",
    "-Dcollector.report.outbox.key-prefix=$redisKeyPrefix:cloud:outbox:v1:",
    "-Dspring.data.redis.stream.key=$redisKeyPrefix:telemetry:stream:v1",
    "-Dcollector.report.mqtt.enabled=true",
    "-Dcollector.report.mqtt.broker-url=$MqttBrokerUrl",
    "-Dcollector.report.mqtt.gateway-product-key=soak-gateway-pk",
    "-Dcollector.report.mqtt.gateway-device-name=soak-gateway",
    "-Dcollector.report.mqtt.client-id=collector-soak-$runId",
    "-Dcollector.report.cloud.ack.timeout-ms=5000",
    "-Dcollector.report.interval-ms=1000",
    "-Dsoak.points=$Points",
    "-Dsoak.devices=$resolvedDevices",
    "-Dsoak.durationSeconds=$DurationSeconds",
    "-Dsoak.collectionIntervalMs=$CollectionIntervalMs",
    "-Dsoak.spreadWithinInterval=$SpreadWithinInterval",
    "-Dsoak.ingressMode=$IngressMode",
    "-Dsoak.scenario=$Scenario",
    "-Dsoak.metricsOutput=$MetricsOutput",
    "test"
  )
  if ($HistoryCoreSize -gt 0) {
    $arguments = $arguments[0..($arguments.Count - 2)] + "-Dcollector.telemetry-executors.history.core-size=$HistoryCoreSize" + $arguments[-1]
  }
  if ($HistoryMaxSize -gt 0) {
    $arguments = $arguments[0..($arguments.Count - 2)] + "-Dcollector.telemetry-executors.history.max-size=$HistoryMaxSize" + $arguments[-1]
  }
  if ($HistoryQueueCapacity -gt 0) {
    $arguments = $arguments[0..($arguments.Count - 2)] + "-Dcollector.telemetry-executors.history.queue-capacity=$HistoryQueueCapacity" + $arguments[-1]
  }
  Write-Host "Starting soak: points=$Points devices=$resolvedDevices duration=$DurationSeconds scenario=$Scenario output=$MetricsOutput"
  & mvn @arguments
  if ($LASTEXITCODE -ne 0) {
    throw "mvn failed with exit code $LASTEXITCODE"
  }
} finally {
  if ($null -ne $outageJob) {
    Wait-Job $outageJob | Out-Null
    Receive-Job $outageJob | Out-Host
    Remove-Job $outageJob
  }
}
