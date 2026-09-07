import type { AllDeviceRealtimeDataResponse, DeviceRealtimeDataResponse, PointRealtimePayload, RealtimePointRow } from "@/types/monitor";

/**
 * 仅供 benchmark/test 使用的实时规模数据集定义，生产代码不得 import。
 */
export interface RealtimeScaleCase {
  label: string;
  totalPoints: number;
  deviceCount: number;
  pointsPerDevice: number;
}

/**
 * 仅供 benchmark/test 使用的单设备规模数据集定义，生产代码不得 import。
 */
export interface SingleDeviceScaleCase {
  label: string;
  totalPoints: number;
  deviceCount: 1;
  pointsPerDevice: number;
}

export interface RealtimePayloadSizeMetric {
  label: string;
  totalPoints: number;
  deviceCount: number;
  rawBytes: number;
  bytesPerPoint: number;
  mibPerRefresh: number;
  mibPerSecond: number;
  mibPerMinute: number;
  gibPerHour: number;
}

export const REALTIME_SCALE_CASES: RealtimeScaleCase[] = [
  { label: "10k/10 devices", totalPoints: 10_000, deviceCount: 10, pointsPerDevice: 1_000 },
  { label: "50k/50 devices", totalPoints: 50_000, deviceCount: 50, pointsPerDevice: 1_000 },
  { label: "100k/100 devices", totalPoints: 100_000, deviceCount: 100, pointsPerDevice: 1_000 }
];

export const SINGLE_DEVICE_SCALE_CASES: SingleDeviceScaleCase[] = [
  { label: "single 1k", totalPoints: 1_000, deviceCount: 1, pointsPerDevice: 1_000 },
  { label: "single 5k", totalPoints: 5_000, deviceCount: 1, pointsPerDevice: 5_000 },
  { label: "single 10k", totalPoints: 10_000, deviceCount: 1, pointsPerDevice: 10_000 }
];

const FIVE_SECOND_REFRESHES_PER_SECOND = 1 / 5;
const SECONDS_PER_MINUTE = 60;
const SECONDS_PER_HOUR = 3_600;
const BYTES_PER_MIB = 1024 * 1024;
const BYTES_PER_GIB = 1024 * 1024 * 1024;

export function buildAllDeviceRealtimeScaleFixture(scaleCase: RealtimeScaleCase): AllDeviceRealtimeDataResponse {
  const devices: DeviceRealtimeDataResponse[] = [];
  let generated = 0;
  const baseTimestamp = 1_800_000_000_000;
  for (let deviceIndex = 0; deviceIndex < scaleCase.deviceCount; deviceIndex += 1) {
    const remaining = scaleCase.totalPoints - generated;
    const pointsForDevice = Math.min(scaleCase.pointsPerDevice, remaining);
    const deviceId = `scale-device-${String(deviceIndex + 1).padStart(3, "0")}`;
    const deviceName = `规模测试设备${String(deviceIndex + 1).padStart(3, "0")}`;
    const data: Record<string, PointRealtimePayload> = {};
    for (let pointOffset = 0; pointOffset < pointsForDevice; pointOffset += 1) {
      const globalIndex = generated + pointOffset;
      const pointId = buildPointId(globalIndex);
      data[pointId] = buildPointPayload(deviceId, deviceName, globalIndex, baseTimestamp + globalIndex * 1_000);
    }
    devices.push({
      status: "success",
      deviceId,
      dataCount: pointsForDevice,
      data,
      timestamp: baseTimestamp + deviceIndex
    });
    generated += pointsForDevice;
  }
  return {
    status: "success",
    deviceCount: devices.length,
    dataCount: generated,
    devices,
    timestamp: baseTimestamp
  };
}

export function buildSingleDeviceRealtimeScaleFixture(scaleCase: SingleDeviceScaleCase): DeviceRealtimeDataResponse {
  const allDevice = buildAllDeviceRealtimeScaleFixture(scaleCase);
  return allDevice.devices?.[0] || { status: "success", deviceId: "scale-device-001", dataCount: 0, data: {}, timestamp: allDevice.timestamp };
}

export function estimatePayloadSizeMetric(label: string, totalPoints: number, deviceCount: number, payload: unknown): RealtimePayloadSizeMetric {
  const rawBytes = utf8ByteLength(JSON.stringify(payload));
  const bytesPerPoint = totalPoints > 0 ? rawBytes / totalPoints : 0;
  const mibPerRefresh = rawBytes / BYTES_PER_MIB;
  const mibPerSecond = mibPerRefresh * FIVE_SECOND_REFRESHES_PER_SECOND;
  const mibPerMinute = mibPerSecond * SECONDS_PER_MINUTE;
  const gibPerHour = rawBytes * FIVE_SECOND_REFRESHES_PER_SECOND * SECONDS_PER_HOUR / BYTES_PER_GIB;
  return {
    label,
    totalPoints,
    deviceCount,
    rawBytes,
    bytesPerPoint,
    mibPerRefresh,
    mibPerSecond,
    mibPerMinute,
    gibPerHour
  };
}

/**
 * RealtimeView 当前本地搜索逻辑的 benchmark 镜像，生产实现仍保留在组件内。
 */
export function filterRealtimeRowsForScaleBenchmark(
  rows: RealtimePointRow[],
  keyword: string,
  deviceDisplayName: (deviceId: string) => string = (deviceId) => deviceId || "-"
): RealtimePointRow[] {
  const normalizedKeyword = keyword.trim().toLowerCase();
  if (!normalizedKeyword) {
    return rows;
  }
  return rows.filter((row) => {
    const searchableValues = [
      row.pointName,
      row.pointCode,
      String(row.address || row.registerAddress || row.pointAddress || "-"),
      row.deviceName || deviceDisplayName(String(row.deviceId || ""))
    ];
    return searchableValues.some((value) => String(value || "").toLowerCase().includes(normalizedKeyword));
  });
}

/**
 * 用于量化 `deviceDisplayName()` fallback 的最坏情况，生产代码未使用该索引。
 */
export function buildDeviceNameFinder(deviceCount: number): (deviceId: string) => string {
  const devices = Array.from({ length: deviceCount }, (_, index) => {
    const normalizedId = `scale-device-${String(index + 1).padStart(3, "0")}`;
    return {
      normalizedId,
      displayName: `规模测试设备${String(index + 1).padStart(3, "0")}`
    };
  });
  return (deviceId: string) => devices.find((device) => device.normalizedId === deviceId)?.displayName || deviceId || "-";
}

export function removeDeviceNameForFallbackBenchmark(rows: RealtimePointRow[]): RealtimePointRow[] {
  return rows.map(({ deviceName: _deviceName, ...row }) => row);
}

export function domLowerBound(totalPoints: number): { rows: number; cells: number } {
  return {
    rows: totalPoints,
    cells: totalPoints * 12
  };
}

function buildPointPayload(deviceId: string, deviceName: string, index: number, timestamp: number): PointRealtimePayload {
  const pointNumber = index + 1;
  const pointId = buildPointId(index);
  const qualityGood = index % 20 !== 0;
  const value = Number((20 + (index % 10) * 0.25).toFixed(2));
  return {
    id: pointNumber,
    unitId: 1 + (index % 4),
    commonAddress: 1,
    pointId,
    pointCode: `scale_point_${String(pointNumber).padStart(6, "0")}`,
    pointName: `规模点位${String(pointNumber).padStart(6, "0")}`,
    pointAlias: `scale-alias-${pointNumber}`,
    deviceId,
    deviceName,
    groupId: `group-${String((index % 10) + 1).padStart(2, "0")}`,
    address: `400${String(pointNumber).padStart(5, "0")}`,
    dataType: index % 3 === 0 ? "DOUBLE" : "FLOAT",
    readWrite: index % 25 === 0 ? "RW" : "R",
    scalingFactor: 1,
    offset: 0,
    deadband: 0.1,
    unit: index % 2 === 0 ? "℃" : "MPa",
    minValue: -100,
    maxValue: 200,
    collectionMode: "AUTO",
    priority: index % 5,
    cacheEnabled: 1,
    cacheDuration: 60,
    alarmEnabled: index % 10 === 0 ? 1 : 0,
    alarmRule: index % 10 === 0 ? "high-limit" : undefined,
    status: 1,
    createTime: timestamp - 86_400_000,
    updateTime: timestamp - 3_600_000,
    precision: 2,
    remark: "规模 benchmark 代表性点位",
    additionalConfig: {
      driverDataType: "float32",
      registerType: "holding-register",
      byteOrder: "ABCD",
      sampling: {
        interval: 5_000,
        jitterMs: index % 50
      }
    },
    baseCollectionInterval: 5_000,
    currentCollectionInterval: 5_000 + (index % 5) * 100,
    minCollectionInterval: 1_000,
    maxCollectionInterval: 60_000,
    pointChangeThreshold: 0.5,
    stableCount: index % 8,
    lastValue: value - 0.1,
    changeRate: Number(((index % 7) * 0.01).toFixed(3)),
    lastAdjustTime: timestamp - 1_000,
    value,
    rawValue: value,
    processedValue: value,
    hasCachedValue: true,
    quality: qualityGood ? 100 : 20,
    qualityDescription: qualityGood ? "数据质量正常" : "数据质量异常",
    qualityLevel: qualityGood ? "GOOD" : "BAD",
    qualityAcceptable: qualityGood,
    qualityAvailable: true,
    processMessage: qualityGood ? "处理成功" : "质量检查失败",
    processSuccess: qualityGood,
    skipped: false,
    processorName: "DataQualityProcessor",
    processingTime: 2 + (index % 6),
    processingTimeAvailable: true,
    metadata: {
      source: "scale-benchmark",
      collectTime: timestamp,
      sequence: pointNumber
    },
    lastUpdateTime: timestamp,
    timestamp
  };
}

function buildPointId(index: number): string {
  return `scale-point-${String(index + 1).padStart(6, "0")}`;
}

function utf8ByteLength(value: string): number {
  return new TextEncoder().encode(value).length;
}
