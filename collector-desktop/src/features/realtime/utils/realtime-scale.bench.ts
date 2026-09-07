import { bench, describe } from "vitest";

import { buildRealtimeSummary, normalizeAllDeviceRealtimeRows, normalizeRealtimeRows } from "./realtime-utils";
import {
  buildAllDeviceRealtimeScaleFixture,
  buildDeviceNameFinder,
  buildSingleDeviceRealtimeScaleFixture,
  estimatePayloadSizeMetric,
  filterRealtimeRowsForScaleBenchmark,
  REALTIME_SCALE_CASES,
  removeDeviceNameForFallbackBenchmark,
  SINGLE_DEVICE_SCALE_CASES
} from "./realtime-scale-fixture";

const BENCH_OPTIONS = {
  iterations: 8,
  warmupIterations: 2,
  time: 100,
  warmupTime: 25
};

const allDeviceCases = REALTIME_SCALE_CASES.map((scaleCase) => {
  const response = buildAllDeviceRealtimeScaleFixture(scaleCase);
  const json = JSON.stringify(response);
  const rows = normalizeAllDeviceRealtimeRows(response);
  const rowsWithoutDeviceName = removeDeviceNameForFallbackBenchmark(rows);
  const deviceNameFinder = buildDeviceNameFinder(scaleCase.deviceCount);
  const size = estimatePayloadSizeMetric(scaleCase.label, scaleCase.totalPoints, scaleCase.deviceCount, response);
  return { scaleCase, response, json, rows, rowsWithoutDeviceName, deviceNameFinder, size };
});

const singleDeviceCases = SINGLE_DEVICE_SCALE_CASES.map((scaleCase) => {
  const response = buildSingleDeviceRealtimeScaleFixture(scaleCase);
  const json = JSON.stringify(response);
  const rows = normalizeRealtimeRows(response, "scale-device-001");
  const size = estimatePayloadSizeMetric(scaleCase.label, scaleCase.totalPoints, scaleCase.deviceCount, response);
  return { scaleCase, response, json, rows, size };
});

describe("realtime scale payload", () => {
  for (const testCase of allDeviceCases) {
    bench(`all payload stringify ${testCase.scaleCase.label} rawBytes=${testCase.size.rawBytes}`, () => {
      JSON.stringify(testCase.response);
    }, BENCH_OPTIONS);

    bench(`all JSON parse ${testCase.scaleCase.label}`, () => {
      JSON.parse(testCase.json) as unknown;
    }, BENCH_OPTIONS);
  }
});

describe("realtime scale all-device frontend CPU", () => {
  for (const testCase of allDeviceCases) {
    bench(`all normalize ${testCase.scaleCase.label} rows=${testCase.rows.length}`, () => {
      normalizeAllDeviceRealtimeRows(testCase.response);
    }, BENCH_OPTIONS);

    bench(`all summary ${testCase.scaleCase.label}`, () => {
      buildRealtimeSummary(testCase.rows);
    }, BENCH_OPTIONS);

    bench(`all filter no keyword ${testCase.scaleCase.label}`, () => {
      filterRealtimeRowsForScaleBenchmark(testCase.rows, "");
    }, BENCH_OPTIONS);

    bench(`all filter many matches ${testCase.scaleCase.label}`, () => {
      filterRealtimeRowsForScaleBenchmark(testCase.rows, "规模点位");
    }, BENCH_OPTIONS);

    bench(`all filter zero matches ${testCase.scaleCase.label}`, () => {
      filterRealtimeRowsForScaleBenchmark(testCase.rows, "not-found-keyword");
    }, BENCH_OPTIONS);

    bench(`all filter fallback device lookup ${testCase.scaleCase.label}`, () => {
      filterRealtimeRowsForScaleBenchmark(testCase.rowsWithoutDeviceName, "not-found-keyword", testCase.deviceNameFinder);
    }, BENCH_OPTIONS);
  }
});

describe("realtime scale single-device frontend CPU", () => {
  for (const testCase of singleDeviceCases) {
    bench(`single normalize ${testCase.scaleCase.label} rows=${testCase.rows.length} rawBytes=${testCase.size.rawBytes}`, () => {
      normalizeRealtimeRows(testCase.response, "scale-device-001");
    }, BENCH_OPTIONS);

    bench(`single summary ${testCase.scaleCase.label}`, () => {
      buildRealtimeSummary(testCase.rows);
    }, BENCH_OPTIONS);

    bench(`single filter many matches ${testCase.scaleCase.label}`, () => {
      filterRealtimeRowsForScaleBenchmark(testCase.rows, "规模点位");
    }, BENCH_OPTIONS);

    bench(`single filter zero matches ${testCase.scaleCase.label}`, () => {
      filterRealtimeRowsForScaleBenchmark(testCase.rows, "not-found-keyword");
    }, BENCH_OPTIONS);
  }
});
