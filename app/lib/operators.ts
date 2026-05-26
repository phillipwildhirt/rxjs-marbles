import { Observable, SchedulerLike } from 'rxjs';
import {
  map,
  scan,
  filter,
  take,
  skip,
  takeLast,
  skipLast,
  first,
  last,
  elementAt,
  delay,
  delayWhen,
  debounce,
  debounceTime,
  throttle,
  throttleTime,
  distinctUntilChanged,
  distinct,
  pairwise,
  buffer,
  bufferCount,
  bufferTime,
  switchMap,
  mergeMap,
  concatMap,
  exhaustMap,
  expand,
  mergeScan,
  combineLatestWith,
  withLatestFrom,
  startWith,
  endWith,
  reduce,
  count,
  max,
  min,
  every,
  defaultIfEmpty,
  repeat,
  repeatWhen,
  takeUntil,
  skipUntil,
  takeWhile,
  skipWhile,
  sample,
  sampleTime,
  audit,
  auditTime,
  ignoreElements,
  toArray,
  tap,
  catchError,
  retry,
  retryWhen,
  timeout,
  finalize,
  window as windowOp,
  windowCount,
  windowTime,
  groupBy,
  pluck,
} from 'rxjs/operators';
import { merge, concat, zip, race, combineLatest, timer, of, from, interval, range, EMPTY, throwError, forkJoin, partition } from 'rxjs';



// ── Types ──────────────────────────────────────────────────────────────

/** A single marble on a timeline. t = time (0–100 scale), c = content/value */
export interface MarbleValue {
  t: number;
  c: string | number;
}

export interface MarbleError {
  t: number;
  error: string;
}

export type TimelineInput = Array<MarbleValue | MarbleError | number>;

export interface OperatorExample {
  label: string;
  inputs: TimelineInput[];
  apply: (inputs: Observable<MarbleValue>[], scheduler?: SchedulerLike) => Observable<unknown>;
}

export interface CategoryMap {
  [categoryName: string]: { [operatorName: string]: OperatorExample };
}

// ── Helpers ────────────────────────────────────────────────────────────

/** Extracts just the content value from a MarbleValue */
const content = (mv: MarbleValue) => mv.c;

/** Transforms the content of a MarbleValue */
const evolveContent = (fn: (c: MarbleValue['c']) => unknown) =>
  (mv: MarbleValue) => ({ ...mv, c: fn(mv.c) });

// ── Transformation Operators ───────────────────────────────────────────

const transformationExamples: Record<string, OperatorExample> = {
  map: {
    label : 'map(x => 10 * x)',
    inputs: [
      [{ t: 10, c: 1 }, { t: 20, c: 2 }, { t: 50, c: 3 }],
    ],
    apply : (inputs) =>
      inputs[0].pipe(map(evolveContent((c) => (c as number) * 10))),
  },

  scan: {
    label : 'scan((acc, x) => acc + x)',
    inputs: [
      [{ t: 5, c: 1 }, { t: 15, c: 2 }, { t: 25, c: 3 }, { t: 35, c: 4 }, { t: 65, c: 5 }],
    ],
    apply : (inputs) =>
      inputs[0].pipe(
        scan((acc: MarbleValue, curr) => ({
          ...curr,
          c: (acc.c as number) + (curr.c as number),
        })),
      ),
  },

  pairwise: {
    label : 'pairwise',
    inputs: [
      [{ t: 9, c: 'A' }, { t: 23, c: 'B' }, { t: 40, c: 'C' }, { t: 54, c: 'D' }, { t: 71, c: 'E' }],
    ],
    apply : (inputs) =>
      inputs[0].pipe(
        map(content),
        pairwise(),
        map(([a, b]) => `[${ a },${ b }]`),
      ),
  },

  bufferCount: {
    label : 'bufferCount(3, 2)',
    inputs: [
      [{ t: 9, c: 'A' }, { t: 23, c: 'B' }, { t: 40, c: 'C' }, { t: 54, c: 'D' }, { t: 71, c: 'E' }, { t: 85, c: 'F' }],
    ],
    apply : (inputs) =>
      inputs[0].pipe(
        map(content),
        bufferCount(3, 2),
        map((x) => `[${ x }]`),
      ),
  },

  switchMap: {
    label : 'obs1$.pipe(switchMap(() => obs2$))',
    inputs: [
      [{ t: 0, c: 'A' }, { t: 42, c: 'B' }, { t: 55, c: 'C' }],
      [{ t: 0, c: 1 }, { t: 10, c: 2 }, { t: 20, c: 3 }, 25],
    ],
    apply : (inputs) =>
      inputs[0].pipe(
        switchMap((x) =>
          inputs[1].pipe(map((y) => `${ x.c }${ y.c }`)),
        ),
      ),
  },

  mergeMap: {
    label : 'obs1$.pipe(mergeMap(() => obs2$, 2))',
    inputs: [
      [{ t: 0, c: 'A' }, { t: 3, c: 'B' }, { t: 6, c: 'C' }],
      [{ t: 0, c: 1 }, { t: 12, c: 2 }, { t: 24, c: 3 }, 28],
    ],
    apply : (inputs) =>
      inputs[0].pipe(
        mergeMap(
          (x) =>
            inputs[1].pipe(map((y) => `${ x.c }${ y.c }`)),
          2,
        ),
      ),
  },

  concatMap: {
    label : 'obs1$.pipe(concatMap(() => obs2$))',
    inputs: [
      [{ t: 0, c: 'A' }, { t: 42, c: 'B' }, { t: 55, c: 'C' }],
      [{ t: 0, c: 1 }, { t: 10, c: 2 }, { t: 20, c: 3 }, 25],
    ],
    apply : (inputs) =>
      inputs[0].pipe(
        concatMap((x) =>
          inputs[1].pipe(map((y) => `${ x.c }${ y.c }`)),
        ),
      ),
  },

  exhaustMap: {
    label : 'obs1$.pipe(exhaustMap(() => obs2$))',
    inputs: [
      [{ t: 0, c: 'A' }, { t: 42, c: 'B' }, { t: 55, c: 'C' }],
      [{ t: 0, c: 1 }, { t: 10, c: 2 }, { t: 20, c: 3 }, 25],
    ],
    apply : (inputs) =>
      inputs[0].pipe(
        exhaustMap((x) =>
          inputs[1].pipe(map((y) => `${ x.c }${ y.c }`)),
        ),
      ),
  },

  repeat: {
    label : 'repeat(3)',
    inputs: [
      [{ t: 0, c: 'A' }, { t: 12, c: 'B' }, 26],
    ],
    apply : (inputs) =>
      inputs[0].pipe(repeat(3)),
  },
};

// ── Filtering Operators ────────────────────────────────────────────────

const filteringExamples: Record<string, OperatorExample> = {
  filter: {
    label : 'filter(x => x > 2)',
    inputs: [
      [{ t: 5, c: 1 }, { t: 15, c: 2 }, { t: 25, c: 3 }, { t: 35, c: 4 }, { t: 65, c: 5 }],
    ],
    apply : (inputs) =>
      inputs[0].pipe(filter((mv) => (mv.c as number) > 2)),
  },

  take: {
    label : 'take(2)',
    inputs: [
      [{ t: 10, c: 1 }, { t: 20, c: 2 }, { t: 50, c: 3 }, { t: 60, c: 4 }],
    ],
    apply : (inputs) =>
      inputs[0].pipe(take(2)),
  },

  skip: {
    label : 'skip(2)',
    inputs: [
      [{ t: 10, c: 1 }, { t: 20, c: 2 }, { t: 50, c: 3 }, { t: 60, c: 4 }],
    ],
    apply : (inputs) =>
      inputs[0].pipe(skip(2)),
  },

  takeLast: {
    label : 'takeLast(2)',
    inputs: [
      [{ t: 10, c: 1 }, { t: 20, c: 2 }, { t: 50, c: 3 }, { t: 60, c: 4 }, 80],
    ],
    apply : (inputs) =>
      inputs[0].pipe(takeLast(2)),
  },

  distinctUntilChanged: {
    label : 'distinctUntilChanged',
    inputs: [
      [{ t: 5, c: 1 }, { t: 15, c: 1 }, { t: 25, c: 2 }, { t: 35, c: 2 }, { t: 65, c: 1 }],
    ],
    apply : (inputs) =>
      inputs[0].pipe(
        distinctUntilChanged((a, b) => a.c === b.c),
      ),
  },

  debounceTime: {
    label : 'debounceTime(10)',
    inputs: [
      [{ t: 0, c: 'A' }, { t: 6, c: 'B' }, { t: 24, c: 'C' }, { t: 40, c: 'D' }, { t: 45, c: 'E' }, { t: 80, c: 'F' }],
    ],
    apply : (inputs, scheduler) =>
      inputs[0].pipe(debounceTime(10, scheduler)),
  },

  'throttle (leading)': {
    label : 'throttle(() => timer(15), { leading: true, trailing: false })',
    inputs: [
      [{ t: 0, c: 'A' }, { t: 6, c: 'B' }, { t: 10, c: 'C' }, { t: 30, c: 'D' }, { t: 34, c: 'E' }, { t: 60, c: 'F' }, { t: 80, c: 'G' }],
    ],
    apply : (inputs, scheduler) =>
      inputs[0].pipe(throttle(() => timer(15, scheduler), { leading: true, trailing: false })),
  },

  'throttle (trailing)': {
    label : 'throttle(() => timer(15), { leading: false, trailing: true })',
    inputs: [
      [{ t: 0, c: 'A' }, { t: 6, c: 'B' }, { t: 10, c: 'C' }, { t: 30, c: 'D' }, { t: 34, c: 'E' }, { t: 60, c: 'F' }, { t: 80, c: 'G' }],
    ],
    apply : (inputs, scheduler) =>
      inputs[0].pipe(throttle(() => timer(15, scheduler), { leading: false, trailing: true })),
  },

  'throttleTime (leading)': {
    label : 'throttleTime(15, { leading: true, trailing: false })',
    inputs: [
      [{ t: 0, c: 'A' }, { t: 6, c: 'B' }, { t: 10, c: 'C' }, { t: 30, c: 'D' }, { t: 34, c: 'E' }, { t: 60, c: 'F' }, { t: 80, c: 'G' }],
    ],
    apply : (inputs, scheduler) =>
      inputs[0].pipe(throttleTime(15, scheduler, { leading: true, trailing: false })),
  },

  'throttleTime (trailing)': {
    label : 'throttleTime(15, { leading: false, trailing: true })',
    inputs: [
      [{ t: 0, c: 'A' }, { t: 6, c: 'B' }, { t: 10, c: 'C' }, { t: 30, c: 'D' }, { t: 34, c: 'E' }, { t: 60, c: 'F' }, { t: 80, c: 'G' }],
    ],
    apply : (inputs, scheduler) =>
      inputs[0].pipe(throttleTime(15, scheduler, { leading: false, trailing: true })),
  },

  'throttleTime (both)': {
    label : 'throttleTime(15, { leading: true, trailing: true })',
    inputs: [
      [{ t: 0, c: 'A' }, { t: 6, c: 'B' }, { t: 10, c: 'C' }, { t: 30, c: 'D' }, { t: 34, c: 'E' }, { t: 60, c: 'F' }, { t: 80, c: 'G' }],
    ],
    apply : (inputs, scheduler) =>
      inputs[0].pipe(throttleTime(15, scheduler, { leading: true, trailing: true })),
  },

  first: {
    label : 'first',
    inputs: [
      [{ t: 10, c: 'A' }, { t: 30, c: 'B' }, { t: 50, c: 'C' }, { t: 70, c: 'D' }],
    ],
    apply : (inputs) =>
      inputs[0].pipe(first()),
  },

  last: {
    label : 'last',
    inputs: [
      [{ t: 10, c: 'A' }, { t: 30, c: 'B' }, { t: 50, c: 'C' }, { t: 70, c: 'D' }, 85],
    ],
    apply : (inputs) =>
      inputs[0].pipe(last()),
  },

  elementAt: {
    label : 'elementAt(2)',
    inputs: [
      [{ t: 10, c: 'A' }, { t: 25, c: 'B' }, { t: 45, c: 'C' }, { t: 65, c: 'D' }],
    ],
    apply : (inputs) =>
      inputs[0].pipe(elementAt(2)),
  },

  skipLast: {
    label : 'skipLast(2)',
    inputs: [
      [{ t: 10, c: 1 }, { t: 25, c: 2 }, { t: 45, c: 3 }, { t: 65, c: 4 }, 80],
    ],
    apply : (inputs) =>
      inputs[0].pipe(skipLast(2)),
  },

  takeWhile: {
    label : 'takeWhile(x => x < 4)',
    inputs: [
      [{ t: 5, c: 1 }, { t: 20, c: 2 }, { t: 35, c: 3 }, { t: 50, c: 4 }, { t: 65, c: 5 }],
    ],
    apply : (inputs) =>
      inputs[0].pipe(takeWhile((mv) => (mv.c as number) < 4)),
  },

  skipWhile: {
    label : 'skipWhile(x => x < 3)',
    inputs: [
      [{ t: 5, c: 1 }, { t: 20, c: 2 }, { t: 35, c: 3 }, { t: 50, c: 4 }, { t: 65, c: 5 }],
    ],
    apply : (inputs) =>
      inputs[0].pipe(skipWhile((mv) => (mv.c as number) < 3)),
  },

  takeUntil: {
    label : 'takeUntil(notifier$)',
    inputs: [
      [{ t: 0, c: 'A' }, { t: 15, c: 'B' }, { t: 30, c: 'C' }, { t: 50, c: 'D' }, { t: 70, c: 'E' }],
      [{ t: 40, c: '!' }],
    ],
    apply : (inputs) =>
      inputs[0].pipe(takeUntil(inputs[1])),
  },

  skipUntil: {
    label : 'skipUntil(notifier$)',
    inputs: [
      [{ t: 0, c: 'A' }, { t: 15, c: 'B' }, { t: 30, c: 'C' }, { t: 50, c: 'D' }, { t: 70, c: 'E' }],
      [{ t: 40, c: '!' }],
    ],
    apply : (inputs) =>
      inputs[0].pipe(skipUntil(inputs[1])),
  },

  sample: {
    label : 'sample(notifier$)',
    inputs: [
      [{ t: 0, c: 'A' }, { t: 15, c: 'B' }, { t: 30, c: 'C' }, { t: 50, c: 'D' }, { t: 70, c: 'E' }],
      [{ t: 20, c: 0 }, { t: 45, c: 0 }, { t: 75, c: 0 }],
    ],
    apply : (inputs) =>
      inputs[0].pipe(sample(inputs[1])),
  },

  auditTime: {
    label : 'auditTime(15)',
    inputs: [
      [{ t: 0, c: 'A' }, { t: 6, c: 'B' }, { t: 24, c: 'C' }, { t: 40, c: 'D' }, { t: 45, c: 'E' }, { t: 80, c: 'F' }],
    ],
    apply : (inputs, scheduler) =>
      inputs[0].pipe(auditTime(15, scheduler)),
  },

  distinct: {
    label : 'distinct — only emits values never seen before',
    inputs: [
      [
        { t: 5, c: 'A' },
        { t: 15, c: 'B' },
        { t: 25, c: 'A' },
        { t: 35, c: 'C' },
        { t: 45, c: 'B' },
        { t: 55, c: 'A' },
        { t: 65, c: 'C' },
        { t: 80, c: 'D' }
      ],
    ],
    apply : (inputs) =>
      inputs[0].pipe(distinct((mv) => mv.c)),
  },

  ignoreElements: {
    label : 'ignoreElements',
    inputs: [
      [{ t: 10, c: 'A' }, { t: 30, c: 'B' }, { t: 50, c: 'C' }, 70],
    ],
    apply : (inputs) =>
      inputs[0].pipe(ignoreElements()),
  },
};

// ── Combination Operators ──────────────────────────────────────────────

const combinationExamples: Record<string, OperatorExample> = {
  merge: {
    label : 'merge(obs1$, obs2$)',
    inputs: [
      [{ t: 0, c: 20 }, { t: 15, c: 40 }, { t: 30, c: 60 }, { t: 45, c: 80 }, { t: 60, c: 100 }],
      [{ t: 2, c: 1 }, { t: 18, c: 1 }, { t: 40, c: 1 }],
    ],
    apply : (inputs) => merge(inputs[0], inputs[1]),
  },

  concat: {
    label : 'concat(obs1$, obs2$)',
    inputs: [
      [{ t: 0, c: 1 }, { t: 15, c: 2 }, { t: 30, c: 3 }, 40],
      [{ t: 0, c: 'A' }, { t: 15, c: 'B' }, { t: 30, c: 'C' }],
    ],
    apply : (inputs) => concat(inputs[0], inputs[1]),
  },

  zip: {
    label : 'zip(obs1$, obs2$, (x, y) => x + y)',
    inputs: [
      [{ t: 0, c: 1 }, { t: 15, c: 2 }, { t: 30, c: 3 }, { t: 55, c: 4 }],
      [{ t: 5, c: 'A' }, { t: 25, c: 'B' }, { t: 45, c: 'C' }],
    ],
    apply : (inputs) =>
      zip(inputs[0], inputs[1]).pipe(
        map(([a, b]) => `${ a.c }${ b.c }`),
      ),
  },

  combineLatest: {
    label : 'combineLatest(obs1$, obs2$, (x, y) => x + y)',
    inputs: [
      [{ t: 0, c: 1 }, { t: 25, c: 2 }, { t: 50, c: 3 }, { t: 75, c: 4 }],
      [{ t: 10, c: 'A' }, { t: 35, c: 'B' }, { t: 80, c: 'C' }],
    ],
    apply : (inputs) =>
      combineLatest([inputs[0], inputs[1]]).pipe(
        map(([a, b]) => `${ a.c }${ b.c }`),
      ),
  },

  withLatestFrom: {
    label : 'obs1$.pipe(withLatestFrom(obs2$, (x, y) => x + y))',
    inputs: [
      [{ t: 0, c: 1 }, { t: 25, c: 2 }, { t: 50, c: 3 }, { t: 75, c: 4 }],
      [{ t: 10, c: 'A' }, { t: 35, c: 'B' }, { t: 80, c: 'C' }],
    ],
    apply : (inputs) =>
      inputs[0].pipe(
        withLatestFrom(inputs[1]),
        map(([a, b]) => `${ a.c }${ b.c }`),
      ),
  },

  race: {
    label : 'race(obs1$, obs2$)',
    inputs: [
      [{ t: 20, c: 1 }, { t: 40, c: 2 }, { t: 60, c: 3 }],
      [{ t: 10, c: 'A' }, { t: 50, c: 'B' }, { t: 70, c: 'C' }],
    ],
    apply : (inputs) => race(inputs[0], inputs[1]),
  },

  startWith: {
    label : 'startWith("S")',
    inputs: [
      [{ t: 15, c: 'A' }, { t: 35, c: 'B' }, { t: 65, c: 'C' }],
    ],
    apply : (inputs) =>
      inputs[0].pipe(
        map(content),
        startWith('S'),
      ),
  },
};

// ── Mathematical Operators ─────────────────────────────────────────────

const mathExamples: Record<string, OperatorExample> = {
  count: {
    label : 'count',
    inputs: [
      [{ t: 5, c: 1 }, { t: 15, c: 2 }, { t: 25, c: 3 }, { t: 35, c: 4 }, { t: 65, c: 5 }, 80],
    ],
    apply : (inputs) =>
      inputs[0].pipe(count()),
  },

  max: {
    label : 'max',
    inputs: [
      [{ t: 5, c: 1 }, { t: 15, c: 5 }, { t: 25, c: 3 }, { t: 35, c: 2 }, { t: 65, c: 4 }, 80],
    ],
    apply : (inputs) =>
      inputs[0].pipe(
        map((mv) => mv.c as number),
        max(),
      ),
  },

  min: {
    label : 'min',
    inputs: [
      [{ t: 5, c: 3 }, { t: 15, c: 5 }, { t: 25, c: 1 }, { t: 35, c: 2 }, { t: 65, c: 4 }, 80],
    ],
    apply : (inputs) =>
      inputs[0].pipe(
        map((mv) => mv.c as number),
        min(),
      ),
  },

  reduce: {
    label : 'reduce((acc, x) => acc + x)',
    inputs: [
      [{ t: 5, c: 1 }, { t: 15, c: 2 }, { t: 25, c: 3 }, { t: 35, c: 4 }, { t: 65, c: 5 }, 80],
    ],
    apply : (inputs) =>
      inputs[0].pipe(
        map((mv) => mv.c as number),
        reduce((acc, curr) => acc + curr, 0),
      ),
  },
};

// ── Conditional Operators ──────────────────────────────────────────────

const conditionalExamples: Record<string, OperatorExample> = {
  every: {
    label : 'every(x => x < 5)',
    inputs: [
      [{ t: 5, c: 1 }, { t: 15, c: 2 }, { t: 25, c: 3 }, { t: 35, c: 4 }, 50],
    ],
    apply : (inputs) =>
      inputs[0].pipe(
        every((mv) => (mv.c as number) < 5),
      ),
  },

  defaultIfEmpty: {
    label : 'defaultIfEmpty(42)',
    inputs: [
      [80], // empty observable that just completes at t=80
    ],
    apply : (inputs) =>
      inputs[0].pipe(defaultIfEmpty(42)),
  },
};

// ── Creation Operators ─────────────────────────────────────────────────
// These are unique: they have NO input timelines. They create observables
// from scratch. The `inputs` array is empty, and `apply` ignores it.

const creationExamples: Record<string, OperatorExample> = {
  of: {
    label : 'of(1, 2, 3, 4, 5)',
    inputs: [],
    apply : () => of(1, 2, 3, 4, 5),
  },

  from: {
    label : 'from([10, 20, 30])',
    inputs: [],
    apply : () => from([10, 20, 30]),
  },

  range: {
    label : 'range(1, 5)',
    inputs: [],
    apply : () => range(1, 5),
  },

  interval: {
    label : 'interval(10).pipe(take(7))',
    inputs: [],
    apply : (_inputs, scheduler) =>
      interval(10, scheduler).pipe(take(7)),
  },

  timer: {
    label : 'timer(10, 15).pipe(take(5))',
    inputs: [],
    apply : (_inputs, scheduler) =>
      timer(10, 15, scheduler).pipe(take(5)),
  },

  'timer (single)': {
    label : 'timer(30) — emits 0 after delay, then completes',
    inputs: [],
    apply : (_inputs, scheduler) =>
      timer(30, scheduler),
  },

  EMPTY: {
    label : 'EMPTY — completes immediately with no values',
    inputs: [],
    apply : () => EMPTY,
  },

  throwError: {
    label : 'throwError(() => "oops!") — errors immediately',
    inputs: [],
    apply : () => throwError(() => 'oops!'),
  },
};

const errorHandlingExamples: Record<string, OperatorExample> = {
  catchError: {
    label: 'catchError(() => of("X", "Y"))',
    inputs: [
      [{ t: 10, c: 'A' }, { t: 30, c: 'B' }, { t: 50, error: 'oops' }],
    ],
    apply: (inputs) =>
      inputs[0].pipe(
        catchError(() => of('X', 'Y')),
      ),
  },

  retry: {
    label: 'retry(2)',
    inputs: [
      [{ t: 10, c: 'A' }, { t: 30, c: 'B' }, { t: 50, error: 'oops' }],
    ],
    apply: (inputs) =>
      inputs[0].pipe(
        retry(1),
      ),
  },
};

// ── Utility Operators ──────────────────────────────────────────────────

const utilityExamples: Record<string, OperatorExample> = {
  delay: {
    label: 'delay(15) — shifts every emission forward in time',
    inputs: [
      [{ t: 5, c: 'A' }, { t: 20, c: 'B' }, { t: 40, c: 'C' }, { t: 60, c: 'D' }],
    ],
    apply: (inputs, scheduler) =>
      inputs[0].pipe(delay(15, scheduler)),
  },

  delayWhen: {
    label: 'delayWhen(x => timer(x * 5)) — variable delay per value',
    inputs: [
      [{ t: 0, c: 1 }, { t: 10, c: 2 }, { t: 20, c: 3 }, { t: 30, c: 4 }, { t: 40, c: 5 }],
    ],
    apply: (inputs, scheduler) =>
      inputs[0].pipe(
        delayWhen((mv) => timer((mv.c as number) * 5, scheduler)),
      ),
  },

  toArray: {
    label: 'toArray — collects all values, emits as array on complete',
    inputs: [
      [{ t: 10, c: 'A' }, { t: 25, c: 'B' }, { t: 40, c: 'C' }, { t: 55, c: 'D' }, 70],
    ],
    apply: (inputs) =>
      inputs[0].pipe(
        map(content),
        toArray(),
        map((arr) => `[${arr}]`),
      ),
  },

  tap: {
    label: 'tap(console.log) — passthrough, side-effects only',
    inputs: [
      [{ t: 10, c: 1 }, { t: 30, c: 2 }, { t: 50, c: 3 }, { t: 70, c: 4 }],
    ],
    apply: (inputs) =>
      inputs[0].pipe(tap(() => { /* side effect here */ })),
  },

  timeout: {
    label: 'timeout(25) — errors if no value within 25ms',
    inputs: [
      [{ t: 5, c: 'A' }, { t: 20, c: 'B' }, { t: 55, c: 'C' }],
    ],
    apply: (inputs, scheduler) =>
      inputs[0].pipe(timeout({ each: 25, scheduler })),
  },

  'timeout (first)': {
    label: 'timeout({ first: 30 }) — errors if first value takes > 30ms',
    inputs: [
      [{ t: 40, c: 'A' }, { t: 55, c: 'B' }, { t: 70, c: 'C' }],
    ],
    apply: (inputs, scheduler) =>
      inputs[0].pipe(timeout({ first: 30, scheduler })),
  },
};

// ── More Transformation Operators ──────────────────────────────────────

const evenMoreTransformation: Record<string, OperatorExample> = {
  buffer: {
    label: 'buffer(notifier$) — collect until notifier emits',
    inputs: [
      [{ t: 5, c: 'A' }, { t: 15, c: 'B' }, { t: 25, c: 'C' }, { t: 45, c: 'D' }, { t: 60, c: 'E' }, { t: 80, c: 'F' }],
      [{ t: 30, c: 0 }, { t: 65, c: 0 }, { t: 90, c: 0 }],
    ],
    apply: (inputs) =>
      inputs[0].pipe(
        map(content),
        buffer(inputs[1]),
        map((x: unknown[]) => `[${x}]`),
      ),
  },

  bufferTime: {
    label: 'bufferTime(25)',
    inputs: [
      [{ t: 0, c: 'A' }, { t: 8, c: 'B' }, { t: 18, c: 'C' }, { t: 35, c: 'D' }, { t: 55, c: 'E' }, { t: 80, c: 'F' }],
    ],
    apply: (inputs, scheduler) =>
      inputs[0].pipe(
        map(content),
        bufferTime(25, scheduler),
        map((x) => `[${x}]`),
      ),
  },

  switchScan: {
    label: 'switchScan((acc, x) => of(acc + x), 0)',
    inputs: [
      [{ t: 5, c: 1 }, { t: 20, c: 2 }, { t: 35, c: 3 }, { t: 50, c: 4 }, { t: 70, c: 5 }],
    ],
    apply: (inputs) =>
      inputs[0].pipe(
        mergeScan((acc, curr) => of((acc as number) + (curr.c as number)), 0),
      ),
  },

  'mergeMap (no limit)': {
    label: 'mergeMap(() => obs2$) — unlimited concurrency',
    inputs: [
      [{ t: 0, c: 'A' }, { t: 15, c: 'B' }, { t: 30, c: 'C' }],
      [{ t: 0, c: 1 }, { t: 10, c: 2 }, { t: 20, c: 3 }, 25],
    ],
    apply: (inputs) =>
      inputs[0].pipe(
        mergeMap((x) => inputs[1].pipe(map((y) => `${x.c}${y.c}`))),
      ),
  },

  'map (to object)': {
    label: 'map(x => { val: x, squared: x*x })',
    inputs: [
      [{ t: 10, c: 1 }, { t: 25, c: 2 }, { t: 40, c: 3 }, { t: 55, c: 4 }, { t: 75, c: 5 }],
    ],
    apply: (inputs) =>
      inputs[0].pipe(
        map((mv) => `${mv.c}→${(mv.c as number) ** 2}`),
      ),
  },

  'scan (with seed)': {
    label: 'scan((acc, x) => acc + x, 100)',
    inputs: [
      [{ t: 5, c: 1 }, { t: 20, c: 2 }, { t: 35, c: 3 }, { t: 50, c: 4 }, { t: 70, c: 5 }],
    ],
    apply: (inputs) =>
      inputs[0].pipe(
        scan((acc, curr) => (acc as number) + (curr.c as number), 100),
      ),
  },
};

// Fix buffer to use actual buffer operator with notifier
evenMoreTransformation.buffer = {
  label: 'buffer(notifier$) — collect until notifier emits',
  inputs: [
    [{ t: 5, c: 'A' }, { t: 15, c: 'B' }, { t: 25, c: 'C' }, { t: 45, c: 'D' }, { t: 60, c: 'E' }, { t: 80, c: 'F' }],
    [{ t: 30, c: 0 }, { t: 65, c: 0 }, { t: 90, c: 0 }],
  ],
  apply: (inputs) => {
    const { buffer } = require('rxjs/operators');
    return inputs[0].pipe(
      map(content),
      buffer(inputs[1]),
      map((x: unknown[]) => `[${x}]`),
    );
  },
};

const moreTransformationExamples: Record<string, OperatorExample> = {
  expand: {
    label: 'expand(x => x < 30 ? of(x * 2) : EMPTY)',
    inputs: [
      [{ t: 5, c: 2 }],
    ],
    apply: (inputs) =>
      inputs[0].pipe(
        map((mv) => mv.c as number),
        expand((x) => (x as number) < 30 ? of((x as number) * 2) : EMPTY),
      ),
  },

  mergeScan: {
    label: 'mergeScan((acc, x) => of(acc + x), 0)',
    inputs: [
      [{ t: 5, c: 1 }, { t: 20, c: 2 }, { t: 35, c: 3 }, { t: 50, c: 4 }, { t: 70, c: 5 }],
    ],
    apply: (inputs) =>
      inputs[0].pipe(
        mergeScan((acc, curr) => of((acc as number) + (curr.c as number)), 0),
      ),
  },

  groupBy: {
    label: 'groupBy(x => x % 2 === 0 ? "even" : "odd") — first group only',
    inputs: [
      [{ t: 5, c: 1 }, { t: 15, c: 2 }, { t: 25, c: 3 }, { t: 35, c: 4 }, { t: 50, c: 5 }, { t: 65, c: 6 }],
    ],
    apply: (inputs) =>
      inputs[0].pipe(
        groupBy((mv) => (mv.c as number) % 2 === 0 ? 'even' : 'odd'),
        mergeMap((group$) => group$.pipe(map((mv) => `${group$.key}:${mv.c}`))),
      ),
  },

  windowCount: {
    label: 'windowCount(3) — split into windows of 3',
    inputs: [
      [{ t: 5, c: 'A' }, { t: 15, c: 'B' }, { t: 25, c: 'C' }, { t: 40, c: 'D' }, { t: 55, c: 'E' }, { t: 70, c: 'F' }, { t: 85, c: 'G' }],
    ],
    apply: (inputs) =>
      inputs[0].pipe(
        map(content),
        windowCount(3),
        mergeMap((win$) => win$.pipe(toArray(), map((arr) => `[${arr}]`))),
      ),
  },

  endWith: {
    label: 'endWith("Z")',
    inputs: [
      [{ t: 10, c: 'A' }, { t: 30, c: 'B' }, { t: 50, c: 'C' }, 65],
    ],
    apply: (inputs) =>
      inputs[0].pipe(
        map(content),
        endWith('Z'),
      ),
  },

  toArray: {
    label: 'toArray — collects all, emits as array on complete',
    inputs: [
      [{ t: 10, c: 'A' }, { t: 25, c: 'B' }, { t: 40, c: 'C' }, { t: 55, c: 'D' }, 70],
    ],
    apply: (inputs) =>
      inputs[0].pipe(
        map(content),
        toArray(),
        map((arr) => `[${arr}]`),
      ),
  },
};

// ── More Filtering Operators ──────────────────────────────────────────

const moreFilteringExamples: Record<string, OperatorExample> = {
  debounce: {
    label: 'debounce(() => timer(10)) — like debounceTime but observable-based',
    inputs: [
      [{ t: 0, c: 'A' }, { t: 6, c: 'B' }, { t: 24, c: 'C' }, { t: 40, c: 'D' }, { t: 45, c: 'E' }, { t: 80, c: 'F' }],
    ],
    apply: (inputs, scheduler) =>
      inputs[0].pipe(debounce(() => timer(10, scheduler))),
  },

  sampleTime: {
    label: 'sampleTime(20) — emits latest value every 20ms',
    inputs: [
      [{ t: 0, c: 'A' }, { t: 8, c: 'B' }, { t: 15, c: 'C' }, { t: 35, c: 'D' }, { t: 55, c: 'E' }, { t: 80, c: 'F' }],
    ],
    apply: (inputs, scheduler) =>
      inputs[0].pipe(sampleTime(20, scheduler)),
  },

  audit: {
    label: 'audit(() => timer(15)) — like auditTime but observable-based',
    inputs: [
      [{ t: 0, c: 'A' }, { t: 6, c: 'B' }, { t: 24, c: 'C' }, { t: 40, c: 'D' }, { t: 45, c: 'E' }, { t: 80, c: 'F' }],
    ],
    apply: (inputs, scheduler) =>
      inputs[0].pipe(audit(() => timer(15, scheduler))),
  },

  'first (with predicate)': {
    label: 'first(x => x > 2)',
    inputs: [
      [{ t: 5, c: 1 }, { t: 20, c: 2 }, { t: 35, c: 3 }, { t: 50, c: 4 }, { t: 65, c: 5 }],
    ],
    apply: (inputs) =>
      inputs[0].pipe(first((mv) => (mv.c as number) > 2)),
  },

  'last (with predicate)': {
    label: 'last(x => x < 4)',
    inputs: [
      [{ t: 5, c: 1 }, { t: 20, c: 2 }, { t: 35, c: 3 }, { t: 50, c: 4 }, { t: 65, c: 5 }, 80],
    ],
    apply: (inputs) =>
      inputs[0].pipe(last((mv) => (mv.c as number) < 4)),
  },
};

// ── More Combination Operators ────────────────────────────────────────

const moreCombinationExamples: Record<string, OperatorExample> = {
  forkJoin: {
    label: 'forkJoin(obs1$, obs2$) — waits for both to complete',
    inputs: [
      [{ t: 10, c: 'A' }, { t: 30, c: 'B' }, { t: 50, c: 'C' }, 60],
      [{ t: 20, c: 1 }, { t: 40, c: 2 }, { t: 70, c: 3 }, 80],
    ],
    apply: (inputs) =>
      forkJoin([inputs[0], inputs[1]]).pipe(
        map(([a, b]) => `${a.c},${b.c}`),
      ),
  },

  'merge (3 streams)': {
    label: 'merge(obs1$, obs2$, obs3$)',
    inputs: [
      [{ t: 0, c: 'A' }, { t: 30, c: 'B' }, { t: 60, c: 'C' }],
      [{ t: 10, c: 1 }, { t: 40, c: 2 }, { t: 70, c: 3 }],
      [{ t: 20, c: 'X' }, { t: 50, c: 'Y' }, { t: 80, c: 'Z' }],
    ],
    apply: (inputs) => merge(inputs[0], inputs[1], inputs[2]),
  },

  'combineLatest (3 streams)': {
    label: 'combineLatest(obs1$, obs2$, obs3$)',
    inputs: [
      [{ t: 0, c: 'A' }, { t: 40, c: 'B' }, { t: 80, c: 'C' }],
      [{ t: 15, c: 1 }, { t: 55, c: 2 }],
      [{ t: 30, c: 'X' }, { t: 70, c: 'Y' }],
    ],
    apply: (inputs) =>
      combineLatest([inputs[0], inputs[1], inputs[2]]).pipe(
        map(([a, b, c]) => `${a.c}${b.c}${c.c}`),
      ),
  },
};

// ── More Error Handling ───────────────────────────────────────────────

const moreErrorExamples: Record<string, OperatorExample> = {
  'catchError (rethrow)': {
    label: 'catchError((err, caught$) => caught$) — retry via caught',
    inputs: [
      [{ t: 10, c: 'A' }, { t: 30, c: 'B' }, { t: 50, error: 'fail' }],
    ],
    apply: (inputs) => {
      let attempts = 0;
      return inputs[0].pipe(
        catchError((_err, caught$) => {
          attempts++;
          return attempts < 2 ? caught$ : throwError(() => 'gave up');
        }),
      );
    },
  },

  'retry (with delay)': {
    label: 'retry({ count: 2, delay: 10 })',
    inputs: [
      [{ t: 5, c: 'A' }, { t: 15, c: 'B' }, { t: 25, error: 'fail' }],
    ],
    apply: (inputs, scheduler) =>
      inputs[0].pipe(
        retry({ count: 2, delay: 10 }),
      ),
  },
};

// ── Categories ─────────────────────────────────────────────────────────

const categories: CategoryMap = {
  'Creation Operators'      : creationExamples,
  'Transformation Operators': { ...transformationExamples, ...moreTransformationExamples, ...evenMoreTransformation },
  'Filtering Operators'     : { ...filteringExamples, ...moreFilteringExamples },
  'Combination Operators'   : { ...combinationExamples, ...moreCombinationExamples },
  'Mathematical Operators'  : mathExamples,
  'Conditional Operators'   : conditionalExamples,
  'Error Handling Operators': { ...errorHandlingExamples, ...moreErrorExamples },
  'Utility Operators'       : utilityExamples,
};

export { sortedCategories as categories };

// ── Sorting Helpers ────────────────────────────────────────────────────

/** Sort the keys of a Record alphabetically (case-insensitive) */
function sortRecord<T>(rec: Record<string, T>): Record<string, T> {
  return Object.keys(rec)
    .sort((a, b) => a.localeCompare(b, undefined, { sensitivity: 'base' }))
    .reduce((acc, key) => { acc[key] = rec[key]; return acc; }, {} as Record<string, T>);
}

/** Sort every category's operators and the categories themselves */
function sortCategories(cats: CategoryMap): CategoryMap {
  return Object.keys(cats)
    .sort((a, b) => a.localeCompare(b, undefined, { sensitivity: 'base' }))
    .reduce((acc, catName) => {
      acc[catName] = sortRecord(cats[catName]);
      return acc;
    }, {} as CategoryMap);
}

// Apply sorting to the exported categories
const sortedCategories = sortCategories(categories);

/** Flat map of all operator examples keyed by operator name (sorted) */
export const allExamples: Record<string, OperatorExample> = sortRecord(
  Object.values(sortedCategories).reduce(
    (acc, cat) => ({ ...acc, ...cat }),
    {} as Record<string, OperatorExample>,
  ),
);

/** List of all operator names (sorted) */
export const operatorNames: string[] = Object.keys(allExamples);
