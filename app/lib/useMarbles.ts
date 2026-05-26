'use client';

import { useState, useEffect, useCallback, useMemo } from 'react';
import {
  Observable,
  VirtualTimeScheduler,
  VirtualAction,
  Subscription,
} from 'rxjs';
import {
  type MarbleValue,
  type OperatorExample,
  type TimelineInput,
  allExamples,
} from './operators';

// ── Types ──────────────────────────────────────────────────────────────

/** A marble with an ID for React keys + drag tracking */
export interface Marble {
  id: string;
  t: number;
  c: string | number;
}

/** A timeline of marbles with an optional completion time and/or error */
export interface Timeline {
  marbles: Marble[];
  completion?: number; // t value where the stream completes
  error?: { t: number; message: string }; // t value + message where the stream errors
}

export interface UseMarblesResult {
  /** The current input timelines (draggable) */
  inputTimelines: Timeline[];
  /** The computed output timeline */
  outputTimeline: Timeline;
  /** The operator label (e.g. "map(x => 10 * x)") */
  label: string;
  /** Move a marble on an input timeline to a new time position */
  moveMarble: (timelineIndex: number, marbleId: string, newT: number) => void;
  /** Move the error marker on an input timeline to a new time position */
  moveError: (timelineIndex: number, newT: number) => void;
}

// ── Helpers ────────────────────────────────────────────────────────────

/** Maximum time value for the timeline scale */
const MAX_TIME = 100;

/** Parse a TimelineInput into a Timeline (marbles + optional completion) */
function parseTimelineInput(input: TimelineInput, timelineIdx: number): Timeline {
  const marbles: Marble[] = [];
  let completion: number | undefined;
  let error: { t: number; message: string } | undefined;

  input.forEach((item, i) => {
    if (typeof item === 'number') {
      // A plain number at the end is the completion time
      completion = item;
    } else if ('error' in item) {
      // An object with `error` key is an error marker: { t: 50, error: 'oops' }
      error = { t: item.t, message: item.error };
    } else {
      marbles.push({
        id: `t${timelineIdx}-m${i}`,
        t: item.t,
        c: item.c,
      });
    }
  });

  return { marbles, completion, error };
}

/** Convert a Timeline back into an Observable using a VirtualTimeScheduler */
function timelineToObservable(
  timeline: Timeline,
  scheduler: VirtualTimeScheduler,
): Observable<MarbleValue> {
  return new Observable<MarbleValue>((subscriber) => {
    // Sort marbles by time to ensure correct emission order
    const sorted = [...timeline.marbles].sort((a, b) => a.t - b.t);

    sorted.forEach((marble) => {
      scheduler.schedule(() => {
        subscriber.next({ t: marble.t, c: marble.c });
      }, marble.t);
    });

    if (timeline.error) {
      scheduler.schedule(() => {
        subscriber.error(timeline.error!.message);
      }, timeline.error.t);
    }

    // Schedule completion
    const completionTime = timeline.completion ?? MAX_TIME;
    scheduler.schedule(() => {
      subscriber.complete();
    }, completionTime);
  });
}

/** Run the operator and compute the output timeline synchronously */
function computeOutput(
  inputTimelines: Timeline[],
  example: OperatorExample,
): Timeline {
  const scheduler = new VirtualTimeScheduler(VirtualAction, MAX_TIME);
  const outputMarbles: Marble[] = [];
  let outputCompletion: number | undefined;
  let outputError: { t: number; message: string } | undefined;
  let marbleIndex = 0;

  // Create input observables from timelines
  const inputObservables = inputTimelines.map((tl) =>
    timelineToObservable(tl, scheduler),
  );

  // Subscribe to the operator's output
  const output$ = example.apply(inputObservables, scheduler);

  const sub = new Subscription();
  sub.add(
    output$.subscribe({
      next: (value) => {
        // Determine the time position from the scheduler's current frame
        const t = Math.round(scheduler.frame);

        // Extract content — could be a MarbleValue or a raw value
        let c: string | number;
        if (value !== null && typeof value === 'object' && 't' in value && 'c' in value) {
          c = (value as MarbleValue).c;
        } else {
          c = value as string | number;
        }

        outputMarbles.push({
          id: `out-m${marbleIndex++}`,
          t: Math.min(t, MAX_TIME),
          c,
        });
      },
      error: (err) => {
        outputError = {
          t: Math.round(scheduler.frame),
          message: typeof err === 'string' ? err : err?.message ?? 'Error',
        };
      },
      complete: () => {
        outputCompletion = Math.round(scheduler.frame);
      },
    }),
  );

  // Flush all scheduled actions synchronously
  scheduler.flush();
  sub.unsubscribe();

  return {
    marbles: outputMarbles,
    completion: outputCompletion,
    error: outputError,
  };
}

// ── Hook ───────────────────────────────────────────────────────────────

/**
 * React hook that takes an operator name, manages input marble state,
 * and computes the output marble diagram reactively.
 *
 * Usage:
 * ```tsx
 * const { inputTimelines, outputTimeline, label, moveMarble } = useMarbles('map');
 * ```
 */
export function useMarbles(operatorName: string): UseMarblesResult {
  const example = useMemo(() => allExamples[operatorName], [operatorName]);

  // Parse initial input timelines from the example definition
  const initialTimelines = useMemo(
    () => example.inputs.map((input, idx) => parseTimelineInput(input, idx)),
    [example],
  );

  const [inputTimelines, setInputTimelines] = useState<Timeline[]>(initialTimelines);

  // Reset inputs when the operator changes
  useEffect(() => {
    setInputTimelines(initialTimelines);
  }, [initialTimelines]);

  // Compute output whenever inputs change.
  // Guard: when switching operators, inputTimelines might still hold the previous
  // operator's data (useEffect hasn't fired yet). Fall back to initialTimelines
  // if the input count doesn't match what the operator expects.
  const safeInputs = inputTimelines.length === example.inputs.length
    ? inputTimelines
    : initialTimelines;

  const outputTimeline = useMemo(
    () => computeOutput(safeInputs, example),
    [safeInputs, example],
  );

  // Drag handler: move a marble to a new time position
  const moveMarble = useCallback(
    (timelineIndex: number, marbleId: string, newT: number) => {
      setInputTimelines((prev) =>
        prev.map((tl, idx) => {
          if (idx !== timelineIndex) return tl;
          return {
            ...tl,
            marbles: tl.marbles.map((m) =>
              m.id === marbleId
              ? { ...m, t: Math.max(0, Math.min(MAX_TIME, Math.round(newT))) }
              : m,
            ),
          };
        }),
      );
    },
    [],
  );


  const moveError = useCallback(
    (timelineIndex: number, newT: number) => {
      setInputTimelines((prev) =>
        prev.map((tl, idx) => {
          if (idx !== timelineIndex || !tl.error) return tl;
          return {
            ...tl,
            error: {
              ...tl.error,
              t: Math.max(0, Math.min(MAX_TIME, Math.round(newT))),
            },
          };
        }),
      );
    },
    [],
  );

  return {
    inputTimelines,
    outputTimeline,
    label: example.label,
    moveMarble,
    moveError,
  };

}
