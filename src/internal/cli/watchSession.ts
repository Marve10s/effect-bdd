import * as Arr from "effect/Array";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Fn from "effect/Function";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Schedule from "effect/Schedule";
import * as Str from "effect/String";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import type { PlatformError } from "effect/PlatformError";
import type { CliOptions } from "./models.ts";
import * as Watch from "./watch.ts";

/** @internal */
export type WatchSessionError = PlatformError;

/**
 * Everything the watch session needs that does not change between reruns:
 * the parsed CLI options, and the child process invocation (interpreter, script
 * path, and the user's flags with `--watch` removed).
 *
 * @internal
 */
export interface WatchSessionOptions {
  readonly cli: CliOptions;
  readonly invocation: {
    readonly executable: string;
    readonly script: string;
    readonly args: ReadonlyArray<string>;
  };
}

/** @internal */
export interface RunOutcome {
  readonly failed: boolean;
}

/**
 * Build the child process invocation from the raw process argv: the
 * interpreter, the script path, and the user's flags with `--watch` removed so
 * a child process does not start its own watcher.
 *
 * @internal
 */
export const childInvocation = (argv: ReadonlyArray<string>): WatchSessionOptions["invocation"] => {
  const [executable, script, ...flags] = argv;
  return {
    executable: executable ?? process.execPath,
    script: script ?? "",
    args: Arr.filter(flags, (token) => token !== "--watch"),
  };
};

/**
 * The flag arguments for one child CLI run. A narrowed plan replaces the
 * `--features` values with the changed feature files; every other flag
 * forwards unchanged, so tag/title filters, reporters, and timeouts behave
 * identically to a normal run.
 *
 * @internal
 */
export const childRunArgv = (
  session: WatchSessionOptions,
  plan: Watch.WatchRunPlan,
): ReadonlyArray<string> => replaceFlagValues("--features", plan.features)(session.invocation.args);

/**
 * Replace the values of an existing repeated flag in an argv, or append the
 * flag when it is absent.
 *
 * @internal
 */
export const replaceFlagValues =
  (
    flag: string,
    values: ReadonlyArray<string>,
  ): ((argv: ReadonlyArray<string>) => ReadonlyArray<string>) =>
  (argv) => {
    const flagIndex = Fn.pipe(
      argv,
      Arr.findFirstIndex((token) => token === flag),
    );
    if (Option.isNone(flagIndex)) {
      return Arr.appendAll(argv, Watch.repeatFlag(flag, values));
    }
    const nextFlagIndex = Fn.pipe(
      argv,
      Arr.findFirstIndex(
        (token, index) => index > flagIndex.value && Fn.pipe(token, Str.startsWith("--")),
      ),
    );
    const before = Fn.pipe(argv, Arr.splitAt(flagIndex.value + 1))[0];
    const after = Option.match(nextFlagIndex, {
      onNone: (): ReadonlyArray<string> => [],
      onSome: (index) => Fn.pipe(argv, Arr.splitAt(index))[1],
    });
    return Arr.appendAll(Arr.appendAll(before, values), after);
  };

/**
 * Execute one child CLI run with inherited stdio, so reports stream to the
 * user's terminal as they are produced.
 *
 * @internal
 */
export const runChildOnce: (
  session: WatchSessionOptions,
  plan: Watch.WatchRunPlan,
) => Effect.Effect<RunOutcome, WatchSessionError, ChildProcessSpawner.ChildProcessSpawner> = (
  session,
  plan,
) =>
  Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const { executable, script } = session.invocation;
    const exitCode = yield* spawner.exitCode(
      ChildProcess.make(executable, [script, ...childRunArgv(session, plan)], {
        stdout: "inherit",
        stderr: "inherit",
      }),
    );
    return { failed: exitCode !== 0 };
  });

/**
 * Run the watch loop: execute the initial full run, then consume trigger
 * batches from the queue, executing one child run per batch until the queue
 * ends.
 *
 * @internal
 */
export const watchLoop: (
  session: WatchSessionOptions,
  triggers: Queue.Dequeue<ReadonlyArray<Watch.WatchTrigger>>,
) => Effect.Effect<void, WatchSessionError, ChildProcessSpawner.ChildProcessSpawner> = (
  session,
  triggers,
) =>
  Effect.gen(function* () {
    yield* runChildOnce(session, { full: true, features: session.cli.features });
    yield* Watch.rerunLoop(triggers, Schedule.spaced("100 millis"), (batch) =>
      Effect.gen(function* () {
        const plan = Watch.planForTriggers(batch, session.cli.features);
        yield* runChildOnce(session, plan);
      }),
    );
  });

/**
 * Run a full watch session: initial run, then rerun on file changes until the
 * process is interrupted. The watcher pump and the rerun loop run as sibling
 * fibers joined at the end, so Ctrl+C interrupts both through the session
 * scope.
 *
 * @internal
 */
export const watchSession: (
  session: WatchSessionOptions,
) => Effect.Effect<
  void,
  WatchSessionError,
  ChildProcessSpawner.ChildProcessSpawner | FileSystem.FileSystem | Path.Path
> = (session) =>
  Effect.gen(function* () {
    const paths = Watch.watchPaths({ features: session.cli.features, steps: session.cli.steps });
    const events = yield* Watch.mergedWatchEvents(paths);
    const triggers = yield* Watch.makeTriggerQueue;
    // The pump and the rerun loop are sibling fibers on the session scope:
    // all([pump.await, loop]) runs them concurrently and joins when the loop
    // ends, so a watcher teardown cannot outlive a rerun in flight.
    yield* Effect.all(
      [
        Effect.flatMap(Effect.forkScoped(Watch.pumpTriggerQueue(events, triggers)), Fiber.join),
        watchLoop(session, triggers),
      ],
      { concurrency: 2 },
    ).pipe(Effect.asVoid);
  }).pipe(Effect.scoped);
