import { NodeServices } from "@effect/platform-node";
import { assert, describe, it } from "@effect/vitest";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schedule from "effect/Schedule";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import { fileURLToPath } from "node:url";
import {
  classifyTrigger,
  globMatcher,
  makeTriggerQueue,
  planForTriggers,
  repeatFlag,
  stepTimeoutArg,
  takePendingTriggers,
  type WatchTrigger,
  watchPaths,
  watchRoot,
} from "../src/internal/cli/watch.ts";
import {
  childInvocation,
  childRunArgv,
  replaceFlagValues,
  type WatchSessionOptions,
  watchLoop,
} from "../src/internal/cli/watchSession.ts";

const update = (path: string): WatchTrigger => ({ _tag: "Update", path });
const remove = (path: string): WatchTrigger => ({ _tag: "Remove", path });

describe("watch trigger classification", () => {
  it("classifies .feature files as feature triggers", () => {
    assert.strictEqual(classifyTrigger(update("a/b/login.feature")), "features");
    assert.strictEqual(classifyTrigger({ _tag: "Create", path: "new.feature" }), "features");
    assert.strictEqual(classifyTrigger(remove("old.feature")), "features");
  });

  it("classifies everything else as a step trigger", () => {
    assert.strictEqual(classifyTrigger(update("steps/login.step.ts")), "steps");
    assert.strictEqual(classifyTrigger(update("README.md")), "steps");
  });
});

describe("watch run plans", () => {
  it("narrows a feature-only batch to the changed files", () => {
    const plan = planForTriggers(
      [update("a/one.feature"), update("b/two.feature")],
      ["features/*.feature"],
    );
    assert.deepStrictEqual(plan, { full: false, features: ["a/one.feature", "b/two.feature"] });
  });

  it("deduplicates repeated feature triggers", () => {
    const plan = planForTriggers([update("a/one.feature"), update("a/one.feature")], []);
    assert.deepStrictEqual(plan, { full: false, features: ["a/one.feature"] });
  });

  it("widens to a full rerun when a step file changes", () => {
    const plan = planForTriggers(
      [update("a/one.feature"), update("steps/login.step.ts")],
      ["features/*.feature"],
    );
    assert.deepStrictEqual(plan, { full: true, features: ["features/*.feature"] });
  });

  it("widens to a full rerun when a feature file is removed", () => {
    const plan = planForTriggers([remove("a/one.feature")], ["features/*.feature"]);
    assert.deepStrictEqual(plan, { full: true, features: ["features/*.feature"] });
  });

  it("treats a feature file creation as a narrowed rerun", () => {
    const plan = planForTriggers(
      [{ _tag: "Create", path: "a/new.feature" }],
      ["features/*.feature"],
    );
    assert.deepStrictEqual(plan, { full: false, features: ["a/new.feature"] });
  });

  it("returns an empty plan for an empty batch", () => {
    assert.deepStrictEqual(planForTriggers([], ["features/*.feature"]), {
      full: false,
      features: [],
    });
  });
});

describe("watch paths", () => {
  it("deduplicates feature and step patterns", () => {
    assert.deepStrictEqual(
      watchPaths({ features: ["a/*.feature"], steps: ["a/*.feature", "steps/*.ts"] }),
      ["a/*.feature", "steps/*.ts"],
    );
  });
});

describe("watch roots", () => {
  it.effect("uses the literal directory prefix of a glob", () =>
    Effect.gen(function* () {
      const path = yield* Path.Path;
      const root = yield* watchRoot("features/**/*.feature");
      assert.strictEqual(root, path.resolve("features"));
    }).pipe(Effect.provide(Path.layer)),
  );

  it.effect("uses the parent directory of a literal file path", () =>
    Effect.gen(function* () {
      const path = yield* Path.Path;
      const root = yield* watchRoot("features/login.feature");
      assert.strictEqual(root, path.resolve("features"));
    }).pipe(Effect.provide(Path.layer)),
  );

  it.effect("resolves a bare glob to the working directory", () =>
    Effect.gen(function* () {
      const path = yield* Path.Path;
      const root = yield* watchRoot("*.feature");
      assert.strictEqual(root, path.resolve("."));
    }).pipe(Effect.provide(Path.layer)),
  );
});

describe("glob matcher", () => {
  it("matches paths covered by the patterns", () => {
    const matches = globMatcher(["features/**/*.feature", "steps/*.ts"]);
    assert.strictEqual(matches("features/login.feature"), true);
    assert.strictEqual(matches("features/a/b/login.feature"), true);
    assert.strictEqual(matches("steps/login.step.ts"), true);
    assert.strictEqual(matches("src/login.ts"), false);
    assert.strictEqual(matches("features/login.txt"), false);
    assert.strictEqual(matches("node_modules/pkg/index.js"), false);
  });

  it("ignores absolute-path differences outside the pattern", () => {
    const matches = globMatcher(["features/*.feature"]);
    assert.strictEqual(matches("node_modules/effect/index.js"), false);
  });
});

describe("child argv assembly", () => {
  const session: Parameters<typeof childRunArgv>[0] = {
    cli: {
      features: ["features/*.feature"],
      steps: ["steps/*.ts"],
      reporters: ["text"],
      outputFiles: {},
      verbose: false,
      strict: false,
      filters: { tags: ["@smoke"], titles: ["login"], failFast: false },
      parallel: 1,
    },
    invocation: {
      executable: "node",
      script: "bin.ts",
      args: [
        "--features",
        "features/*.feature",
        "--steps",
        "steps/*.ts",
        "--tags",
        "@smoke",
        "--title",
        "login",
      ],
    },
  };

  it("replaces --features values for a narrowed plan", () => {
    const argv = childRunArgv(session, { full: false, features: ["features/login.feature"] });
    assert.deepStrictEqual(argv, [
      "--features",
      "features/login.feature",
      "--steps",
      "steps/*.ts",
      "--tags",
      "@smoke",
      "--title",
      "login",
    ]);
  });

  it("uses the original patterns for a full plan", () => {
    const argv = childRunArgv(session, { full: true, features: ["features/*.feature"] });
    assert.deepStrictEqual(argv, session.invocation.args);
  });

  it("strips --watch from the child invocation", () => {
    assert.deepStrictEqual(childInvocation(["node", "bin.ts", "--watch", "--tags", "@smoke"]), {
      executable: "node",
      script: "bin.ts",
      args: ["--tags", "@smoke"],
    });
  });

  it("falls back to the running interpreter when argv is empty", () => {
    const invocation = childInvocation([]);
    assert.strictEqual(invocation.executable, process.execPath);
    assert.strictEqual(invocation.script, "");
  });

  it("replaces flag values in the middle of argv", () => {
    assert.deepStrictEqual(
      replaceFlagValues("--tags", ["@nightly"])([
        "--features",
        "a",
        "--tags",
        "@smoke",
        "--parallel",
        "2",
      ]),
      ["--features", "a", "--tags", "@nightly", "--parallel", "2"],
    );
  });

  it("appends the flag when absent", () => {
    assert.deepStrictEqual(replaceFlagValues("--tags", ["@nightly"])(["--features", "a"]), [
      "--features",
      "a",
      "--tags",
      "@nightly",
    ]);
  });
});

describe("repeatFlag", () => {
  it("emits one flag per value", () => {
    assert.deepStrictEqual(repeatFlag("--tags", ["@a", "@b"]), ["--tags", "@a", "--tags", "@b"]);
    assert.deepStrictEqual(repeatFlag("--tags", []), []);
  });
});

describe("stepTimeoutArg", () => {
  it("renders finite durations as millis", () => {
    assert.strictEqual(stepTimeoutArg(Duration.seconds(5)), "5000 millis");
  });

  it("renders infinite durations literally", () => {
    assert.strictEqual(stepTimeoutArg(Duration.infinity), "Infinity");
  });
});

describe("trigger queue batching", () => {
  it.effect("takePendingTriggers merges batches that arrived while waiting", () =>
    Effect.gen(function* () {
      const queue = yield* makeTriggerQueue;
      yield* Queue.offer(queue, [update("a.feature")]);
      yield* Queue.offer(queue, [update("b.feature"), update("c.feature")]);
      const batch = yield* takePendingTriggers(queue);
      assert.deepStrictEqual(batch, [
        update("a.feature"),
        update("b.feature"),
        update("c.feature"),
      ]);
    }),
  );
});

describe("watchLoop", () => {
  // The stub handle resolves its exit code immediately; the loop never needs
  // the real process streams.
  const stubHandle = ChildProcessSpawner.makeHandle({
    pid: ChildProcessSpawner.ProcessId(0),
    exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(0)),
    isRunning: Effect.succeed(false),
    kill: () => Effect.void,
    stdin: Sink.drain,
    stdout: Stream.empty,
    stderr: Stream.empty,
    all: Stream.empty,
    getInputFd: () => Sink.drain,
    getOutputFd: () => Stream.empty,
    unref: Effect.void,
    reref: Effect.void,
  });

  const recordingSpawner = (commands: Ref.Ref<Array<ReadonlyArray<string>>>) =>
    ChildProcessSpawner.make((command) => {
      if (command._tag !== "StandardCommand") {
        return Effect.die("unexpected command form");
      }
      return Effect.as(
        Ref.update(commands, (recorded) => [...recorded, command.args]),
        stubHandle,
      );
    });

  it.live(
    "runs the initial full run, then one run per poll batch",
    () =>
      Effect.gen(function* () {
        const commands = yield* Ref.make<Array<ReadonlyArray<string>>>([]);
        const session: WatchSessionOptions = {
          cli: {
            features: ["features/*.feature"],
            steps: [],
            reporters: ["text"],
            outputFiles: {},
            verbose: false,
            strict: false,
            filters: { tags: [], titles: [], failFast: false },
            parallel: 1,
          },
          invocation: {
            executable: "node",
            script: "bin.ts",
            args: ["--features", "features/*.feature"],
          },
        };
        const triggers = yield* makeTriggerQueue;
        yield* Queue.offer(triggers, [update("a/one.feature")]);
        yield* Queue.offer(triggers, [update("steps/x.step.ts")]);
        yield* Effect.forkScoped(
          watchLoop(session, triggers).pipe(
            Effect.provide(
              Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, recordingSpawner(commands)),
            ),
          ),
        );
        // Wait for the initial run plus at least one poll tick, then stop.
        yield* Effect.sleep("600 millis");
        const recorded = yield* Ref.get(commands);
        // The interpreter and script lead every invocation; the poll drains both
        // batches at once, so the narrowed feature run and the widened step run
        // collapse into a single full rerun after the initial run.
        assert.deepStrictEqual(recorded, [
          ["bin.ts", "--features", "features/*.feature"],
          ["bin.ts", "--features", "features/*.feature"],
        ]);
      }),
    15000,
  );
});

describe("watch session end to end", () => {
  it.live(
    "reruns through the real CLI when a feature file changes",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
          const testDir = path.dirname(fileURLToPath(import.meta.url));
          const directory = yield* fs.makeTempDirectoryScoped({
            directory: testDir,
            prefix: ".effect-bdd-watch-",
          });
          const featurePath = path.join(directory, "counter.feature");
          const stepsPath = path.join(directory, "steps.mjs");
          const textReport = path.join(directory, "watch-report.txt");
          const writeFeature = (name: string) =>
            fs.writeFileString(
              featurePath,
              `Feature: Counter\n\n  Scenario: ${name}\n    When increment\n    Then the counter is 1\n`,
            );
          const writeSteps = (name: string) => fs.writeFileString(stepsPath, watchSteps(name));

          yield* Effect.all([writeFeature("Increment"), writeSteps("Increment")]);

          const binPath = path.join(testDir, "..", "src", "bin.ts");
          const handle = yield* spawner.spawn(
            ChildProcess.make(process.execPath, [
              binPath,
              "--features",
              path.join(directory, "*.feature"),
              "--steps",
              stepsPath,
              "--reporter",
              "text",
              "--output-file.text",
              textReport,
              "--verbose",
              "--watch",
            ]),
          );

          // Initial run writes the report; wait for it before editing. The
          // report lists scenarios as "Feature / Scenario", which only appears
          // with --verbose scenario lines.
          yield* waitForReport(textReport, "Counter / Increment");

          // Rename the scenario in the feature and its matching step chain; the
          // watch should rerun (the steps change widens it to a full run) and
          // the report should pick up the new title.
          yield* Effect.sleep("400 millis");
          yield* Effect.all([writeFeature("Renamed"), writeSteps("Renamed")]);
          yield* waitForReport(textReport, "Counter / Renamed");

          yield* handle.kill();
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
    30000,
  );
});

const waitForReport = (
  file: string,
  contains: string,
): Effect.Effect<void, Error, FileSystem.FileSystem> =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    return yield* fs.readFileString(file);
  }).pipe(
    Effect.filterOrFail(
      (text) => text.includes(contains),
      () => new Error(`report never contained "${contains}"`),
    ),
    // 75 attempts spaced 200ms apart bounds the wait at ~15 seconds.
    Effect.retry(Schedule.addDelay(Schedule.recurs(75), () => Effect.succeed("200 millis"))),
    Effect.asVoid,
  );

const watchSteps = (scenarioName: string) => `
import { Bdd } from "effect-bdd";
import { Effect, Schema } from "effect";

const expected = Bdd.capture("expected", Schema.NumberFromString);
const whenIncrement = Bdd.when\`increment\`((state) => Effect.succeed((state ?? 0) + 1));
const thenCounterIs = Bdd.then\`the counter is \${expected}\`(({ expected }, state) =>
  (state ?? 0) === expected
    ? Effect.succeed(state ?? 0)
    : Effect.fail(\`expected \${expected}, got \${state ?? 0}\`)
);

export const counter = Bdd.feature("Counter").pipe(
  Bdd.scenario("${scenarioName}").pipe(whenIncrement, thenCounterIs),
);
`;
