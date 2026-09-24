# 28. The integration-test model is trained locally, not downloaded

Date: 2026-09-24

## Status

Accepted. Implemented in Phase 3 (`workers/inference/tools/make_tiny_model.py`,
`workers/inference/tests/test_real_model.py`).

## Context

Phase 3's exit criterion is deliberately worded to be unfakeable: *client → inference
worker → model → streamed response with a real model*. A conformance suite that both
adapters pass proves the abstraction is coherent; it does not prove that the llama.cpp
adapter can drive an actual GGUF. Only a real model does that, and only a test that
asserts on the *content* of the output can tell a real model from a stub wearing the
adapter's name.

That requires a GGUF file in CI, which turns out to be a harder requirement than it
looks.

**The obvious source is unavailable.** The development sandbox this phase was built in
cannot reach Hugging Face or any other model host — every attempt is refused by the
egress policy. A test that can only run on a developer's laptop is a test that stops
running.

**Downloading is a bad trade even where it works.** The reference development model,
Qwen2.5-0.5B-Instruct-Q4_K_M, is roughly 400 MB. Fetching it on every CI run spends
minutes and bandwidth to prove something about *our* streaming path, not about the
model. It also puts a third-party host in the critical path of the build: when it rate
limits, or renames a file, or is down, our CI goes red for a reason that has nothing
to do with the change under review.

**Committing it is worse.** `.gitignore` refuses `*.gguf` for good reason. A
multi-hundred-megabyte binary in git history is permanent, and Git LFS is another
piece of infrastructure to run in order to store a test fixture.

**And a stub would defeat the point.** A "model" that is really the mock behind a
different name would make the exit criterion self-certifying. The one thing this test
exists to catch is exactly that substitution.

## Decision

The integration fixture is **trained locally by a script in the repository** and
written directly as GGUF. `tools/make_tiny_model.py` produces
`nebula-tiny.gguf`: 2 layers, 128 embedding dimensions, 4 heads, a 285-token
vocabulary, 401,280 parameters, about 1.6 MB. It trains in seconds on two CPU cores
and is loaded and run by **upstream `llama-server`, unmodified**.

Four properties make this a fixture rather than a fake:

1. **Nothing about the serving path is simulated.** Real GGUF parsing, real tokenizer,
   real attention, real KV cache, real sampling, real SSE. The only small thing is the
   model.

2. **The tokenizer the model trains against is byte-identical to the one it is served
   with.** The script runs in two passes: it writes a vocabulary-only GGUF, tokenises
   the corpus by invoking the engine's own `llama-tokenize --ids`, trains on those
   ids, and only then rewrites the file with weights. Training against a
   reimplementation of the tokenizer would produce a model that loads cleanly and
   emits confident garbage — the worst possible failure mode for a fixture, because it
   looks like a passing test.

3. **Rotary embedding is implemented the way ggml does it** — rotating *adjacent*
   pairs (`GGML_ROPE_TYPE_NORM`) rather than split halves. This is why Hugging Face →
   GGUF conversion scripts permute Q and K projections; writing the weights in ggml's
   layout from the start sidesteps the permutation rather than reimplementing it.

4. **The script fails loudly** if next-token accuracy on the corpus is below 0.95. A
   fixture that trains badly must not silently become a fixture that tests nothing.

The corpus is one paragraph about this worker, chosen so that memorised output is
obviously from *this* fixture:

> nebula streams tokens from a supervised llama server. the worker measures time to
> first token. cancellation frees the slot and stops the engine. the deadline aborts
> the request. a deployment is desired state until a controller reconciles it.

`tests/test_real_model.py` asserts on a literal prefix of that text, and separately
asserts `runtime.name == "llamacpp"`. A silent fallback to the mock fails the suite
instead of passing it.

Two things this decision does **not** do. It does not replace a real model for
anything about output quality: for that, point the same adapter at
Qwen2.5-0.5B-Instruct-Q4_K_M, which is what the reference development model is for.
And it does not make the fixture a committed artifact — it is generated into a path
named by `NEBULA_TEST_MODEL_PATH`, and `*.gguf` stays in `.gitignore`.

## Consequences

**Good.**

- The real-model integration suite runs anywhere Python, torch and a `llama-server`
  binary exist, including in a sandbox with no model-host access. No external service
  is in the critical path of the build.
- The fixture is 1.6 MB, so generating it costs seconds rather than minutes, and
  keeping it out of git costs nothing.
- Asserting on known content makes the "is this actually a real model?" question
  answerable by the test rather than by trust. That is the assertion that would have
  caught the one failure mode a reviewer cannot see.
- The two-pass tokenisation is a reusable pattern: any future fixture for any engine
  can be built the same way, by asking that engine to tokenise.

**Costs, accepted.**

- **torch and `gguf` become development dependencies.** They are in the `dev` extra
  only, are never installed into either worker image, and `[tool.setuptools] packages`
  excludes `tools/` from the distribution, so a training framework cannot reach a
  runtime image by accident.
- **The declared context window is larger than the trained one, deliberately.** The
  GGUF declares `n_ctx = 4096` while the corpus is 243 tokens. `llama.cpp` caps a
  generation at the model's declared context, and the cancellation and deadline tests
  need to abort *mid-generation* rather than against a request the engine has already
  finished — with a 512-token declaration, a 350 ms deadline could never bite, because
  generation completed in ~130 ms. Past a couple of hundred tokens the model is
  extrapolating rotary positions it never saw and emits nonsense. The content
  assertions stay inside the coherent span and say so where they are written. This is
  the one place the fixture is configured for the test rather than for itself, and it
  is recorded here rather than left as a surprising constant.
- **The engine build is pinned** (`Dockerfile.worker`, `LLAMA_CPP_REF`). A fixture's
  output is a property of a specific engine build, and an engine tracking master would
  make an upstream sampling change look like a model regression.
- **The script is ~400 lines of code that is not the product.** It earns that by being
  the only thing standing between "we have a runtime abstraction" and "we have a
  runtime abstraction that demonstrably drives a real engine".

## Alternatives considered

**Download the reference model, cache it in CI.** Rejected for this phase, not
forever: it cannot run in the development sandbox at all, so the suite would be
unrunnable exactly where it was being written. A cached download is a reasonable
*addition* later for quality checks; it is not a substitute for a fixture that always
works.

**Commit a very small third-party GGUF.** Rejected: even the smallest published GGUF
is tens of megabytes, the licence would have to be tracked, and `*.gguf` is ignored
for reasons that do not stop applying because a file is small.

**Random weights in a valid GGUF.** Tempting, and it would prove the pipeline moves
bytes. Rejected because the output is indistinguishable from the output of a *broken*
pipeline: with random weights there is nothing to assert except "some tokens arrived",
which the mock already proves. Training to memorise a known paragraph is what turns
the test from a smoke test into an assertion.

**Skip the real-model test; trust the conformance suite.** Rejected. The conformance
suite is shared by both adapters, so it is satisfied by anything shaped like a
runtime. The substitution it cannot detect — an adapter that claims to be llamacpp and
is not — is the one this test is for.
