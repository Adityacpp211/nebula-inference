#!/usr/bin/env python3
"""Build a tiny, genuinely trained GGUF model for the integration tests.

**Why this exists.** The Phase 3 exit criterion is a real model streaming real
tokens through the worker. That needs a GGUF file, and a CI runner (or a sandbox)
cannot always reach a model host — ours cannot reach Hugging Face at all. Downloading
a 400 MB artifact on every CI run would also be a poor trade for a test whose job is
to prove the *pipeline*, not the model.

So the fixture is produced rather than fetched: this script trains a small
LLaMA-architecture model on a short corpus and writes it as a GGUF that upstream
``llama-server`` loads and runs unmodified. Nothing about the serving path is faked —
real GGUF parsing, real tokenizer, real attention, real KV cache, real sampling. The
only thing that is small is the model.

**What it is honest about.** The model has roughly 0.5 M parameters and is trained to
memorise one paragraph. It is not a general language model and will produce nonsense
outside its corpus. It exists to prove that tokens flow, that time-to-first-token is
measurable, that cancellation frees a slot, and that a deadline aborts a generation.
For anything about *output quality*, point the same adapter at a real model: that is
the entire point of the adapter being an adapter.

**Why the weights are written directly rather than converted.** Hugging Face stores
LLaMA Q/K projections in an interleaved layout that GGUF does not use, which is why
conversion scripts permute them. This script sidesteps the permutation by
implementing rotary embedding the way ggml does — rotating *adjacent* pairs
(``GGML_ROPE_TYPE_NORM``) rather than split halves — so the trained tensors are
already in GGUF's layout. Getting this wrong produces a model that loads cleanly and
emits confident garbage, which is the worst possible failure mode for a test fixture,
so the two-pass tokenisation below exists to catch it.

Usage::

    python tools/make_tiny_model.py --llama-tokenize <path> --out model.gguf
"""

from __future__ import annotations

import argparse
import ast
import json
import math
import subprocess
import sys
import time
from pathlib import Path

import gguf
import torch
from torch import nn

# The corpus. Chosen so a memorising model produces output that is obviously from
# *this* fixture and not from a real model someone forgot to swap out.
CORPUS = (
    "nebula streams tokens from a supervised llama server. "
    "the worker measures time to first token. "
    "cancellation frees the slot and stops the engine. "
    "the deadline aborts the request. "
    "a deployment is desired state until a controller reconciles it."
)

# Architecture. Small enough to train in seconds on two CPU cores, large enough to be
# a real transformer: attention, RMSNorm, SwiGLU, rotary embeddings.
N_LAYER = 2
N_EMBD = 128
N_HEAD = 4
N_FF = 256
# Declared context. Deliberately larger than the 243-token corpus this model is
# trained on: llama.cpp caps a generation at the model's declared context, and the
# integration tests need to request a few thousand tokens so that cancellation and
# deadline abort are exercised *mid-generation* rather than against a request the
# engine has already finished. Only the first couple of hundred tokens are coherent;
# past that the model is extrapolating rotary positions it never saw and emits
# nonsense. The tests assert on content only within the coherent span, and
# tests/test_real_model.py says so where it asserts it.
N_CTX = 4096
ROPE_BASE = 10000.0
RMS_EPS = 1e-5

HEAD_DIM = N_EMBD // N_HEAD


# ---------------------------------------------------------------------------
# tokenizer vocabulary
# ---------------------------------------------------------------------------


def build_vocab(corpus: str) -> tuple[list[str], list[float], list[int]]:
    """A minimal SentencePiece-style vocabulary.

    Three specials, then one piece per character in the corpus, then all 256 byte
    tokens. The byte tokens are what make the tokenizer total: llama.cpp falls back to
    them for any character the vocabulary does not contain, so an unexpected input
    cannot produce an unknown token.

    ``U+2581`` (``▁``) is included because SentencePiece represents a space that way,
    and llama.cpp's SPM path escapes spaces into it before looking anything up.
    """
    tokens = ["<unk>", "<s>", "</s>"]
    types = [
        int(gguf.TokenType.UNKNOWN),
        int(gguf.TokenType.CONTROL),
        int(gguf.TokenType.CONTROL),
    ]
    scores = [0.0, 0.0, 0.0]

    pieces = sorted({ch for ch in corpus if not ch.isspace()} | {"▁", "\n"})
    for piece in pieces:
        tokens.append(piece)
        types.append(int(gguf.TokenType.NORMAL))
        scores.append(0.0)

    for byte in range(256):
        tokens.append(f"<0x{byte:02X}>")
        types.append(int(gguf.TokenType.BYTE))
        scores.append(0.0)

    return tokens, scores, types


# ---------------------------------------------------------------------------
# the model
# ---------------------------------------------------------------------------


def rope(x: torch.Tensor, pos: torch.Tensor) -> torch.Tensor:
    """Rotary embedding over adjacent pairs, matching ``GGML_ROPE_TYPE_NORM``.

    ggml's non-NeoX rope walks the head dimension two elements at a time and rotates
    ``(x[i], x[i+1])``. Implementing it the same way here is what lets the trained
    tensors be written to GGUF without the Q/K permutation a Hugging Face conversion
    needs.
    """
    *lead, dim = x.shape
    half = dim // 2
    idx = torch.arange(half, dtype=torch.float32, device=x.device)
    freqs = ROPE_BASE ** (-2.0 * idx / dim)
    angle = pos.to(torch.float32)[:, None] * freqs[None, :]
    cos, sin = angle.cos(), angle.sin()
    while cos.dim() < len(lead) + 1:
        cos = cos.unsqueeze(0)
        sin = sin.unsqueeze(0)

    paired = x.reshape(*lead, half, 2)
    x0, x1 = paired[..., 0], paired[..., 1]
    return torch.stack([x0 * cos - x1 * sin, x0 * sin + x1 * cos], dim=-1).reshape(*lead, dim)


class RMSNorm(nn.Module):
    def __init__(self, dim: int) -> None:
        super().__init__()
        self.weight = nn.Parameter(torch.ones(dim))

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        norm = x * torch.rsqrt(x.pow(2).mean(-1, keepdim=True) + RMS_EPS)
        return norm * self.weight


class Block(nn.Module):
    def __init__(self) -> None:
        super().__init__()
        self.attn_norm = RMSNorm(N_EMBD)
        self.wq = nn.Linear(N_EMBD, N_EMBD, bias=False)
        self.wk = nn.Linear(N_EMBD, N_EMBD, bias=False)
        self.wv = nn.Linear(N_EMBD, N_EMBD, bias=False)
        self.wo = nn.Linear(N_EMBD, N_EMBD, bias=False)
        self.ffn_norm = RMSNorm(N_EMBD)
        self.gate = nn.Linear(N_EMBD, N_FF, bias=False)
        self.up = nn.Linear(N_EMBD, N_FF, bias=False)
        self.down = nn.Linear(N_FF, N_EMBD, bias=False)

    def forward(self, x: torch.Tensor, pos: torch.Tensor, mask: torch.Tensor) -> torch.Tensor:
        b, t, _ = x.shape
        h = self.attn_norm(x)
        q = self.wq(h).view(b, t, N_HEAD, HEAD_DIM).transpose(1, 2)
        k = self.wk(h).view(b, t, N_HEAD, HEAD_DIM).transpose(1, 2)
        v = self.wv(h).view(b, t, N_HEAD, HEAD_DIM).transpose(1, 2)

        q = rope(q, pos)
        k = rope(k, pos)

        scores = (q @ k.transpose(-2, -1)) / math.sqrt(HEAD_DIM)
        scores = scores + mask[:t, :t]
        attn = scores.softmax(dim=-1) @ v
        x = x + self.wo(attn.transpose(1, 2).reshape(b, t, N_EMBD))

        h = self.ffn_norm(x)
        return x + self.down(nn.functional.silu(self.gate(h)) * self.up(h))


class TinyLlama(nn.Module):
    def __init__(self, n_vocab: int) -> None:
        super().__init__()
        self.embed = nn.Embedding(n_vocab, N_EMBD)
        self.blocks = nn.ModuleList(Block() for _ in range(N_LAYER))
        self.norm = RMSNorm(N_EMBD)
        self.head = nn.Linear(N_EMBD, n_vocab, bias=False)

    def forward(self, ids: torch.Tensor) -> torch.Tensor:
        _batch, t = ids.shape
        x = self.embed(ids)
        pos = torch.arange(t, device=ids.device)
        mask = torch.full((t, t), float("-inf"), device=ids.device).triu(1)
        for block in self.blocks:
            x = block(x, pos, mask)
        return self.head(self.norm(x))


# ---------------------------------------------------------------------------
# GGUF writing
# ---------------------------------------------------------------------------


def write_gguf(
    path: Path,
    model: TinyLlama | None,
    vocab: tuple[list[str], list[float], list[int]],
    n_vocab: int,
) -> None:
    """Write a llama-architecture GGUF.

    Called twice: once with ``model=None`` to produce a vocabulary-only file used to
    ask llama.cpp itself how the corpus tokenises, and once with the trained weights.
    Reusing one writer for both is what guarantees the tokenizer the model was trained
    against is byte-for-byte the tokenizer it will be served with.
    """
    tokens, scores, types = vocab
    writer = gguf.GGUFWriter(str(path), "llama")

    writer.add_name("nebula-tiny-fixture")
    writer.add_description(
        "Tiny trained LLaMA fixture for NEBULA's Phase 3 integration tests. "
        "Not a general language model: ~0.5M parameters trained to memorise one paragraph."
    )
    writer.add_context_length(N_CTX)
    writer.add_embedding_length(N_EMBD)
    writer.add_block_count(N_LAYER)
    writer.add_feed_forward_length(N_FF)
    writer.add_head_count(N_HEAD)
    writer.add_head_count_kv(N_HEAD)
    writer.add_layer_norm_rms_eps(RMS_EPS)
    writer.add_rope_dimension_count(HEAD_DIM)
    writer.add_rope_freq_base(ROPE_BASE)
    writer.add_vocab_size(n_vocab)
    writer.add_file_type(gguf.LlamaFileType.ALL_F32)

    writer.add_tokenizer_model("llama")
    writer.add_tokenizer_pre("default")
    writer.add_token_list(tokens)
    writer.add_token_scores(scores)
    writer.add_token_types(types)
    writer.add_bos_token_id(1)
    writer.add_eos_token_id(2)
    writer.add_unk_token_id(0)
    writer.add_add_bos_token(True)
    writer.add_add_eos_token(False)

    def put(name: str, tensor: torch.Tensor) -> None:
        writer.add_tensor(name, tensor.detach().to(torch.float32).numpy())

    if model is None:
        # Vocabulary-only pass: llama.cpp still needs tensors present to load the
        # file, so zeros stand in. They are never served — this file exists for one
        # llama-tokenize call and is then overwritten.
        put("token_embd.weight", torch.zeros(n_vocab, N_EMBD))
        for i in range(N_LAYER):
            put(f"blk.{i}.attn_norm.weight", torch.ones(N_EMBD))
            for proj in ("attn_q", "attn_k", "attn_v", "attn_output"):
                put(f"blk.{i}.{proj}.weight", torch.zeros(N_EMBD, N_EMBD))
            put(f"blk.{i}.ffn_norm.weight", torch.ones(N_EMBD))
            put(f"blk.{i}.ffn_gate.weight", torch.zeros(N_FF, N_EMBD))
            put(f"blk.{i}.ffn_up.weight", torch.zeros(N_FF, N_EMBD))
            put(f"blk.{i}.ffn_down.weight", torch.zeros(N_EMBD, N_FF))
        put("output_norm.weight", torch.ones(N_EMBD))
        put("output.weight", torch.zeros(n_vocab, N_EMBD))
    else:
        # torch's Linear stores (out_features, in_features), which is exactly the
        # row-major layout GGUF expects for these tensors, so no transposes here.
        put("token_embd.weight", model.embed.weight)
        for i, block in enumerate(model.blocks):
            put(f"blk.{i}.attn_norm.weight", block.attn_norm.weight)
            put(f"blk.{i}.attn_q.weight", block.wq.weight)
            put(f"blk.{i}.attn_k.weight", block.wk.weight)
            put(f"blk.{i}.attn_v.weight", block.wv.weight)
            put(f"blk.{i}.attn_output.weight", block.wo.weight)
            put(f"blk.{i}.ffn_norm.weight", block.ffn_norm.weight)
            put(f"blk.{i}.ffn_gate.weight", block.gate.weight)
            put(f"blk.{i}.ffn_up.weight", block.up.weight)
            put(f"blk.{i}.ffn_down.weight", block.down.weight)
        put("output_norm.weight", model.norm.weight)
        put("output.weight", model.head.weight)

    writer.write_header_to_file()
    writer.write_kv_data_to_file()
    writer.write_tensors_to_file()
    writer.close()


def tokenize_with_llama(binary: Path, model_path: Path, text: str) -> list[int]:
    """Ask llama.cpp to tokenise the corpus.

    Deliberately not a reimplementation of SentencePiece. If the training data were
    tokenised by our own code and served through llama.cpp's tokenizer, any
    disagreement between the two would show up as a model that emits plausible-looking
    nonsense — the hardest class of bug to notice in a fixture.
    """
    out = subprocess.run(
        [str(binary), "--model", str(model_path), "--prompt", text, "--ids"],
        capture_output=True,
        text=True,
        check=True,
    )
    for line in out.stdout.splitlines():
        line = line.strip()
        if line.startswith("[") and line.endswith("]"):
            return [int(v) for v in ast.literal_eval(line)]
    raise SystemExit(f"could not parse token ids from llama-tokenize:\n{out.stdout}\n{out.stderr}")


def train(ids: list[int], n_vocab: int, steps: int, seed: int) -> TinyLlama:
    torch.manual_seed(seed)
    model = TinyLlama(n_vocab)
    data = torch.tensor(ids, dtype=torch.long).unsqueeze(0)
    inputs, targets = data[:, :-1], data[:, 1:]

    opt = torch.optim.AdamW(model.parameters(), lr=3e-3, weight_decay=0.0)
    sched = torch.optim.lr_scheduler.CosineAnnealingLR(opt, T_max=steps)
    model.train()
    loss = torch.tensor(float("nan"))
    for step in range(steps):
        logits = model(inputs)
        loss = nn.functional.cross_entropy(logits.reshape(-1, logits.size(-1)), targets.reshape(-1))
        opt.zero_grad(set_to_none=True)
        loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
        opt.step()
        sched.step()
        if step % 50 == 0 or step == steps - 1:
            print(f"  step {step:4d}  loss {loss.item():.4f}", file=sys.stderr)

    model.eval()
    with torch.no_grad():
        greedy = model(inputs).argmax(-1)
        accuracy = (greedy == targets).float().mean().item()
    print(f"  next-token accuracy on the corpus: {accuracy:.3f}", file=sys.stderr)
    if accuracy < 0.95:
        raise SystemExit(
            f"training did not converge (accuracy {accuracy:.3f}); the fixture would "
            "produce nonsense and the integration test could not tell that from a "
            "broken serving path"
        )
    return model


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--llama-tokenize", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--steps", type=int, default=400)
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()

    vocab = build_vocab(CORPUS)
    n_vocab = len(vocab[0])
    args.out.parent.mkdir(parents=True, exist_ok=True)

    print(f"vocabulary: {n_vocab} tokens", file=sys.stderr)
    print("pass 1: writing a vocabulary-only GGUF", file=sys.stderr)
    write_gguf(args.out, None, vocab, n_vocab)

    print("pass 2: tokenising the corpus with llama.cpp itself", file=sys.stderr)
    ids = tokenize_with_llama(args.llama_tokenize, args.out, CORPUS)
    print(f"  {len(ids)} tokens", file=sys.stderr)

    print(f"pass 3: training for {args.steps} steps", file=sys.stderr)
    started = time.perf_counter()
    model = train(ids, n_vocab, args.steps, args.seed)
    print(f"  trained in {time.perf_counter() - started:.1f}s", file=sys.stderr)

    print("pass 4: writing the trained GGUF", file=sys.stderr)
    write_gguf(args.out, model, vocab, n_vocab)

    params = sum(p.numel() for p in model.parameters())
    meta = {
        "path": str(args.out),
        "size_bytes": args.out.stat().st_size,
        "parameters": params,
        "vocab_size": n_vocab,
        "corpus": CORPUS,
        "prompt_for_tests": "nebula streams",
    }
    print(json.dumps(meta, indent=2))


if __name__ == "__main__":
    main()
