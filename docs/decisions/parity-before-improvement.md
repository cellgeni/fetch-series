---
title: Parity before improvement
---

# Parity before improvement

**Status:** in force for the `fetch10xmeta` replacement ·
**Evidence:** [the parity record](../fetch10xmeta-parity.md)

## The decision

`fetch links` reproduces `fetch10xmeta`'s nine-step priority chain
**deliberately**, including the places where this project's own
[file layer](../files.md) would choose differently.

A swap that changes behaviour is not a swap. The pipeline downstream —
`WGET10X` first — reads a five-column `links.tsv` with a `ORIFQ`/`ENAFQ`/`BAM`/
`SRA` vocabulary, and the moment that file changes shape or content, a
replacement stops being a replacement and becomes a rewrite with an unmeasured
blast radius.

**Result: 14 of 14 test cases, no differences** — 422 snapshot rows covering 400
distinct runs, plus one case (E-MTAB-8060, 15 rows) that has no snapshot and passes
on assertions alone ([the count](../fetch10xmeta-parity.md#results)).

## The contract is the incumbent's own test suite

Not a specification written for the occasion.
`modules/cellgeni/fetch10xmeta/tests/main.nf.test.snap` records, per case, the
`(run, species, type, sample)` tuple for every run, and
`scripts/parity_fetch10xmeta.py` reads that file directly rather than carrying a
copy — so the comparison stays true as the incumbent's tests change.

The snapshot **omits the URLs**, because SDL hands out signed links that rotate.
That omission is what makes it the right comparison: it asks which files were
*chosen*, not which mirror served them.

## Where the two would disagree, and why parity still holds

The incumbent decides `ENAFQ` by requiring both `_1.fastq.gz` and
`_2.fastq.gz`. That is a **name test standing in for a completeness test**: it
rejects a genuinely single-end library along with a truncated paired one.

This project reaches the same verdict from `library_layout`, which is the fact
rather than a proxy for it — and catches the
[24 genuinely single-end libraries](../pathologies/ena-paired-library-single-fastq.md)
in the same sample that the name test also rejects.

The two agree on every case the snapshots cover, so parity holds while the
better mechanism ships underneath. That is the only shape in which "improve it"
and "do not change it" are both satisfiable.

## What is deliberately not reproduced

**Steps 8 and 9** — the SRA table's archive URL, then a `srapath` shell-out.
Both ask the SRA toolkit for a signed, rotating link, which is a download-time
concern rather than a resolution result. The incumbent's own snapshots exclude
those URLs for the same reason.

**The intermediate files.** `*.list`, `*.txt` and `*.soft` were by-products of a
shell pipeline; nothing downstream consumes them. `relations.tsv` replaces them
with the part that makes a failure diagnosable afterwards — which route supplied
each field, and where two archives disagreed.

**`errorStrategy = { task.attempt == 5 ? 'ignore' : 'retry' }`.** Ignoring on
the fifth attempt drops the dataset silently: the pipeline continues, the sample
is simply absent, and nothing downstream reports it. That is how datasets go
missing without anyone noticing. The replacement uses `'retry'` and lets a
genuine failure fail.

**The stub's file names.** The incumbent's `stub:` block writes
`${prefix}.sample.list` and `${prefix}.accessions.tsv`, which its real script
never produces. A stub that disagrees with its process tests nothing.

## Three defects on *our* side had to be fixed to reach parity

Worth recording, because each was a case where this project resolved **fewer**
runs than the bash module it is replacing — and none was visible without the
comparison.

1. **ArrayExpress was not a working entry point.** ENA's `filereport` rejects an
   `E-MTAB` accession with a 400 naming the eight shapes it accepts. The 400 was
   caught, logged and swallowed, so every ArrayExpress accession resolved to a
   series with no runs.
2. **The GEO sample was lost where GEO omits the SRA relation.** GSE135325 and
   GSE137444 record a BioSample per sample and an SRA relation for none, so
   every row carried the INSDC sample instead — a different identity from the one
   a reprocessing pipeline is asked about. 36 of 36 and 50 of 50 recovered.
3. **The SDRF was read through a dict.** SDRF repeats `Comment[FASTQ_URI]` once
   per mate and `csv.DictReader` keeps only the last. E-MTAB-9221 yielded 20
   URIs for its 20 runs instead of 40 — half the data, no error. Caught only by
   checking against the 40 files the study registers.

## What would end this

The swap landing. Once `FETCH10XMETA` calls the container and the incumbent's
bash is deleted, the parity contract has served its purpose and the file layer's
own ranking becomes the one that ships. Until then, changing `links.tsv` moves
the target.

That is also why `CLAUDE.md` says not to "fix" the incumbent's bash from this
repository: its behaviour *is* the contract.
