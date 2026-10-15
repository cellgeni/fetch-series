/*
 * A drop-in replacement for cellgeni/fetch10xmeta, backed by the fetch-series
 * container.
 *
 * Emits links.tsv in the same schema the incumbent does -- run, species,
 * url(s), type, sample -- so WGET10X and everything downstream is untouched at
 * swap time. Parity is measured against the incumbent's own nf-test snapshots;
 * see docs/fetch10xmeta-parity.md.
 */

process FETCH10XMETA {
    tag "${meta.id}"

    input:
    tuple val(meta), val(sample_ids)

    output:
    tuple val(meta), path("links.tsv")          , emit: links
    tuple val(meta), path("relations.tsv")      , emit: relations
    tuple val(meta), path("screen.tsv")         , emit: screen, optional: true
    path "versions.yml"                         , emit: versions

    script:
    def samples = sample_ids ? "--samples '${sample_ids}'" : ''
    """
    # The download links themselves, in the incumbent's schema.
    fetch links ${meta.id} ${samples} --out links.tsv

    # The cross-archive relation table. Not consumed by the pipeline today, but
    # it is what makes a failure diagnosable afterwards: which route supplied
    # each field, and where two archives disagreed.
    fetch relations ${meta.id} --out relations.tsv

    # Assay screening is advisory here and must not fail the process: it exits
    # non-zero when nothing looks like 10x, which is a finding to record rather
    # than a reason to stop resolving. RENAME10XRUN remains authoritative.
    fetch screen ${meta.id} > screen.tsv || true

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        fetch-series: \$(fetch --version)
    END_VERSIONS
    """

    stub:
    """
    # The same file names the real script produces. The incumbent's stub wrote
    # prefixed names its own script never emits, so any test exercising the
    # stub path asserted against files that could not exist in a real run.
    touch links.tsv relations.tsv screen.tsv

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        fetch-series: stub
    END_VERSIONS
    """
}
