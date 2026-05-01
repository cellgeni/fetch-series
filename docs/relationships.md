```mermaid
graph LR
    BP((BioProject)) --> SRE{SRA Experiments}
    BP               --> ERR{ENA Runs}
    BP               --> BS((BioSamples))
    BP               --> GSE((GEO Series))
```