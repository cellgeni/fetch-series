# BioSamples Series

NCBI BioProject records describe the umbrella study or research effort, while
BioSample records describe the individual biological materials used in that
study. A typical workflow starts from a BioProject accession, finds the linked
BioSamples, and then follows those samples to related sequencing runs,
assemblies, or other submitted data.

## Connections to Sequencing Databases

BioProject and BioSample accessions act as shared metadata anchors across many
sequence and expression repositories. The BioProject groups the overall study,
and each BioSample records the biological source material for one or more
experiments. Other databases then attach their own records to those same study
and sample identifiers.

In NCBI SRA, sequencing records usually sit below BioSample and BioProject
metadata. A BioProject can contain many BioSamples, each BioSample can have one
or more SRA experiments, and each experiment can have one or more sequencing
runs. This lets a user start with a study accession such as `PRJNA...`, find the
sample accessions such as `SAMN...`, and then locate the raw read accessions
such as `SRR...`, `SRX...`, or `SRS...`.

GEO is often used for expression studies, including RNA-seq and array-based
experiments. GEO records have their own accessions, such as series (`GSE...`),
samples (`GSM...`), and platforms (`GPL...`). For sequencing-based GEO studies,
the GEO series or samples may link out to SRA records, and those SRA records may
then link back to the relevant BioProject and BioSample records. In practice,
GEO often describes the experimental design and processed expression data, while
SRA stores the raw sequencing reads.

ENA is the European nucleotide archive and exchanges many sequence records with
NCBI through the International Nucleotide Sequence Database Collaboration. ENA
uses accessions such as `PRJEB...` for projects, `SAMEA...` for samples,
`ERX...` for experiments, and `ERR...` for runs. These often correspond to the
same conceptual levels as NCBI BioProject, BioSample, SRA experiment, and SRA
run records, even when the accession prefixes differ.

ArrayExpress, now closely connected with functional genomics data resources at
EMBL-EBI, is similar to GEO in that it describes expression experiments and
their processed metadata. ArrayExpress studies may point to ENA run or
experiment records for raw sequencing data, while array-based studies may remain
primarily within ArrayExpress. When the same study is represented in both NCBI
and EMBL-EBI systems, BioProject/BioSample-style identifiers help connect the
study-level and sample-level metadata to the raw and processed data records.

A common lookup chain is:

```text
BioProject -> BioSample -> SRA/ENA experiment -> SRA/ENA run
GEO series -> GEO sample -> SRA run -> BioSample/BioProject
ArrayExpress experiment -> ENA run/experiment -> sample/project metadata
```

## Fetching Sequencing Data from BioSample Records

## BioSample database from e-tools API's point of view

BioSample database is indexed in the e-tools API. This is the latest information about the BioSamples database as of 2026-04-21:

```json
{
  "header": {
    "type": "einfo",
    "version": "0.3"
  },
  "einforesult": {
    "dbinfo": [
      {
        "dbname": "biosample",
        "menuname": "BioSample",
        "description": "BioSample Database",
        "dbbuild": "Build260430-0301m.1",
        "count": "54057175",
        "lastupdate": "2026/04/30 07:11"
      }
    ]
  }
}
```

The fields that are indexed in the BioSample database are:


| # | name | fullname | description | termcount | isdate | isnumerical | singletoken | hierarchy | ishidden |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | ALL | All Fields | All terms from all searchable fields | 462127089 | N | N | N | N | N |
| 1 | UID | UID | Unique number assigned to publication | 0 | N | Y | Y | N | Y |
| 2 | FILT | Filter | Limits the records | 296 | N | N | Y | N | N |
| 3 | ACCN | Accession | Accession number of sequence | 100585623 | N | N | Y | N | N |
| 4 | TITL | Title | Words in definition line | 19553227 | N | N | N | N | N |
| 5 | PROP | Properties | Classification by source qualifiers and molecular types | 4224 | N | N | Y | N | N |
| 6 | WORD | Text Word | Free text associated with record | 313550255 | N | N | N | N | N |
| 7 | ORGN | Organism | Scientific and common names of organism, and a... | 1740903 | N | N | Y | Y | N |
| 8 | AUTH | Author | Author(s) of publication | 192502 | N | N | Y | N | N |
| 9 | PDAT | Publication Date | Date sequence added to GenBank | 9824 | Y | N | Y | N | N |
| 10 | MDAT | Modification Date | Date of last update | 5038 | Y | N | Y | N | N |
| 11 | ATNM | Attribute Name | Attribute Name | 84737 | N | N | Y | N | N |
| 12 | ATTR | Attribute | Attribute | 145899988 | N | N | Y | N | N |
| 13 | CEN | Submitter Organization | Submitter Organization(s) | 128391 | N | N | Y | N | N |


The links that are indexed in the BioSample database are:

| # | name | menu | description | dbto |
| --- | --- | --- | --- | --- |
| 0 | biosample_assembly | Assembly links | Assembly | assembly |
| 1 | biosample_biocollections | BioCollections | BioCollections | biocollections |
| 2 | biosample_bioproject | BioProject Links | BioProject links | bioproject |
| 3 | biosample_dbvar | dbVar Links | Links to dbVar | dbvar |
| 4 | biosample_gap | dbGaP Links | Links to dbGap Studies | gap |
| 5 | biosample_gds | GEO DataSets Links | GEO DataSets links | gds |
| 6 | biosample_nuccore | Nucleotide Links | Nucleotide links | nuccore |
| 7 | biosample_omim | OMIM links | OMIM links | omim |
| 8 | biosample_pubmed | PubMed Links | PubMed links | pubmed |
| 9 | biosample_snp | SNP Links | Related SNP record | snp |
| 10 | biosample_sra | SRA Links | Links to SRA experiments | sra |
| 11 | biosample_taxonomy | Taxonomy Links | Links to Taxonomy | taxonomy |

## Fetching directions from BioSamples series