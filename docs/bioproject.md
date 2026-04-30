# BioProject Series

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

## Fetching Sequencing Data from BioProject and BioSample Records

## BioProject and BioSample databases from e-tools API's point of view

BioProject and BioSample databases are indexed in the e-tools API. This is the latest information about the BioProject database as of 2026-04-21:

```json
{
  "header": {
    "type": "einfo",
    "version": "0.3"
  },
  "einforesult": {
    "dbinfo": [
      {
        "dbname": "bioproject",
        "menuname": "BioProject",
        "description": "BioProject Database",
        "dbbuild": "Build260421-0620.1",
        "count": "1038660",
        "lastupdate": "2026/04/21 07:06",
      }
    ]
  }
}
```

We can also look at the fields that are indexed in the BioProject database:

| # | name | fullname | description | termcount | isdate | isnumerical | singletoken | hierarchy | ishidden |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | ALL | All Fields | All terms from all searchable fields | 22947927 | N | N | N | N | N |
| 1 | UID | UID | Unique number assigned to publication | 0 | N | Y | Y | N | Y |
| 2 | FILT | Filter | Limits the records | 113 | N | N | Y | N | N |
| 3 | ORGN | Organism | Organism | 1631760 | N | N | Y | Y | N |
| 4 | PRJA | Project Accession | Project Accession | 1318565 | N | N | Y | N | N |
| 5 | TYPE | Project Type | Project Type | 2 | N | N | Y | N | N |
| 6 | STPE | Project Subtype | Project Subtype | 7 | N | N | Y | N | N |
| 7 | DATE | Registration Date | Registration Date | 8509 | Y | N | Y | N | N |
| 8 | TITL | Title | Title | 2569751 | N | N | Y | N | N |
| 9 | CEN | Submitter Organization | Submitter Organization(s) | 177331 | N | N | Y | N | N |
| 10 | ACCN | Replicon accession | Space delimited GenBank or RefSeq Replicon Acc... | 473857 | N | N | Y | N | N |
| 11 | RTYP | Replicon type | Replicon Type | 17 | N | N | Y | N | N |
| 12 | RNME | Replicon name | Replicon Name | 129 | N | N | Y | N | Y |
| 13 | LTP | Locus Tag Prefix | Locus Tag Prefix | 3913133 | N | N | Y | N | N |
| 14 | WORD | Description | Organism/Project Description | 10280547 | N | N | Y | N | N |
| 15 | KWRD | Keyword | Keyword | 439 | N | N | Y | N | N |
| 16 | PROP | Properties | Project/Organism Properties | 50 | N | N | Y | N | N |
| 17 | DTPE | Project Data Type | Project Data Type | 37 | N | N | Y | N | N |
| 18 | GRNT | Grant ID | Grant ID | 64034 | N | N | Y | N | N |
| 19 | FUND | Funding Agency | Funding Agency | 15063 | N | N | Y | N | N |
| 20 | PMID | PMID | Pubmed ID | 211632 | N | Y | Y | N | N |
| 21 | DOID | DOI | DOI ID | 7190 | N | N | Y | N | Y |
| 22 | PID | ProjectID | Project ID | 1038713 | N | Y | Y | Y | N |
| 23 | RELV | Relevance | Relevance | 5061 | N | N | Y | N | N |
| 24 | ANME | Assembly name | Assembly Name | 3368401 | N | N | Y | N | N |
| 25 | BPRJ | BioProject ID | BioProject ID or accession | 2357225 | N | N | Y | N | Y |
| 26 | TPRJ | Top Bioproject | Top Bioproject ID | 64059 | N | Y | Y | N | Y |
| 27 | WGSA | WGS Accession | WGS Accessions | 2095332 | N | N | Y | N | N |
| 28 | AACC | Assembly Accession | Assembly Accession | 2543040 | N | N | Y | N | N |
| 29 | ATNM | Attribute Name | Attribute Name | 6 | N | N | Y | N | N |
| 30 | ATTR | Attribute | Attribute | 209727 | N | N | Y | N | N |


Moreover, we can look at the links that are indexed in the BioProject database:

| # | name | menu | description | dbto |
| --- | --- | --- | --- | --- |
| 0 | bioproject_assembly_all | Assembly Links | All related Assemblies | assembly |
| 1 | bioproject_bioproject | BioProject | Links from project to related projects | bioproject |
| 2 | bioproject_bioproject_d2u | Umbrella projects | All Umbrella projects | bioproject |
| 3 | bioproject_bioproject_u2d | Data projects | All Data projects | bioproject |
| 4 | bioproject_biosample_all | BioSample Links | All related BioSamples | biosample |
| 5 | bioproject_dbvar | dbVar | Link from BioProjects to dbVar | dbvar |
| 6 | bioproject_gap | dbGaP Links | dbGaP Links | gap |
| 7 | bioproject_gds | GEO DataSet Links | GEO DataSet links | gds |
| 8 | bioproject_genome | Genome Links | Related Genomes | genome |
| 9 | bioproject_nuccore | Nucleotide Links | Related Nucleotide entry | nuccore |
| 10 | bioproject_nuccore_genomic_dna | Genomic DNA | Link to genomic DNA records | nuccore |
| 11 | bioproject_nuccore_genomic_rna | Genomic RNA | Link to genomic RNA records | nuccore |
| 12 | bioproject_nuccore_map | Map Records | Link to map record(s) | nuccore |
| 13 | bioproject_nuccore_reference | Reference Genome Sequences Links | Reference Genome Sequences | nuccore |
| 14 | bioproject_nuccore_repr | Representative Genome Sequences Links | Representative Genome Sequences | nuccore |
| 15 | bioproject_nuccore_transcript | Transcript | Link to transcript records | nuccore |
| 16 | bioproject_nuccore_tsamaster | TSA master | Link to TSA master record(s) | nuccore |
| 17 | bioproject_nuccore_tsatranscript | TSA transcript | Link to TSA transcript record(s) | nuccore |
| 18 | bioproject_nuccore_wgsmaster | WGS master | Link to WGS master record(s) | nuccore |
| 19 | bioproject_nuccore_wgstranscript | WGS transcript | Link to WGS transcript record(s) | nuccore |
| 20 | bioproject_pmc | PMC Links | PMC links | pmc |
| 21 | bioproject_protein | Protein Links | Related Proteins | protein |
| 22 | bioproject_pubmed | PubMed Links | PubMed Links | pubmed |
| 23 | bioproject_snp | SNP Links | Related SNP record | snp |
| 24 | bioproject_sra_all | SRA Links | All related SRA Experiments | sra |
| 25 | bioproject_taxonomy | Taxonomy Links | Taxonomy Links | taxonomy |

---

In the same way, we can also look at the BioSample database:

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

## Fetching directions from BioProject series

We are particularly interested in fetching sequencing data from BioProject and BioSample records. So we will focus on the links to GEO, SRA, ENA and ArrayExpress databases.

From a BioProject accession we should be able to find:
- GEO datasets linked to the BioProject
- BioSamples linked to the BioProject
- SRA experiments linked to the BioProject
- ENA experiments linked to the BioProject

Let's try to fetch the linked GEO datasets and SRA experiments for a specific BioProject. Let's go with `PRJNA988806`, which is a lung metastasis study in mice. Let's start by searching for the BioProject accession in the BioProject database. `e-search` query `eutils_search(query="PRJNA988806[PRJA]", db=db)` will give us the following results:

```json
{
  "header": {
    "type": "esearch",
    "version": "0.3"
  },
  "esearchresult": {
    "count": "1",
    "retmax": "1",
    "retstart": "0",
    "querykey": "1",
    "webenv": "MCID_69f375ae0f91922ac904b7db",
    "idlist": [
      "988806"
    ],
    "translationset": [],
    "translationstack": [
      {
        "term": "PRJNA988806[PRJA]",
        "field": "PRJA",
        "count": "1",
        "explode": "N"
      },
      "GROUP"
    ],
    "querytranslation": "PRJNA988806[PRJA]"
  }
}
```

We can see that the search returned one result, which is the BioProject with accession. Let's take at look at the summary of this BioProject using `eutils_summary`:

```json
{
  "header": {
    "type": "esummary",
    "version": "0.3"
  },
  "result": {
    "uids": [
      "988806"
    ],
    "988806": {
      "uid": "988806",
      "taxid": 10090,
      "project_id": 988806,
      "project_acc": "PRJNA988806",
      "project_type": "Primary submission",
      "project_data_type": "Transcriptome or Gene expression",
      "sort_by_projecttype": 317146,
      "sort_by_datatype": 293436,
      "sort_by_organism": 418525,
      "project_subtype": "",
      "project_target_scope": "Multiisolate",
      "project_target_material": "Transcriptome",
      "project_target_capture": "Whole",
      "project_methodtype": "Sequencing",
      "project_method": "",
      "project_objectives_list": [
        {
          "project_objectivestype": "Expression",
          "project_objectives": ""
        }
      ],
      "registration_date": "2023/06/28 00:00",
      "project_name": "Aspartate signaling increases the aggressiveness of lung metastases by inducing eIF5A-mediated translation (scRNA-Seq)",
      "project_title": "Aspartate signaling increases the aggressiveness of lung metastases by inducing eIF5A-mediated translation (scRNA-Seq)",
      "project_description": "Lung metastases are detected in more than half of patients with metastatic tumors. However, it remains largely unknown why the lung environment is a permissive niche for metastases. Here, we discover that pulmonary aspartate triggers a cellular signaling cascade in disseminated cancer cells resulting in a translational program that boosts lung metastasis. Specifically, we observe that patients and mice with breast cancer have high concentrations of aspartate in their lung interstitial fluid. This extracellular aspartate activates the ionotropic N-methyl-D-aspartate (NMDA) receptor in cancer cells, which induces CREB-dependent mRNA expression of deoxyhypusine hydroxylase (DOHH). The latter is essential for hypusination, a posttranslational modification required for the activity of the non-classical translation initiation factor eIF5A. In turn, a translational program with TGF-\u03b2 signaling as a central hub promotes collagen remodeling in the disseminated breast cancer cells. We detect key aspects of this mechanism in lung metastases from patients with breast cancer. In summary, we discover that pulmonary aspartate increases with breast cancer and induces a signaling cascade promoting the growth of lung metastases. Overall design: scRNA-seq from lungs of female BALB/c mice treated with control medium (CM) or medium containing tumor-secreted factors (TSF) for 3 weeks (3 injections per week), and subsequently either euthanized (pre-metastatic niche, CM0/TSF0), or injected i.v. with 25,000 CD90.1-expressing 4T1 cancer cells and then euthanized 11 days (metastatic seeding, CM1/TSF1) or 16 days (metastatic colonization, CM2/TSF2) after cancer-cell injection.",
      "keyword": "",
      "relevance_agricultural": "",
      "relevance_medical": "",
      "relevance_industrial": "",
      "relevance_environmental": "",
      "relevance_evolution": "",
      "relevance_model": "Yes",
      "relevance_other": "",
      "organism_name": "Mus musculus",
      "organism_strain": "",
      "organism_label": "",
      "sequencing_status": "SRA/Trace",
      "submitter_organization": "Laboratory of Cellular Metabolism and Metabolic Regulation, VIB-KU Leuven Center for Cancer Biology, VIB/KU Leuven",
      "submitter_organization_list": [
        "Laboratory of Cellular Metabolism and Metabolic Regulation, VIB-KU Leuven Center for Cancer Biology, VIB/KU Leuven"
      ],
      "supergroup": "Eukaryotes"
    }
  }
}
```

From the summary, we can confirm that this BioProject is a transcriptome study of lung metastases in mice. Now we can look at the links that are indexed in the BioProject database. Let's start with GEO datasets linked to this BioProject.

```python
eutils_link(
    dbfrom="bioproject",
    db="gds",
    webenv=search_results["esearchresult"]["webenv"],
    query_key=search_results["esearchresult"]["querykey"],
)
```
```json
{
  "header": {
    "type": "elink",
    "version": "0.3"
  },
  "linksets": [
    {
      "dbfrom": "bioproject",
      "ids": [
        "988806"
      ],
      "linksetdbhistories": [
        {
          "dbto": "gds",
          "linkname": "bioproject_gds",
          "querykey": "2"
        }
      ],
      "webenv": "MCID_69f379d2a5849e8d8c0781b5"
    }
  ]
}
```

We can see that there is a link to the GEO DataSet database, with a query key of 2. We can use this query key to fetch the linked GEO datasets:

```python
eutils_summary(
    webenv=links["linksets"][0]["webenv"],
    query_key=links["linksets"][0]["linksetdbhistories"][0]["querykey"],
    db="gds",
)
```
```json
{
  "header": {
    "type": "esummary",
    "version": "0.3"
  },
  "result": {
    "uids": [
      "200236084"
    ],
    "200236084": {
      "uid": "200236084",
      "accession": "GSE236084",
      "gds": "",
      "title": "Aspartate signaling increases the aggressiveness of lung metastases by inducing eIF5A-mediated translation (scRNA-Seq)",
      "summary": "Lung metastases are detected in more than half of patients with metastatic tumors. However, it remains largely unknown why the lung environment is a permissive niche for metastases. Here, we discover that pulmonary aspartate triggers a cellular signaling cascade in disseminated cancer cells resulting in a translational program that boosts lung metastasis. Specifically, we observe that patients and mice with breast cancer have high concentrations of aspartate in their lung interstitial fluid. This extracellular aspartate activates the ionotropic N-methyl-D-aspartate (NMDA) receptor in cancer cells, which induces CREB-dependent mRNA expression of deoxyhypusine hydroxylase (DOHH). The latter is essential for hypusination, a posttranslational modification required for the activity of the non-classical translation initiation factor eIF5A. In turn, a translational program with TGF-\u03b2 signaling as a central hub promotes collagen remodeling in the disseminated breast cancer cells. We detect key aspects of this mechanism in lung metastases from patients with breast cancer. In summary, we discover that pulmonary aspartate increases with breast cancer and induces a signaling cascade promoting the growth of lung metastases.",
      "gpl": "24247",
      "gse": "236084",
      "taxon": "Mus musculus",
      "entrytype": "GSE",
      "gdstype": "Expression profiling by high throughput sequencing",
      "ptechtype": "",
      "valtype": "",
      "ssinfo": "",
      "subsetinfo": "",
      "pdat": "2024/10/04",
      "suppfile": "TAR, TXT",
      "samples": [
        {
          "accession": "GSM7518067",
          "title": "TSF1a, Lungs from TSF Injection, Metastatic Seeding (d11), Replicate a"
        },
        {
          "accession": "GSM7518065",
          "title": "CM1, Lungs from CM Injection, Metastatic Seeding (d11)"
        },
        {
          "accession": "GSM7518068",
          "title": "TSF1b, Lungs from TSF Injection, Metastatic Seeding (d11), Replicate b"
        },
        {
          "accession": "GSM7518066",
          "title": "CM2, Lungs from CM Injection, Metastatic Colonization (d16)"
        },
        {
          "accession": "GSM7518069",
          "title": "TSF2, Lungs from TSF Injection, Metastatic Colonization (d16)"
        }
      ],
      "relations": [],
      "extrelations": [],
      "n_samples": 5,
      "seriestitle": "",
      "platformtitle": "",
      "platformtaxa": "",
      "samplestaxa": "",
      "pubmedids": [
        "39743589"
      ],
      "projects": [],
      "ftplink": "ftp://ftp.ncbi.nlm.nih.gov/geo/series/GSE236nnn/GSE236084/",
      "geo2r": "no",
      "bioproject": "PRJNA988806"
    }
  }
}
```

In the same way we can also fetch the linked SRA experiments for this BioProject, let's try that:

```python
eutils_link(
    dbfrom="bioproject",
    db="sra",
    webenv=search_results["esearchresult"]["webenv"],
    query_key=search_results["esearchresult"]["querykey"],
)
```
```json
{
  "header": {
    "type": "elink",
    "version": "0.3"
  },
  "linksets": [
    {
      "dbfrom": "bioproject",
      "ids": [
        "988806"
      ],
      "linksetdbhistories": [
        {
          "dbto": "sra",
          "linkname": "bioproject_sra",
          "querykey": "3"
        },
        {
          "dbto": "sra",
          "linkname": "bioproject_sra_all",
          "querykey": "4"
        }
      ],
      "webenv": "MCID_69f379d2a5849e8d8c0781b5"
    }
  ]
}
```

We can see that there are two links to the SRA database, one with the link name `bioproject_sra` and another with the link name `bioproject_sra_all`. The first one will give us the SRA experiments that are directly linked to the BioProject, while the second one will give us all the SRA experiments that are linked to the BioProject, including those that are linked through BioSamples. Let's fetch the SRA experiments using the first link:

```python
eutils_summary(
    webenv=links["linksets"][0]["webenv"],
    query_key=links["linksets"][0]["linksetdbhistories"][0]["querykey"],
    db="gds",
)
```
```json
{
  "header": {
    "type": "esummary",
    "version": "0.3"
  },
  "result": {
    "uids": [
      "28241758",
      "28241757",
      "28241756",
      "28241755",
      "28241754"
    ],
    "28241758": {
      "uid": "28241758",
      "expxml": "  <Summary><Title>GSM7518069: TSF2, Lungs from TSF Injection, Metastatic Colonization (d16); Mus musculus; RNA-Seq</Title><Platform instrument_model=\"Illumina NovaSeq 6000\">ILLUMINA</Platform><Statistics total_runs=\"1\" total_spots=\"332639939\" total_bases=\"39584152741\" total_size=\"13332204657\" load_done=\"true\" cluster_name=\"public\"/></Summary><Submitter acc=\"SRA1663813\" center_name=\"Laboratory of Cellular Metabolism and Metabolic Re\" contact_name=\"GEO Group\" lab_name=\"\"/><Experiment acc=\"SRX20810436\" ver=\"2\" status=\"public\" name=\"GSM7518069: TSF2, Lungs from TSF Injection, Metastatic Colonization (d16); Mus musculus; RNA-Seq\"/><Study acc=\"SRP446371\" name=\"Aspartate signaling increases the aggressiveness of lung metastases by inducing eIF5A-mediated translation (scRNA-Seq)\"/><Organism taxid=\"10090\" ScientificName=\"Mus musculus\"/><Sample acc=\"SRS18093899\" name=\"\"/><Instrument ILLUMINA=\"Illumina NovaSeq 6000\"/><Library_descriptor><LIBRARY_NAME>GSM7518069</LIBRARY_NAME><LIBRARY_STRATEGY>RNA-Seq</LIBRARY_STRATEGY><LIBRARY_SOURCE>TRANSCRIPTOMIC SINGLE CELL</LIBRARY_SOURCE><LIBRARY_SELECTION>cDNA</LIBRARY_SELECTION><LIBRARY_LAYOUT><PAIRED/></LIBRARY_LAYOUT><LIBRARY_CONSTRUCTION_PROTOCOL>Upon mice euthanasia, lungs were extracted and washed in blood bank saline, dried, and minced for ~ 2 minutes using blades. Minced lung tissues were then incubated with Liberase (0.3 mg/mL) and DNAse1 (1 \u03bcg/mL) for 45 min at 37\u00b0C, with occasional vortexing. The reaction was then quenched with PBS + 3% FBS + 2mM EDTA, after which cells were filtered through a 70\u03bcm cell strainer, washed once, and incubated with Red Blood Cell Lysis buffer. After red blood cell lysis, cells were washed once more and transferred through a 40 \u03bcm cell strainer. Cells were then pooled together from the 3 independent lung dissociations performed per group, resuspended in cell-culture medium at a density of 10^6 cells/mL, and kept on ice for immediate processing into single-cell cDNA libraries. Cell suspensions for each sample were converted to barcoded single-cell cDNA libraries using the Chromium Single Cell 5' v1.1 Library Kit, following the manufacturer's guidelines, and aiming for a total of 10,000 cells per library.</LIBRARY_CONSTRUCTION_PROTOCOL></Library_descriptor><Bioproject>PRJNA988806</Bioproject><Biosample>SAMN36028297</Biosample>  ",
      "runs": "                                <Run acc=\"SRR25056225\" total_spots=\"332639939\" total_bases=\"39584152741\" load_done=\"true\" is_public=\"true\" cluster_name=\"public\" static_data_available=\"true\"/>                                ",
      "extlinks": "",
      "createdate": "2024/10/04",
      "updatedate": "2023/06/28"
    },
    "28241757": {
      "uid": "28241757",
      "expxml": "  <Summary><Title>GSM7518068: TSF1b, Lungs from TSF Injection, Metastatic Seeding (d11), Replicate b; Mus musculus; RNA-Seq</Title><Platform instrument_model=\"Illumina NovaSeq 6000\">ILLUMINA</Platform><Statistics total_runs=\"1\" total_spots=\"321922508\" total_bases=\"38308778452\" total_size=\"12983749287\" load_done=\"true\" cluster_name=\"public\"/></Summary><Submitter acc=\"SRA1663813\" center_name=\"Laboratory of Cellular Metabolism and Metabolic Re\" contact_name=\"GEO Group\" lab_name=\"\"/><Experiment acc=\"SRX20810435\" ver=\"2\" status=\"public\" name=\"GSM7518068: TSF1b, Lungs from TSF Injection, Metastatic Seeding (d11), Replicate b; Mus musculus; RNA-Seq\"/><Study acc=\"SRP446371\" name=\"Aspartate signaling increases the aggressiveness of lung metastases by inducing eIF5A-mediated translation (scRNA-Seq)\"/><Organism taxid=\"10090\" ScientificName=\"Mus musculus\"/><Sample acc=\"SRS18093900\" name=\"\"/><Instrument ILLUMINA=\"Illumina NovaSeq 6000\"/><Library_descriptor><LIBRARY_NAME>GSM7518068</LIBRARY_NAME><LIBRARY_STRATEGY>RNA-Seq</LIBRARY_STRATEGY><LIBRARY_SOURCE>TRANSCRIPTOMIC SINGLE CELL</LIBRARY_SOURCE><LIBRARY_SELECTION>cDNA</LIBRARY_SELECTION><LIBRARY_LAYOUT><PAIRED/></LIBRARY_LAYOUT><LIBRARY_CONSTRUCTION_PROTOCOL>Upon mice euthanasia, lungs were extracted and washed in blood bank saline, dried, and minced for ~ 2 minutes using blades. Minced lung tissues were then incubated with Liberase (0.3 mg/mL) and DNAse1 (1 \u03bcg/mL) for 45 min at 37\u00b0C, with occasional vortexing. The reaction was then quenched with PBS + 3% FBS + 2mM EDTA, after which cells were filtered through a 70\u03bcm cell strainer, washed once, and incubated with Red Blood Cell Lysis buffer. After red blood cell lysis, cells were washed once more and transferred through a 40 \u03bcm cell strainer. Cells were then pooled together from the 3 independent lung dissociations performed per group, resuspended in cell-culture medium at a density of 10^6 cells/mL, and kept on ice for immediate processing into single-cell cDNA libraries. Cell suspensions for each sample were converted to barcoded single-cell cDNA libraries using the Chromium Single Cell 5' v1.1 Library Kit, following the manufacturer's guidelines, and aiming for a total of 10,000 cells per library.</LIBRARY_CONSTRUCTION_PROTOCOL></Library_descriptor><Bioproject>PRJNA988806</Bioproject><Biosample>SAMN36028298</Biosample>  ",
      "runs": "                                <Run acc=\"SRR25056226\" total_spots=\"321922508\" total_bases=\"38308778452\" load_done=\"true\" is_public=\"true\" cluster_name=\"public\" static_data_available=\"true\"/>                                ",
      "extlinks": "",
      "createdate": "2024/10/04",
      "updatedate": "2023/06/28"
    },
    "28241756": {
      "uid": "28241756",
      "expxml": "  <Summary><Title>GSM7518067: TSF1a, Lungs from TSF Injection, Metastatic Seeding (d11), Replicate a; Mus musculus; RNA-Seq</Title><Platform instrument_model=\"Illumina NovaSeq 6000\">ILLUMINA</Platform><Statistics total_runs=\"1\" total_spots=\"362167958\" total_bases=\"43097987002\" total_size=\"14474390207\" load_done=\"true\" cluster_name=\"public\"/></Summary><Submitter acc=\"SRA1663813\" center_name=\"Laboratory of Cellular Metabolism and Metabolic Re\" contact_name=\"GEO Group\" lab_name=\"\"/><Experiment acc=\"SRX20810434\" ver=\"2\" status=\"public\" name=\"GSM7518067: TSF1a, Lungs from TSF Injection, Metastatic Seeding (d11), Replicate a; Mus musculus; RNA-Seq\"/><Study acc=\"SRP446371\" name=\"Aspartate signaling increases the aggressiveness of lung metastases by inducing eIF5A-mediated translation (scRNA-Seq)\"/><Organism taxid=\"10090\" ScientificName=\"Mus musculus\"/><Sample acc=\"SRS18093898\" name=\"\"/><Instrument ILLUMINA=\"Illumina NovaSeq 6000\"/><Library_descriptor><LIBRARY_NAME>GSM7518067</LIBRARY_NAME><LIBRARY_STRATEGY>RNA-Seq</LIBRARY_STRATEGY><LIBRARY_SOURCE>TRANSCRIPTOMIC SINGLE CELL</LIBRARY_SOURCE><LIBRARY_SELECTION>cDNA</LIBRARY_SELECTION><LIBRARY_LAYOUT><PAIRED/></LIBRARY_LAYOUT><LIBRARY_CONSTRUCTION_PROTOCOL>Upon mice euthanasia, lungs were extracted and washed in blood bank saline, dried, and minced for ~ 2 minutes using blades. Minced lung tissues were then incubated with Liberase (0.3 mg/mL) and DNAse1 (1 \u03bcg/mL) for 45 min at 37\u00b0C, with occasional vortexing. The reaction was then quenched with PBS + 3% FBS + 2mM EDTA, after which cells were filtered through a 70\u03bcm cell strainer, washed once, and incubated with Red Blood Cell Lysis buffer. After red blood cell lysis, cells were washed once more and transferred through a 40 \u03bcm cell strainer. Cells were then pooled together from the 3 independent lung dissociations performed per group, resuspended in cell-culture medium at a density of 10^6 cells/mL, and kept on ice for immediate processing into single-cell cDNA libraries. Cell suspensions for each sample were converted to barcoded single-cell cDNA libraries using the Chromium Single Cell 5' v1.1 Library Kit, following the manufacturer's guidelines, and aiming for a total of 10,000 cells per library.</LIBRARY_CONSTRUCTION_PROTOCOL></Library_descriptor><Bioproject>PRJNA988806</Bioproject><Biosample>SAMN36028299</Biosample>  ",
      "runs": "                                <Run acc=\"SRR25056227\" total_spots=\"362167958\" total_bases=\"43097987002\" load_done=\"true\" is_public=\"true\" cluster_name=\"public\" static_data_available=\"true\"/>                                ",
      "extlinks": "",
      "createdate": "2024/10/04",
      "updatedate": "2023/06/28"
    },
    "28241755": {
      "uid": "28241755",
      "expxml": "  <Summary><Title>GSM7518066: CM2, Lungs from CM Injection, Metastatic Colonization (d16); Mus musculus; RNA-Seq</Title><Platform instrument_model=\"Illumina NovaSeq 6000\">ILLUMINA</Platform><Statistics total_runs=\"1\" total_spots=\"339308480\" total_bases=\"40377709120\" total_size=\"13796528558\" load_done=\"true\" cluster_name=\"public\"/></Summary><Submitter acc=\"SRA1663813\" center_name=\"Laboratory of Cellular Metabolism and Metabolic Re\" contact_name=\"GEO Group\" lab_name=\"\"/><Experiment acc=\"SRX20810433\" ver=\"2\" status=\"public\" name=\"GSM7518066: CM2, Lungs from CM Injection, Metastatic Colonization (d16); Mus musculus; RNA-Seq\"/><Study acc=\"SRP446371\" name=\"Aspartate signaling increases the aggressiveness of lung metastases by inducing eIF5A-mediated translation (scRNA-Seq)\"/><Organism taxid=\"10090\" ScientificName=\"Mus musculus\"/><Sample acc=\"SRS18093897\" name=\"\"/><Instrument ILLUMINA=\"Illumina NovaSeq 6000\"/><Library_descriptor><LIBRARY_NAME>GSM7518066</LIBRARY_NAME><LIBRARY_STRATEGY>RNA-Seq</LIBRARY_STRATEGY><LIBRARY_SOURCE>TRANSCRIPTOMIC SINGLE CELL</LIBRARY_SOURCE><LIBRARY_SELECTION>cDNA</LIBRARY_SELECTION><LIBRARY_LAYOUT><PAIRED/></LIBRARY_LAYOUT><LIBRARY_CONSTRUCTION_PROTOCOL>Upon mice euthanasia, lungs were extracted and washed in blood bank saline, dried, and minced for ~ 2 minutes using blades. Minced lung tissues were then incubated with Liberase (0.3 mg/mL) and DNAse1 (1 \u03bcg/mL) for 45 min at 37\u00b0C, with occasional vortexing. The reaction was then quenched with PBS + 3% FBS + 2mM EDTA, after which cells were filtered through a 70\u03bcm cell strainer, washed once, and incubated with Red Blood Cell Lysis buffer. After red blood cell lysis, cells were washed once more and transferred through a 40 \u03bcm cell strainer. Cells were then pooled together from the 3 independent lung dissociations performed per group, resuspended in cell-culture medium at a density of 10^6 cells/mL, and kept on ice for immediate processing into single-cell cDNA libraries. Cell suspensions for each sample were converted to barcoded single-cell cDNA libraries using the Chromium Single Cell 5' v1.1 Library Kit, following the manufacturer's guidelines, and aiming for a total of 10,000 cells per library.</LIBRARY_CONSTRUCTION_PROTOCOL></Library_descriptor><Bioproject>PRJNA988806</Bioproject><Biosample>SAMN36028300</Biosample>  ",
      "runs": "                                <Run acc=\"SRR25056228\" total_spots=\"339308480\" total_bases=\"40377709120\" load_done=\"true\" is_public=\"true\" cluster_name=\"public\" static_data_available=\"true\"/>                                ",
      "extlinks": "",
      "createdate": "2024/10/04",
      "updatedate": "2023/06/28"
    },
    "28241754": {
      "uid": "28241754",
      "expxml": "  <Summary><Title>GSM7518065: CM1, Lungs from CM Injection, Metastatic Seeding (d11); Mus musculus; RNA-Seq</Title><Platform instrument_model=\"Illumina NovaSeq 6000\">ILLUMINA</Platform><Statistics total_runs=\"1\" total_spots=\"325016382\" total_bases=\"38676949458\" total_size=\"13072276347\" load_done=\"true\" cluster_name=\"public\"/></Summary><Submitter acc=\"SRA1663813\" center_name=\"Laboratory of Cellular Metabolism and Metabolic Re\" contact_name=\"GEO Group\" lab_name=\"\"/><Experiment acc=\"SRX20810432\" ver=\"2\" status=\"public\" name=\"GSM7518065: CM1, Lungs from CM Injection, Metastatic Seeding (d11); Mus musculus; RNA-Seq\"/><Study acc=\"SRP446371\" name=\"Aspartate signaling increases the aggressiveness of lung metastases by inducing eIF5A-mediated translation (scRNA-Seq)\"/><Organism taxid=\"10090\" ScientificName=\"Mus musculus\"/><Sample acc=\"SRS18093896\" name=\"\"/><Instrument ILLUMINA=\"Illumina NovaSeq 6000\"/><Library_descriptor><LIBRARY_NAME>GSM7518065</LIBRARY_NAME><LIBRARY_STRATEGY>RNA-Seq</LIBRARY_STRATEGY><LIBRARY_SOURCE>TRANSCRIPTOMIC SINGLE CELL</LIBRARY_SOURCE><LIBRARY_SELECTION>cDNA</LIBRARY_SELECTION><LIBRARY_LAYOUT><PAIRED/></LIBRARY_LAYOUT><LIBRARY_CONSTRUCTION_PROTOCOL>Upon mice euthanasia, lungs were extracted and washed in blood bank saline, dried, and minced for ~ 2 minutes using blades. Minced lung tissues were then incubated with Liberase (0.3 mg/mL) and DNAse1 (1 \u03bcg/mL) for 45 min at 37\u00b0C, with occasional vortexing. The reaction was then quenched with PBS + 3% FBS + 2mM EDTA, after which cells were filtered through a 70\u03bcm cell strainer, washed once, and incubated with Red Blood Cell Lysis buffer. After red blood cell lysis, cells were washed once more and transferred through a 40 \u03bcm cell strainer. Cells were then pooled together from the 3 independent lung dissociations performed per group, resuspended in cell-culture medium at a density of 10^6 cells/mL, and kept on ice for immediate processing into single-cell cDNA libraries. Cell suspensions for each sample were converted to barcoded single-cell cDNA libraries using the Chromium Single Cell 5' v1.1 Library Kit, following the manufacturer's guidelines, and aiming for a total of 10,000 cells per library.</LIBRARY_CONSTRUCTION_PROTOCOL></Library_descriptor><Bioproject>PRJNA988806</Bioproject><Biosample>SAMN36028301</Biosample>  ",
      "runs": "                                <Run acc=\"SRR25056229\" total_spots=\"325016382\" total_bases=\"38676949458\" load_done=\"true\" is_public=\"true\" cluster_name=\"public\" static_data_available=\"true\"/>                                ",
      "extlinks": "",
      "createdate": "2024/10/04",
      "updatedate": "2023/06/28"
    }
  }
}
```

We can see that there are 5 SRA experiments linked to this BioProject, which correspond to the 5 samples that were sequenced in the study. Each experiment has a unique accession number (SRX20810432, SRX20810433, SRX20810434, SRX20810435, SRX20810436) and contains information about the library construction protocol, the instrument used for sequencing, the number of runs, spots, bases and size of the data, as well as links to the corresponding BioSample and BioProject records.

Let's also try to fetch the SRA experiments using the second link, which should give us the same results:

```python
eutils_summary(
    webenv=links["linksets"][0]["webenv"],
    query_key=links["linksets"][0]["linksetdbhistories"][1]["querykey"],
    db="gds",
)
```
```json
{
  "header": {
    "type": "esummary",
    "version": "0.3"
  },
  "result": {
    "uids": [
      "28241758",
      "28241757",
      "28241756",
      "28241755",
      "28241754"
    ],
    "28241758": {
      "uid": "28241758",
      "expxml": "  <Summary><Title>GSM7518069: TSF2, Lungs from TSF Injection, Metastatic Colonization (d16); Mus musculus; RNA-Seq</Title><Platform instrument_model=\"Illumina NovaSeq 6000\">ILLUMINA</Platform><Statistics total_runs=\"1\" total_spots=\"332639939\" total_bases=\"39584152741\" total_size=\"13332204657\" load_done=\"true\" cluster_name=\"public\"/></Summary><Submitter acc=\"SRA1663813\" center_name=\"Laboratory of Cellular Metabolism and Metabolic Re\" contact_name=\"GEO Group\" lab_name=\"\"/><Experiment acc=\"SRX20810436\" ver=\"2\" status=\"public\" name=\"GSM7518069: TSF2, Lungs from TSF Injection, Metastatic Colonization (d16); Mus musculus; RNA-Seq\"/><Study acc=\"SRP446371\" name=\"Aspartate signaling increases the aggressiveness of lung metastases by inducing eIF5A-mediated translation (scRNA-Seq)\"/><Organism taxid=\"10090\" ScientificName=\"Mus musculus\"/><Sample acc=\"SRS18093899\" name=\"\"/><Instrument ILLUMINA=\"Illumina NovaSeq 6000\"/><Library_descriptor><LIBRARY_NAME>GSM7518069</LIBRARY_NAME><LIBRARY_STRATEGY>RNA-Seq</LIBRARY_STRATEGY><LIBRARY_SOURCE>TRANSCRIPTOMIC SINGLE CELL</LIBRARY_SOURCE><LIBRARY_SELECTION>cDNA</LIBRARY_SELECTION><LIBRARY_LAYOUT><PAIRED/></LIBRARY_LAYOUT><LIBRARY_CONSTRUCTION_PROTOCOL>Upon mice euthanasia, lungs were extracted and washed in blood bank saline, dried, and minced for ~ 2 minutes using blades. Minced lung tissues were then incubated with Liberase (0.3 mg/mL) and DNAse1 (1 \u03bcg/mL) for 45 min at 37\u00b0C, with occasional vortexing. The reaction was then quenched with PBS + 3% FBS + 2mM EDTA, after which cells were filtered through a 70\u03bcm cell strainer, washed once, and incubated with Red Blood Cell Lysis buffer. After red blood cell lysis, cells were washed once more and transferred through a 40 \u03bcm cell strainer. Cells were then pooled together from the 3 independent lung dissociations performed per group, resuspended in cell-culture medium at a density of 10^6 cells/mL, and kept on ice for immediate processing into single-cell cDNA libraries. Cell suspensions for each sample were converted to barcoded single-cell cDNA libraries using the Chromium Single Cell 5' v1.1 Library Kit, following the manufacturer's guidelines, and aiming for a total of 10,000 cells per library.</LIBRARY_CONSTRUCTION_PROTOCOL></Library_descriptor><Bioproject>PRJNA988806</Bioproject><Biosample>SAMN36028297</Biosample>  ",
      "runs": "                                <Run acc=\"SRR25056225\" total_spots=\"332639939\" total_bases=\"39584152741\" load_done=\"true\" is_public=\"true\" cluster_name=\"public\" static_data_available=\"true\"/>                                ",
      "extlinks": "",
      "createdate": "2024/10/04",
      "updatedate": "2023/06/28"
    },
    "28241757": {
      "uid": "28241757",
      "expxml": "  <Summary><Title>GSM7518068: TSF1b, Lungs from TSF Injection, Metastatic Seeding (d11), Replicate b; Mus musculus; RNA-Seq</Title><Platform instrument_model=\"Illumina NovaSeq 6000\">ILLUMINA</Platform><Statistics total_runs=\"1\" total_spots=\"321922508\" total_bases=\"38308778452\" total_size=\"12983749287\" load_done=\"true\" cluster_name=\"public\"/></Summary><Submitter acc=\"SRA1663813\" center_name=\"Laboratory of Cellular Metabolism and Metabolic Re\" contact_name=\"GEO Group\" lab_name=\"\"/><Experiment acc=\"SRX20810435\" ver=\"2\" status=\"public\" name=\"GSM7518068: TSF1b, Lungs from TSF Injection, Metastatic Seeding (d11), Replicate b; Mus musculus; RNA-Seq\"/><Study acc=\"SRP446371\" name=\"Aspartate signaling increases the aggressiveness of lung metastases by inducing eIF5A-mediated translation (scRNA-Seq)\"/><Organism taxid=\"10090\" ScientificName=\"Mus musculus\"/><Sample acc=\"SRS18093900\" name=\"\"/><Instrument ILLUMINA=\"Illumina NovaSeq 6000\"/><Library_descriptor><LIBRARY_NAME>GSM7518068</LIBRARY_NAME><LIBRARY_STRATEGY>RNA-Seq</LIBRARY_STRATEGY><LIBRARY_SOURCE>TRANSCRIPTOMIC SINGLE CELL</LIBRARY_SOURCE><LIBRARY_SELECTION>cDNA</LIBRARY_SELECTION><LIBRARY_LAYOUT><PAIRED/></LIBRARY_LAYOUT><LIBRARY_CONSTRUCTION_PROTOCOL>Upon mice euthanasia, lungs were extracted and washed in blood bank saline, dried, and minced for ~ 2 minutes using blades. Minced lung tissues were then incubated with Liberase (0.3 mg/mL) and DNAse1 (1 \u03bcg/mL) for 45 min at 37\u00b0C, with occasional vortexing. The reaction was then quenched with PBS + 3% FBS + 2mM EDTA, after which cells were filtered through a 70\u03bcm cell strainer, washed once, and incubated with Red Blood Cell Lysis buffer. After red blood cell lysis, cells were washed once more and transferred through a 40 \u03bcm cell strainer. Cells were then pooled together from the 3 independent lung dissociations performed per group, resuspended in cell-culture medium at a density of 10^6 cells/mL, and kept on ice for immediate processing into single-cell cDNA libraries. Cell suspensions for each sample were converted to barcoded single-cell cDNA libraries using the Chromium Single Cell 5' v1.1 Library Kit, following the manufacturer's guidelines, and aiming for a total of 10,000 cells per library.</LIBRARY_CONSTRUCTION_PROTOCOL></Library_descriptor><Bioproject>PRJNA988806</Bioproject><Biosample>SAMN36028298</Biosample>  ",
      "runs": "                                <Run acc=\"SRR25056226\" total_spots=\"321922508\" total_bases=\"38308778452\" load_done=\"true\" is_public=\"true\" cluster_name=\"public\" static_data_available=\"true\"/>                                ",
      "extlinks": "",
      "createdate": "2024/10/04",
      "updatedate": "2023/06/28"
    },
    "28241756": {
      "uid": "28241756",
      "expxml": "  <Summary><Title>GSM7518067: TSF1a, Lungs from TSF Injection, Metastatic Seeding (d11), Replicate a; Mus musculus; RNA-Seq</Title><Platform instrument_model=\"Illumina NovaSeq 6000\">ILLUMINA</Platform><Statistics total_runs=\"1\" total_spots=\"362167958\" total_bases=\"43097987002\" total_size=\"14474390207\" load_done=\"true\" cluster_name=\"public\"/></Summary><Submitter acc=\"SRA1663813\" center_name=\"Laboratory of Cellular Metabolism and Metabolic Re\" contact_name=\"GEO Group\" lab_name=\"\"/><Experiment acc=\"SRX20810434\" ver=\"2\" status=\"public\" name=\"GSM7518067: TSF1a, Lungs from TSF Injection, Metastatic Seeding (d11), Replicate a; Mus musculus; RNA-Seq\"/><Study acc=\"SRP446371\" name=\"Aspartate signaling increases the aggressiveness of lung metastases by inducing eIF5A-mediated translation (scRNA-Seq)\"/><Organism taxid=\"10090\" ScientificName=\"Mus musculus\"/><Sample acc=\"SRS18093898\" name=\"\"/><Instrument ILLUMINA=\"Illumina NovaSeq 6000\"/><Library_descriptor><LIBRARY_NAME>GSM7518067</LIBRARY_NAME><LIBRARY_STRATEGY>RNA-Seq</LIBRARY_STRATEGY><LIBRARY_SOURCE>TRANSCRIPTOMIC SINGLE CELL</LIBRARY_SOURCE><LIBRARY_SELECTION>cDNA</LIBRARY_SELECTION><LIBRARY_LAYOUT><PAIRED/></LIBRARY_LAYOUT><LIBRARY_CONSTRUCTION_PROTOCOL>Upon mice euthanasia, lungs were extracted and washed in blood bank saline, dried, and minced for ~ 2 minutes using blades. Minced lung tissues were then incubated with Liberase (0.3 mg/mL) and DNAse1 (1 \u03bcg/mL) for 45 min at 37\u00b0C, with occasional vortexing. The reaction was then quenched with PBS + 3% FBS + 2mM EDTA, after which cells were filtered through a 70\u03bcm cell strainer, washed once, and incubated with Red Blood Cell Lysis buffer. After red blood cell lysis, cells were washed once more and transferred through a 40 \u03bcm cell strainer. Cells were then pooled together from the 3 independent lung dissociations performed per group, resuspended in cell-culture medium at a density of 10^6 cells/mL, and kept on ice for immediate processing into single-cell cDNA libraries. Cell suspensions for each sample were converted to barcoded single-cell cDNA libraries using the Chromium Single Cell 5' v1.1 Library Kit, following the manufacturer's guidelines, and aiming for a total of 10,000 cells per library.</LIBRARY_CONSTRUCTION_PROTOCOL></Library_descriptor><Bioproject>PRJNA988806</Bioproject><Biosample>SAMN36028299</Biosample>  ",
      "runs": "                                <Run acc=\"SRR25056227\" total_spots=\"362167958\" total_bases=\"43097987002\" load_done=\"true\" is_public=\"true\" cluster_name=\"public\" static_data_available=\"true\"/>                                ",
      "extlinks": "",
      "createdate": "2024/10/04",
      "updatedate": "2023/06/28"
    },
    "28241755": {
      "uid": "28241755",
      "expxml": "  <Summary><Title>GSM7518066: CM2, Lungs from CM Injection, Metastatic Colonization (d16); Mus musculus; RNA-Seq</Title><Platform instrument_model=\"Illumina NovaSeq 6000\">ILLUMINA</Platform><Statistics total_runs=\"1\" total_spots=\"339308480\" total_bases=\"40377709120\" total_size=\"13796528558\" load_done=\"true\" cluster_name=\"public\"/></Summary><Submitter acc=\"SRA1663813\" center_name=\"Laboratory of Cellular Metabolism and Metabolic Re\" contact_name=\"GEO Group\" lab_name=\"\"/><Experiment acc=\"SRX20810433\" ver=\"2\" status=\"public\" name=\"GSM7518066: CM2, Lungs from CM Injection, Metastatic Colonization (d16); Mus musculus; RNA-Seq\"/><Study acc=\"SRP446371\" name=\"Aspartate signaling increases the aggressiveness of lung metastases by inducing eIF5A-mediated translation (scRNA-Seq)\"/><Organism taxid=\"10090\" ScientificName=\"Mus musculus\"/><Sample acc=\"SRS18093897\" name=\"\"/><Instrument ILLUMINA=\"Illumina NovaSeq 6000\"/><Library_descriptor><LIBRARY_NAME>GSM7518066</LIBRARY_NAME><LIBRARY_STRATEGY>RNA-Seq</LIBRARY_STRATEGY><LIBRARY_SOURCE>TRANSCRIPTOMIC SINGLE CELL</LIBRARY_SOURCE><LIBRARY_SELECTION>cDNA</LIBRARY_SELECTION><LIBRARY_LAYOUT><PAIRED/></LIBRARY_LAYOUT><LIBRARY_CONSTRUCTION_PROTOCOL>Upon mice euthanasia, lungs were extracted and washed in blood bank saline, dried, and minced for ~ 2 minutes using blades. Minced lung tissues were then incubated with Liberase (0.3 mg/mL) and DNAse1 (1 \u03bcg/mL) for 45 min at 37\u00b0C, with occasional vortexing. The reaction was then quenched with PBS + 3% FBS + 2mM EDTA, after which cells were filtered through a 70\u03bcm cell strainer, washed once, and incubated with Red Blood Cell Lysis buffer. After red blood cell lysis, cells were washed once more and transferred through a 40 \u03bcm cell strainer. Cells were then pooled together from the 3 independent lung dissociations performed per group, resuspended in cell-culture medium at a density of 10^6 cells/mL, and kept on ice for immediate processing into single-cell cDNA libraries. Cell suspensions for each sample were converted to barcoded single-cell cDNA libraries using the Chromium Single Cell 5' v1.1 Library Kit, following the manufacturer's guidelines, and aiming for a total of 10,000 cells per library.</LIBRARY_CONSTRUCTION_PROTOCOL></Library_descriptor><Bioproject>PRJNA988806</Bioproject><Biosample>SAMN36028300</Biosample>  ",
      "runs": "                                <Run acc=\"SRR25056228\" total_spots=\"339308480\" total_bases=\"40377709120\" load_done=\"true\" is_public=\"true\" cluster_name=\"public\" static_data_available=\"true\"/>                                ",
      "extlinks": "",
      "createdate": "2024/10/04",
      "updatedate": "2023/06/28"
    },
    "28241754": {
      "uid": "28241754",
      "expxml": "  <Summary><Title>GSM7518065: CM1, Lungs from CM Injection, Metastatic Seeding (d11); Mus musculus; RNA-Seq</Title><Platform instrument_model=\"Illumina NovaSeq 6000\">ILLUMINA</Platform><Statistics total_runs=\"1\" total_spots=\"325016382\" total_bases=\"38676949458\" total_size=\"13072276347\" load_done=\"true\" cluster_name=\"public\"/></Summary><Submitter acc=\"SRA1663813\" center_name=\"Laboratory of Cellular Metabolism and Metabolic Re\" contact_name=\"GEO Group\" lab_name=\"\"/><Experiment acc=\"SRX20810432\" ver=\"2\" status=\"public\" name=\"GSM7518065: CM1, Lungs from CM Injection, Metastatic Seeding (d11); Mus musculus; RNA-Seq\"/><Study acc=\"SRP446371\" name=\"Aspartate signaling increases the aggressiveness of lung metastases by inducing eIF5A-mediated translation (scRNA-Seq)\"/><Organism taxid=\"10090\" ScientificName=\"Mus musculus\"/><Sample acc=\"SRS18093896\" name=\"\"/><Instrument ILLUMINA=\"Illumina NovaSeq 6000\"/><Library_descriptor><LIBRARY_NAME>GSM7518065</LIBRARY_NAME><LIBRARY_STRATEGY>RNA-Seq</LIBRARY_STRATEGY><LIBRARY_SOURCE>TRANSCRIPTOMIC SINGLE CELL</LIBRARY_SOURCE><LIBRARY_SELECTION>cDNA</LIBRARY_SELECTION><LIBRARY_LAYOUT><PAIRED/></LIBRARY_LAYOUT><LIBRARY_CONSTRUCTION_PROTOCOL>Upon mice euthanasia, lungs were extracted and washed in blood bank saline, dried, and minced for ~ 2 minutes using blades. Minced lung tissues were then incubated with Liberase (0.3 mg/mL) and DNAse1 (1 \u03bcg/mL) for 45 min at 37\u00b0C, with occasional vortexing. The reaction was then quenched with PBS + 3% FBS + 2mM EDTA, after which cells were filtered through a 70\u03bcm cell strainer, washed once, and incubated with Red Blood Cell Lysis buffer. After red blood cell lysis, cells were washed once more and transferred through a 40 \u03bcm cell strainer. Cells were then pooled together from the 3 independent lung dissociations performed per group, resuspended in cell-culture medium at a density of 10^6 cells/mL, and kept on ice for immediate processing into single-cell cDNA libraries. Cell suspensions for each sample were converted to barcoded single-cell cDNA libraries using the Chromium Single Cell 5' v1.1 Library Kit, following the manufacturer's guidelines, and aiming for a total of 10,000 cells per library.</LIBRARY_CONSTRUCTION_PROTOCOL></Library_descriptor><Bioproject>PRJNA988806</Bioproject><Biosample>SAMN36028301</Biosample>  ",
      "runs": "                                <Run acc=\"SRR25056229\" total_spots=\"325016382\" total_bases=\"38676949458\" load_done=\"true\" is_public=\"true\" cluster_name=\"public\" static_data_available=\"true\"/>                                ",
      "extlinks": "",
      "createdate": "2024/10/04",
      "updatedate": "2023/06/28"
    }
  }
}
```

We can see that the results are the same as before, which confirms that both links in the LinkOut section of the BioProject record point to the same set of SRA experiments.

Next we can try to fetch biosamples linked to this BioProject using the first link:

```python
eutils_link(
    dbfrom="bioproject",
    db="biosample",
    webenv=search_results["esearchresult"]["webenv"],
    query_key=search_results["esearchresult"]["querykey"],
)
```
```json
{
  "header": {
    "type": "elink",
    "version": "0.3"
  },
  "linksets": [
    {
      "dbfrom": "bioproject",
      "ids": [
        "988806"
      ],
      "linksetdbhistories": [
        {
          "dbto": "biosample",
          "linkname": "bioproject_biosample",
          "querykey": "5"
        },
        {
          "dbto": "biosample",
          "linkname": "bioproject_biosample_all",
          "querykey": "6"
        }
      ],
      "webenv": "MCID_69f379d2a5849e8d8c0781b5"
    }
  ]
}
```

We can see that there are two links to BioSample records, one for the linked biosamples and another for all biosamples linked to the BioProject. Let's try to fetch the linked biosamples using the first link:

```python
eutils_summary(
    webenv=links["linksets"][0]["webenv"],
    query_key=links["linksets"][0]["linksetdbhistories"][0]["querykey"],
    db="gds",
)
```
```json
{
  "header": {
    "type": "esummary",
    "version": "0.3"
  },
  "result": {
    "uids": [
      "36028301",
      "36028300",
      "36028299",
      "36028298",
      "36028297"
    ],
    "36028301": {
      "uid": "36028301",
      "title": "CM1, Lungs from CM Injection, Metastatic Seeding (d11)",
      "accession": "SAMN36028301",
      "date": "2024/10/04",
      "publicationdate": "2024/10/04",
      "modificationdate": "2024/10/04",
      "organization": "Laboratory of Cellular Metabolism and Metabolic Regulation, VIB-KU Leuven Center for Cancer Biology, VIB/KU Leuven",
      "taxonomy": "10090",
      "organism": "Mus musculus",
      "sourcesample": "BioSample:SAMN36028301",
      "sampledata": "<BioSample access=\"public\" publication_date=\"2024-10-04T00:00:00.000\" last_update=\"2024-10-04T14:36:16.933\" submission_date=\"2023-06-28T20:00:08.023\" id=\"36028301\" accession=\"SAMN36028301\">   <Ids>     <Id db=\"BioSample\" is_primary=\"1\">SAMN36028301</Id>     <Id db=\"SRA\">SRS18093896</Id>     <Id db=\"GEO\">GSM7518065</Id>   </Ids>   <Description>     <Title>CM1, Lungs from CM Injection, Metastatic Seeding (d11)</Title>     <Organism taxonomy_id=\"10090\" taxonomy_name=\"Mus musculus\">       <OrganismName>Mus musculus</OrganismName>     </Organism>   </Description>   <Owner>     <Name>Laboratory of Cellular Metabolism and Metabolic Regulation, VIB-KU Leuven Center for Cancer Biology, VIB/KU Leuven</Name>     <Contacts>       <Contact>         <Name>           <First>Juan</First>           <Last>Fern\u00e1ndez-Garc\u00eda</Last>         </Name>       </Contact>     </Contacts>   </Owner>   <Models>     <Model>Generic</Model>   </Models>   <Package display_name=\"Generic\">Generic.1.0</Package>   <Attributes>     <Attribute attribute_name=\"source_name\" harmonized_name=\"source_name\" display_name=\"source name\">Lung</Attribute>     <Attribute attribute_name=\"strain\" harmonized_name=\"strain\" display_name=\"strain\">BALB/c</Attribute>     <Attribute attribute_name=\"Sex\" harmonized_name=\"sex\" display_name=\"sex\">female</Attribute>     <Attribute attribute_name=\"age\" harmonized_name=\"age\" display_name=\"age\">10 weeks-old</Attribute>     <Attribute attribute_name=\"tissue\" harmonized_name=\"tissue\" display_name=\"tissue\">Lung</Attribute>     <Attribute attribute_name=\"treatment\" harmonized_name=\"treatment\" display_name=\"treatment\">Control Medium (CM)</Attribute>     <Attribute attribute_name=\"injected cell_line\">4T1 CD90.1</Attribute>     <Attribute attribute_name=\"metastasis stage\">Seeding (d11)</Attribute>     <Attribute attribute_name=\"geo_loc_name\" harmonized_name=\"geo_loc_name\" display_name=\"geographic location\">missing</Attribute>     <Attribute attribute_name=\"collection_date\" harmonized_name=\"collection_date\" display_name=\"collection date\">missing</Attribute>   </Attributes>   <Links>     <Link type=\"url\" label=\"GEO Sample GSM7518065\">https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSM7518065</Link>     <Link type=\"entrez\" target=\"bioproject\" label=\"PRJNA988806\">988806</Link>   </Links>   <Status status=\"live\" when=\"2024-10-04T14:36:16.933\"/> </BioSample> ",
      "identifiers": "BioSample: SAMN36028301; SRA: SRS18093896; GEO: GSM7518065",
      "infraspecies": "strain: BALB/c",
      "package": "Generic",
      "sortkey": 120241004
    },
    "36028300": {
      "uid": "36028300",
      "title": "CM2, Lungs from CM Injection, Metastatic Colonization (d16)",
      "accession": "SAMN36028300",
      "date": "2024/10/04",
      "publicationdate": "2024/10/04",
      "modificationdate": "2024/10/04",
      "organization": "Laboratory of Cellular Metabolism and Metabolic Regulation, VIB-KU Leuven Center for Cancer Biology, VIB/KU Leuven",
      "taxonomy": "10090",
      "organism": "Mus musculus",
      "sourcesample": "BioSample:SAMN36028300",
      "sampledata": "<BioSample access=\"public\" publication_date=\"2024-10-04T00:00:00.000\" last_update=\"2024-10-04T14:36:16.816\" submission_date=\"2023-06-28T20:00:07.930\" id=\"36028300\" accession=\"SAMN36028300\">   <Ids>     <Id db=\"BioSample\" is_primary=\"1\">SAMN36028300</Id>     <Id db=\"SRA\">SRS18093897</Id>     <Id db=\"GEO\">GSM7518066</Id>   </Ids>   <Description>     <Title>CM2, Lungs from CM Injection, Metastatic Colonization (d16)</Title>     <Organism taxonomy_id=\"10090\" taxonomy_name=\"Mus musculus\">       <OrganismName>Mus musculus</OrganismName>     </Organism>   </Description>   <Owner>     <Name>Laboratory of Cellular Metabolism and Metabolic Regulation, VIB-KU Leuven Center for Cancer Biology, VIB/KU Leuven</Name>     <Contacts>       <Contact>         <Name>           <First>Juan</First>           <Last>Fern\u00e1ndez-Garc\u00eda</Last>         </Name>       </Contact>     </Contacts>   </Owner>   <Models>     <Model>Generic</Model>   </Models>   <Package display_name=\"Generic\">Generic.1.0</Package>   <Attributes>     <Attribute attribute_name=\"source_name\" harmonized_name=\"source_name\" display_name=\"source name\">Lung</Attribute>     <Attribute attribute_name=\"strain\" harmonized_name=\"strain\" display_name=\"strain\">BALB/c</Attribute>     <Attribute attribute_name=\"Sex\" harmonized_name=\"sex\" display_name=\"sex\">female</Attribute>     <Attribute attribute_name=\"age\" harmonized_name=\"age\" display_name=\"age\">11 weeks-old</Attribute>     <Attribute attribute_name=\"tissue\" harmonized_name=\"tissue\" display_name=\"tissue\">Lung</Attribute>     <Attribute attribute_name=\"treatment\" harmonized_name=\"treatment\" display_name=\"treatment\">Control Medium (CM)</Attribute>     <Attribute attribute_name=\"injected cell_line\">4T1 CD90.1</Attribute>     <Attribute attribute_name=\"metastasis stage\">Colonization (d16)</Attribute>     <Attribute attribute_name=\"geo_loc_name\" harmonized_name=\"geo_loc_name\" display_name=\"geographic location\">missing</Attribute>     <Attribute attribute_name=\"collection_date\" harmonized_name=\"collection_date\" display_name=\"collection date\">missing</Attribute>   </Attributes>   <Links>     <Link type=\"url\" label=\"GEO Sample GSM7518066\">https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSM7518066</Link>     <Link type=\"entrez\" target=\"bioproject\" label=\"PRJNA988806\">988806</Link>   </Links>   <Status status=\"live\" when=\"2024-10-04T14:36:16.816\"/> </BioSample> ",
      "identifiers": "BioSample: SAMN36028300; SRA: SRS18093897; GEO: GSM7518066",
      "infraspecies": "strain: BALB/c",
      "package": "Generic",
      "sortkey": 120241004
    },
    "36028299": {
      "uid": "36028299",
      "title": "TSF1a, Lungs from TSF Injection, Metastatic Seeding (d11), Replicate a",
      "accession": "SAMN36028299",
      "date": "2024/10/04",
      "publicationdate": "2024/10/04",
      "modificationdate": "2024/10/04",
      "organization": "Laboratory of Cellular Metabolism and Metabolic Regulation, VIB-KU Leuven Center for Cancer Biology, VIB/KU Leuven",
      "taxonomy": "10090",
      "organism": "Mus musculus",
      "sourcesample": "BioSample:SAMN36028299",
      "sampledata": "<BioSample access=\"public\" publication_date=\"2024-10-04T00:00:00.000\" last_update=\"2024-10-04T14:36:16.712\" submission_date=\"2023-06-28T20:00:07.830\" id=\"36028299\" accession=\"SAMN36028299\">   <Ids>     <Id db=\"BioSample\" is_primary=\"1\">SAMN36028299</Id>     <Id db=\"SRA\">SRS18093898</Id>     <Id db=\"GEO\">GSM7518067</Id>   </Ids>   <Description>     <Title>TSF1a, Lungs from TSF Injection, Metastatic Seeding (d11), Replicate a</Title>     <Organism taxonomy_id=\"10090\" taxonomy_name=\"Mus musculus\">       <OrganismName>Mus musculus</OrganismName>     </Organism>   </Description>   <Owner>     <Name>Laboratory of Cellular Metabolism and Metabolic Regulation, VIB-KU Leuven Center for Cancer Biology, VIB/KU Leuven</Name>     <Contacts>       <Contact>         <Name>           <First>Juan</First>           <Last>Fern\u00e1ndez-Garc\u00eda</Last>         </Name>       </Contact>     </Contacts>   </Owner>   <Models>     <Model>Generic</Model>   </Models>   <Package display_name=\"Generic\">Generic.1.0</Package>   <Attributes>     <Attribute attribute_name=\"source_name\" harmonized_name=\"source_name\" display_name=\"source name\">Lung</Attribute>     <Attribute attribute_name=\"strain\" harmonized_name=\"strain\" display_name=\"strain\">BALB/c</Attribute>     <Attribute attribute_name=\"Sex\" harmonized_name=\"sex\" display_name=\"sex\">female</Attribute>     <Attribute attribute_name=\"age\" harmonized_name=\"age\" display_name=\"age\">10 weeks-old</Attribute>     <Attribute attribute_name=\"tissue\" harmonized_name=\"tissue\" display_name=\"tissue\">Lung</Attribute>     <Attribute attribute_name=\"treatment\" harmonized_name=\"treatment\" display_name=\"treatment\">Tumor-Secreted Factors (TSF)</Attribute>     <Attribute attribute_name=\"injected cell_line\">4T1 CD90.1</Attribute>     <Attribute attribute_name=\"metastasis stage\">Seeding (d11)</Attribute>     <Attribute attribute_name=\"geo_loc_name\" harmonized_name=\"geo_loc_name\" display_name=\"geographic location\">missing</Attribute>     <Attribute attribute_name=\"collection_date\" harmonized_name=\"collection_date\" display_name=\"collection date\">missing</Attribute>   </Attributes>   <Links>     <Link type=\"url\" label=\"GEO Sample GSM7518067\">https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSM7518067</Link>     <Link type=\"entrez\" target=\"bioproject\" label=\"PRJNA988806\">988806</Link>   </Links>   <Status status=\"live\" when=\"2024-10-04T14:36:16.712\"/> </BioSample> ",
      "identifiers": "BioSample: SAMN36028299; SRA: SRS18093898; GEO: GSM7518067",
      "infraspecies": "strain: BALB/c",
      "package": "Generic",
      "sortkey": 120241004
    },
    "36028298": {
      "uid": "36028298",
      "title": "TSF1b, Lungs from TSF Injection, Metastatic Seeding (d11), Replicate b",
      "accession": "SAMN36028298",
      "date": "2024/10/04",
      "publicationdate": "2024/10/04",
      "modificationdate": "2024/10/04",
      "organization": "Laboratory of Cellular Metabolism and Metabolic Regulation, VIB-KU Leuven Center for Cancer Biology, VIB/KU Leuven",
      "taxonomy": "10090",
      "organism": "Mus musculus",
      "sourcesample": "BioSample:SAMN36028298",
      "sampledata": "<BioSample access=\"public\" publication_date=\"2024-10-04T00:00:00.000\" last_update=\"2024-10-04T14:36:16.583\" submission_date=\"2023-06-28T20:00:07.706\" id=\"36028298\" accession=\"SAMN36028298\">   <Ids>     <Id db=\"BioSample\" is_primary=\"1\">SAMN36028298</Id>     <Id db=\"SRA\">SRS18093900</Id>     <Id db=\"GEO\">GSM7518068</Id>   </Ids>   <Description>     <Title>TSF1b, Lungs from TSF Injection, Metastatic Seeding (d11), Replicate b</Title>     <Organism taxonomy_id=\"10090\" taxonomy_name=\"Mus musculus\">       <OrganismName>Mus musculus</OrganismName>     </Organism>   </Description>   <Owner>     <Name>Laboratory of Cellular Metabolism and Metabolic Regulation, VIB-KU Leuven Center for Cancer Biology, VIB/KU Leuven</Name>     <Contacts>       <Contact>         <Name>           <First>Juan</First>           <Last>Fern\u00e1ndez-Garc\u00eda</Last>         </Name>       </Contact>     </Contacts>   </Owner>   <Models>     <Model>Generic</Model>   </Models>   <Package display_name=\"Generic\">Generic.1.0</Package>   <Attributes>     <Attribute attribute_name=\"source_name\" harmonized_name=\"source_name\" display_name=\"source name\">Lung</Attribute>     <Attribute attribute_name=\"strain\" harmonized_name=\"strain\" display_name=\"strain\">BALB/c</Attribute>     <Attribute attribute_name=\"Sex\" harmonized_name=\"sex\" display_name=\"sex\">female</Attribute>     <Attribute attribute_name=\"age\" harmonized_name=\"age\" display_name=\"age\">10 weeks-old</Attribute>     <Attribute attribute_name=\"tissue\" harmonized_name=\"tissue\" display_name=\"tissue\">Lung</Attribute>     <Attribute attribute_name=\"treatment\" harmonized_name=\"treatment\" display_name=\"treatment\">Tumor-Secreted Factors (TSF)</Attribute>     <Attribute attribute_name=\"injected cell_line\">4T1 CD90.1</Attribute>     <Attribute attribute_name=\"metastasis stage\">Seeding (d11)</Attribute>     <Attribute attribute_name=\"geo_loc_name\" harmonized_name=\"geo_loc_name\" display_name=\"geographic location\">missing</Attribute>     <Attribute attribute_name=\"collection_date\" harmonized_name=\"collection_date\" display_name=\"collection date\">missing</Attribute>   </Attributes>   <Links>     <Link type=\"url\" label=\"GEO Sample GSM7518068\">https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSM7518068</Link>     <Link type=\"entrez\" target=\"bioproject\" label=\"PRJNA988806\">988806</Link>   </Links>   <Status status=\"live\" when=\"2024-10-04T14:36:16.583\"/> </BioSample> ",
      "identifiers": "BioSample: SAMN36028298; SRA: SRS18093900; GEO: GSM7518068",
      "infraspecies": "strain: BALB/c",
      "package": "Generic",
      "sortkey": 120241004
    },
    "36028297": {
      "uid": "36028297",
      "title": "TSF2, Lungs from TSF Injection, Metastatic Colonization (d16)",
      "accession": "SAMN36028297",
      "date": "2024/10/04",
      "publicationdate": "2024/10/04",
      "modificationdate": "2024/10/04",
      "organization": "Laboratory of Cellular Metabolism and Metabolic Regulation, VIB-KU Leuven Center for Cancer Biology, VIB/KU Leuven",
      "taxonomy": "10090",
      "organism": "Mus musculus",
      "sourcesample": "BioSample:SAMN36028297",
      "sampledata": "<BioSample access=\"public\" publication_date=\"2024-10-04T00:00:00.000\" last_update=\"2024-10-04T14:36:16.467\" submission_date=\"2023-06-28T20:00:07.600\" id=\"36028297\" accession=\"SAMN36028297\">   <Ids>     <Id db=\"BioSample\" is_primary=\"1\">SAMN36028297</Id>     <Id db=\"SRA\">SRS18093899</Id>     <Id db=\"GEO\">GSM7518069</Id>   </Ids>   <Description>     <Title>TSF2, Lungs from TSF Injection, Metastatic Colonization (d16)</Title>     <Organism taxonomy_id=\"10090\" taxonomy_name=\"Mus musculus\">       <OrganismName>Mus musculus</OrganismName>     </Organism>   </Description>   <Owner>     <Name>Laboratory of Cellular Metabolism and Metabolic Regulation, VIB-KU Leuven Center for Cancer Biology, VIB/KU Leuven</Name>     <Contacts>       <Contact>         <Name>           <First>Juan</First>           <Last>Fern\u00e1ndez-Garc\u00eda</Last>         </Name>       </Contact>     </Contacts>   </Owner>   <Models>     <Model>Generic</Model>   </Models>   <Package display_name=\"Generic\">Generic.1.0</Package>   <Attributes>     <Attribute attribute_name=\"source_name\" harmonized_name=\"source_name\" display_name=\"source name\">Lung</Attribute>     <Attribute attribute_name=\"strain\" harmonized_name=\"strain\" display_name=\"strain\">BALB/c</Attribute>     <Attribute attribute_name=\"Sex\" harmonized_name=\"sex\" display_name=\"sex\">female</Attribute>     <Attribute attribute_name=\"age\" harmonized_name=\"age\" display_name=\"age\">11 weeks-old</Attribute>     <Attribute attribute_name=\"tissue\" harmonized_name=\"tissue\" display_name=\"tissue\">Lung</Attribute>     <Attribute attribute_name=\"treatment\" harmonized_name=\"treatment\" display_name=\"treatment\">Tumor-Secreted Factors (TSF)</Attribute>     <Attribute attribute_name=\"injected cell_line\">4T1 CD90.1</Attribute>     <Attribute attribute_name=\"metastasis stage\">Colonization (d16)</Attribute>     <Attribute attribute_name=\"geo_loc_name\" harmonized_name=\"geo_loc_name\" display_name=\"geographic location\">missing</Attribute>     <Attribute attribute_name=\"collection_date\" harmonized_name=\"collection_date\" display_name=\"collection date\">missing</Attribute>   </Attributes>   <Links>     <Link type=\"url\" label=\"GEO Sample GSM7518069\">https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSM7518069</Link>     <Link type=\"entrez\" target=\"bioproject\" label=\"PRJNA988806\">988806</Link>   </Links>   <Status status=\"live\" when=\"2024-10-04T14:36:16.467\"/> </BioSample> ",
      "identifiers": "BioSample: SAMN36028297; SRA: SRS18093899; GEO: GSM7518069",
      "infraspecies": "strain: BALB/c",
      "package": "Generic",
      "sortkey": 120241004
    }
  }
}
```