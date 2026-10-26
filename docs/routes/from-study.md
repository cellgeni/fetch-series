---
title: Promoted routes from Study
---

# Promoted routes from Study

Catalogue links from an INSDC study that were surveyed **as routes**: each at `sample:3000` of its
source's population (or the whole of it, where smaller), gated first on `probe-families` and
`hard-cases`. A route here makes the catalogue mechanism's own request and reads it with the
link survey's own extraction (`routes.catalogue`). The table is generated at build time from
`graph.PROMOTED`. Within each direction, routes are listed in the order `ranked()` tries them,
from the evidence and never by hand ([how routes are ranked](../decisions/route-ranking.md)).

<!-- promoted-routes:study -->
