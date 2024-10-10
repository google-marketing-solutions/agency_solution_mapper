# Agency Solution Mapper

A tool to help agencies select the best gTech solutions for their specific cohort of advertisers.

## Goal

We have so much data... we should use it... to make decisions... that help us land projects... by aligning more closely with where OpCos and their advertisers are.

This is important because the impact is measured at the advertiser level but work, especially CSE work, is done at the OpCo/HoldCo levels.  We hope to align on both opportunities.

## Roadmap
   * DV360 - 100% implemented, this is our proof of concept.
   * CM360 - Low hanging opportunity to replicate given ease of access.
   * SA360 - Data restrictions based on team access.
   * Google Ads - Data Restrictions based on team access.

## Methodologies
1. Create the ability to compare Advertisers and OpCos against solutions, needs, offerings.
1. Dive deeper than spend and impressions, actually look at what advertisers are running.
1. Use the following normalization techniques (all are floats 0-1 or 0%-100%)
   1. average - the average of a value across advertisers.
   1. percent - when options are bound, for example enums.
   1. rank - useful for counts of unbound things, relative to the max.
   1. share - compared relative to total among peers.
1. We do this BigQuery because it has nesting and pivots.

## Process

### Load DV360 settings and rank attributes.
This uses the DV360 APIs to loaf the attributes, spend, and impressions.

```
python bqflow/run.py dv360_warehouse.json -u user.json -p gcp_project -v
python bqflow/run.py dv360_attributes_advertisers.json -u user.json -p gcp_project -v
python bqflow/run.py dv360_attributes_campaigns.json -u user.json -p gcp_project -v
python bqflow/run.py dv360_attributes_creatives.json -u user.json -p gcp_project -v
python bqflow/run.py dv360_attributes_lineitems.json -u user.json -p gcp_project -v
python bqflow/run.py dv360_attributes_partners.json -u user.json -p gcp_project -v
python bqflow/run.py dv360_attributes_score.json -u user.json -p gcp_project -v
python bqflow/run.py dv360_spend.json -u user.json -p gcp_project -v
```

### Load the solution definitions and compute the scores. 
This will also create the **Solution Advertiser Mapping** table to connect to the [Dashboard](https://lookerstudio.google.com/c/reporting/d2ce2f78-1cab-4634-af4c-f04f1058ad0c/).


```
python bqflow/bq.py -dataset mapper_dv360 -u user.json -p gcp_project -table Solutions -from_json solutions.json -from_schema schema.json 
python bqflow/run.py dv360_mapping.json -u user.json -p gcp_project -v
```

### Some Helpers
These are helper tasks that can be used to export attribute lists and dump the dashboard contents into a CSV file.

```
python bqflow/run.py dv360_attributes_lookup.json -u user.json -p gcp_project -v
python bqflow/run.py dv360_export.json -u user.json -p gcp_project -v
python bqflow/run.py SHEET_solutions.json -u user.json -p gcp_project -v
```

## Dashboard

The [Dashboard](https://lookerstudio.google.com/c/reporting/d2ce2f78-1cab-4634-af4c-f04f1058ad0c/) provides scores at for three dimensions:

1. **Solution** - easily rank solutions against each other across the entire advertiser cohort.
1. **Partner** - make decisions for holistic partner wide solutions.
1. **Advertiser** - a specific lead list of where to apply each solution once the agency and partner scope has been identified.

# Requirements
1. [Google Cloud Project](https://cloud.google.com)
1. [BQFlow](https://github.com/google-marketing-solutions/bqflow)

# License

Copyright 2024 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

# Disclaimer

This is NOT an official Google product.
