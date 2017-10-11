[![Build Status](https://travis-ci.org/treasure-data/embulk-input-marketo.svg?branch=master)](https://travis-ci.org/treasure-data/embulk-input-marketo)
[![Code Climate](https://codeclimate.com/github/treasure-data/embulk-input-marketo/badges/gpa.svg)](https://codeclimate.com/github/treasure-data/embulk-input-marketo)
[![Test Coverage](https://codeclimate.com/github/treasure-data/embulk-input-marketo/badges/coverage.svg)](https://codeclimate.com/github/treasure-data/embulk-input-marketo/coverage)
[![Gem Version](https://badge.fury.io/rb/embulk-input-marketo.svg)](http://badge.fury.io/rb/embulk-input-marketo)

# Marketo input plugin for Embulk

embulk-input-marketo is the gem preparing Embulk input plugins for [Marketo](http://www.marketo.com/).

- Lead(lead)
- Activity log(activity)
- Lead by list(all_lead_with_list_id)
- Lead by program(all_lead_with_program_id)
- Campaign(campaign)

This plugin uses Marketo REST API.

## Overview

Required Embulk version >= 0.8.33 (since 0.6.0).

* **Plugin type**: input
* **Resume supported**: no
* **Cleanup supported**: no
* **Guess supported**: no

## Install

```
$ embulk gem install embulk-input-marketo
```

## Configuration

### API

Below parameters are shown in "Admin" > "Web Services" page in Marketo.

### Base configuration parameter

All target have this configuration parameters

| name                             | required | default value | description                                                                                                                      |
|----------------------------------|----------|---------------|----------------------------------------------------------------------------------------------------------------------------------|
| **target**                       | true     |               | Marketo targets                                                                                                                  |
| **account_id**                   | true     |               | Marketo Muchkin id                                                                                                               |
| **client_id**                    | true     |               | Marketo REST client id                                                                                                           |
| **client_secret**                | true     |               | Marketo REST client secret                                                                                                       |
| **marketo_limit_interval_milis** | false    | 20            | Marketo have limitation of 100 calls per 20 second. If REST API calls are failed they will wait this amount of time before retry |
| **batch_size**                   | false    | 300           | Token paging batch size. Some REST API support batch                                                                             |
| **max_return**                   | false    | 200           | Max return for Endpoint that use offset paging                                                                                   |

### Bulk extract target configuration parameter (Lead and Activity)

All bulk extract target use this configuration parameter

| name                        | required | default value | description                                                                                                                   |
|-----------------------------|----------|---------------|-------------------------------------------------------------------------------------------------------------------------------|
| **from_date**               | true     |               | Import data since this date. Example: 2017-10-11T06:43:24+00:00                                                               |
| **fetch_days**              | false    | 1             | Ammount of days to fetch since from_date                                                                                      |
| **polling_interval_second** | false    | 60            | Amount of time to wait between pooling job status in second                                                                   |
| **bulk_job_timeout_second** | false    | 3600          | Amount of time to wait for bulk job to complete in second                                                                     |
| **incremental**             | false    | true          | If incremental is set to true, next run will have from_date set to the previous to_date(calculated by from_date + fetch_days) |
| **incremental_column**      | false    | createdAt     | Column use to filter from_date and to_date                                                                                    |

### Lead

Lead target extract all Marketo leads, it use Marketo bulk extract feature. Configuration include bulk extract configuration. 

`target: lead` 

Configuration:

| name               | required | default value | description                                                                                                                                                      |
|--------------------|----------|---------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **use_updated_at** | false    | false         | Lead data are not immutable so it better to do incremental ingesting with `updateAt` column, but not all Marketo Account have the feature to filter by updatedAt |

Schema type: Dynamic via describe lead endpoint.

Incremental support: yes

Range ingestion: yes

### Activity

Activity target extract all Marketo  actvity log. Configuration include all bulk extract configuraiont

`target: activity`

Schema type: Static schema

Incremental support: yes

Range ingestion: yes

### Campaign

Campaign extract all campaign data from Marketo

`target: campaign`

Schema type: Static schema

Incremental support: no

Range ingestion: no

### Lead by list

Extract all Lead data including lead's list id

`target: all_lead_with_list_id`

Schema type: Dynamic via describe leads. Schema will have 1 addition column name listId that contain the id of the list the lead belong to

Incremental support: no

Range ingestion: no

### Laed by program

Extract all Lead data including lead's program id

`target: all_lead_with_program_id`

Schema type: Dynamic via describe leads. Schema will have 1 addition column name listId that contain the id of the list the lead belong to

Incremental support: no

Range ingestion: no

## Example

For lead, you have `partial-config.yml` like below:

```yaml
in:
  type: marketo
  target: lead
  account_id: ACCOUNT_ID
  client_id: CLIENT_ID
  client_secret: CLIENT_SECRET
  from_date: 2017-09-01
  fetch_days: 1
out:
  type: stdout
```

You can run `embulk guess partial-config.yml -o lead-config.yml` and got `lead-config.yml`. `lead-config.yml` includes a schema for Lead.

Next, you can run `embulk preview lead-config.yml` for preview and `embulk run lead-config.yml` for run.

