# Marketo input plugin for Embulk (through proxy)

* This plugin is an extension of [embulk-input-marketo](https://rubygems.org/gems/embulk-input-marketo) that allows you to access Marketo API through a proxy server.

embulk-input-marketo is the gem preparing Embulk input plugins for [Marketo](http://www.marketo.com/).

- Lead(lead)
- Activity log(activity)
- Lead by list(all_lead_with_list_id)
- Lead by program(all_lead_with_program_id)
- Campaign(campaign)
- Assets Programs (program)

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
| **partner_api_key**              | false    |               | Set Marketo Partner API Key see: http://developers.marketo.com/support/Marketo_LaunchPoint_Technology_Partner_API_Key.pdf        |

### Bulk extract target configuration parameter (Lead and Activity)

All bulk extract target use this configuration parameter

| name                        | required | default value | description                                                                                                                   |
|-----------------------------|----------|---------------|-------------------------------------------------------------------------------------------------------------------------------|
| **from_date**               | true     |               | Import data since this date. Example: 2017-10-11T06:43:24+00:00                                                               |
| **fetch_days**              | false    | 1             | Amount of days to fetch since from_date                                                                                       |
| **polling_interval_second** | false    | 60            | Amount of time to wait between pooling job status in second                                                                   |
| **bulk_job_timeout_second** | false    | 3600          | Amount of time to wait for bulk job to complete in second                                                                     |
| **incremental**             | false    | true          | If incremental is set to true, next run will have from_date set to the previous to_date(calculated by from_date + fetch_days) |
| **incremental_column**      | false    | createdAt     | Column use to filter from_date and to_date                                                                                    |

### Lead

Lead target extract all Marketo leads, it use Marketo bulk extract feature. Configuration include bulk extract configuration. 

`target: lead` 

Configuration:

| name                | required | default value | description                                                                                                                                               |
|---------------------|----------|---------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------|
| **use_updated_at**  | false    | false         | Support filter with `updateAt` column, but not all Marketo Account have the feature to filter by updatedAt, updatedAt don't support incremental ingestion |
| **included_fields** | false    | null         | List of lead fields to included in export request sent to Marketo, can be used to reduce the size of BulkExtract file                                     |

Schema type: Dynamic via describe lead endpoint.

Incremental support: yes

Range ingestion: yes

### Activity

Activity target extract all Marketo  activity log. Configuration include all bulk extract configuration

`target: activity`

Schema type: Static schema

Incremental support: yes

Range ingestion: yes

Filter by specific activity type ids: yes. See [#95](https://github.com/treasure-data/embulk-input-marketo/issues/95)

### Campaign

Campaign extract all campaign data from Marketo

`target: campaign`

Schema type: Static schema

Incremental support: no

Range ingestion: no

### Lead by list

Extract all Lead data including lead's list id

`target: all_lead_with_list_id`

Configuration:

| name                | required | default value | description                                                                                                     |
|---------------------|----------|---------------|-----------------------------------------------------------------------------------------------------------------|
| **included_fields** | false    | null          | List of lead fields to included in export request sent to Marketo, can be used to reduce request, response size |
| **list_ids**        | false    | null          | Import Leads by specified Lists_ID. If not specified will import all Leads by all List IDs                      |

Schema type: Dynamic via describe leads. Schema will have 1 addition column name listId that contain the id of the list the lead belong to

Incremental support: no

Range ingestion: no

### Lead by program

Extract all Lead data including lead's program id

`target: all_lead_with_program_id`

Configuration:

| name                | required | default value | description                                                                                                           |
|---------------------|----------|---------------|-----------------------------------------------------------------------------------------------------------------------|
| **included_fields** | false    | null          | List of lead fields to included in export request sent to Marketo, can be used to reduce request, response size       |
| **program_ids**     | false    | null          | Import Members by specified Program_ID (comma-separated). If not specified will import all Members by all Program IDs |

Schema type: Dynamic via describe leads. Schema will have 1 addition column name listId that contain the id of the list the lead belong to

Incremental support: no

Range ingestion: no

### Assets programs

Get Assets Programs by Query Tag type, Date range or all if no query by specified.

`target: program`

Configuration:

| name                        | required | default value | description                                                                                                                                                  |
|-----------------------------|----------|---------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **query_by**                | false    |   null        | Get assets programs by query, supported values `date_range`, `tag_type` leave unset to fetch all programs                                                    |
| **earliest_updated_at**     | false    |   null        | Required if query by `date_range` is selected. Exclude programs prior to this date. Must be valid ISO-8601 string                                            |
| **latest_updated_at**       | false    |   null        | Required if query by `date_range` is selected. Exclude programs after this date. Must be valid ISO-8601 string                                               |
| **filter_type**             | false    |   null        | Optional value send with query by `date_range` is selected to filter out the result from Marketo. Supported values `id`, `programId`, `folderId`, `workspace`|
| **filter_values**           | false    |   null        | Set the values associated with `filter_type`                                                                                                                 |
| **tag_type**                | false    |   null        | Required if query by `tag_type` is selected. Type of program tag                                                                                             |
| **tag_value**               | false    |   null        | Required if query by `tag_type` is selected. Value of the tag                                                                                                |
| **report_duration**         | false    |   null        | Amount of milliseconds to fetch from `earliest_updated_at`. If `incremental = true` this value will automatically calculated for the first run by `latest_updated_at` - `earliest_updated_at` |
| **incremental**             | false    |   true        | If incremental is set to true, next run will have `earliest_updated_at` set to the previous `latest_updated_at` + `report_duration`. Incremental import only support by query `date_range`     |

Schema type: Static schema

Incremental support: yes (Query by `date_range` only)

Range ingestion: yes

`target: all_lead_with_program_id`

Configuration:

| name                                  | required | default value | description                                                                                                           |
|---------------------------------------|----------|---------------|-----------------------------------------------------------------------------------------------------------------------|
| **custom_object_api_name**            | true     | null          | The API name of the custom object                                                                                     |
| **custom_object_fields**              | false    | null          | Comma separated API name of fields of the custom object (Optional)                                                    |
| **custom_object_filter_type**         | true     | null          | Field to search on Valid values are: dedupeFields, idFields, and any field defined in searchableFields attribute of Describe endpoint. Default is dedupeFields |
| **custom_object_filter_values**       | false    | null          | Comma-separated list of field values to match.                                                                        |
| **custom_object_filter_from_value**   | false    | null          | Filter Marketo Custom Object has value greater than this value                                                        |
| **custom_object_filter_to_value**     | false    | null          | Filter Marketo Custom Object has value smaller than this value. If not set, only records that have value greater than "From Value" will be returned. Job will stop if no record found in 300 consecutive value. |

Schema type: dynamic schema
Incremental support: no 

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

Example of Assets Programs config
```yaml
in:
  account_id: ACCOUNT_ID
  client_id: CLIENT_ID
  client_secret: CLIENT_SECRET
  target: program
  type: marketo
  query_by: date_range
  filter_type: folderId
  filter_values: 
   - 2598
   - 1001
  earliest_updated_at: 2018-08-20T00:00:00.000Z
  latest_updated_at: 2018-08-31T00:00:00.000Z
  incremental: true
  ```

If you need to access the Marketo API through a proxy, set the environment variable.

```
export embulk_proxy_host=<Your-Proxy-Host>
export embulk_proxy_port=<Your-Proxy-Port>
embulk run config.yml
```