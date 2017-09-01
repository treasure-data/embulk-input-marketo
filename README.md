[![Build Status](https://travis-ci.org/treasure-data/embulk-input-marketo.svg?branch=master)](https://travis-ci.org/treasure-data/embulk-input-marketo)
[![Code Climate](https://codeclimate.com/github/treasure-data/embulk-input-marketo/badges/gpa.svg)](https://codeclimate.com/github/treasure-data/embulk-input-marketo)
[![Test Coverage](https://codeclimate.com/github/treasure-data/embulk-input-marketo/badges/coverage.svg)](https://codeclimate.com/github/treasure-data/embulk-input-marketo/coverage)
[![Gem Version](https://badge.fury.io/rb/embulk-input-marketo.svg)](http://badge.fury.io/rb/embulk-input-marketo)

# Marketo input plugin for Embulk

embulk-input-marketo is the gem preparing Embulk input plugins for [Marketo](http://www.marketo.com/).

- Lead
- Activity log

This plugin uses Marketo SOAP API.

## Overview

Required Embulk version >= 0.8.7 (since 0.5.0).

* **Plugin type**: input
* **Resume supported**: no for `marketo_lead`, yes for `marketo_activity_log`
* **Cleanup supported**: no
* **Guess supported**: yes

## Install

```
$ embulk gem install embulk-input-marketo
```

## Configuration

### API

Below parameters are shown in "Admin" > "Web Services" page in Marketo.

### marketo_lead

- **endpoint** SOAP endpoint URL for your account (string, required)
- **wsdl** SOAP endpoint URL for your account (string, default: endpoint + "?WSDL")
- **user_id** Your user id (string, reqiured)
- **encryption_key** Your encryption key (string, reqiured)
- **from_datetime** Fetch leads since this time (string, required)
- **to_datetime** Fetch leads until this time (string, default: Time.now)
- **retry_initial_wait_sec** Wait seconds for exponential backoff initial value (integer, default: 1)
- **retry_limit**: Try to retry this times (integer, default: 5)
- **append_processed_time_column**: If you want the column for processed time (boolean, default: true)

### marketo_activity_log

- **endpoint** SOAP endpoint URL for your account (string, required)
- **wsdl** SOAP endpoint URL for your account (string, default: endpoint + "?WSDL")(Note "?WSDL" needs to be in all caps)
- **user_id** Your user id (string, reqiured)
- **encryption_key** Your encryption key (string, reqiured)
- **from_datetime** Fetch activity_logs since this time (string, required)
- **to_datetime** Fetch activity_logs until this time (string, default: Time.now)
- **retry_initial_wait_sec** Wait seconds for exponential backoff initial value (integer, default: 1)
- **retry_limit**: Try to retry this times (integer, default: 5)

### Selecting plugin type

You should specify `type: marketo_lead` or `type: marketo_activity_log` on your demand.


## Example

For lead, you have `partial-config.yml` like below:

```yaml
in:
  type: marketo_lead
  endpoint: https://soap-end-point.mktoapi.com/
  wsdl: https://wsdl-url.mktoapi.com/?WSDL
  user_id: user_ABC123
  encryption_key: TOPSECRET
  from_datetime: "2015-06-30"
out:
  type: stdout
```

You can run `embulk guess partial-config.yml -o lead-config.yml` and got `lead-config.yml`. `lead-config.yml` includes a schema for Lead.

Next, you can run `embulk preview lead-config.yml` for preview and `embulk run lead-config.yml` for run.

