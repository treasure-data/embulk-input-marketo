[![Build Status](https://travis-ci.org/treasure-data/embulk-input-marketo.svg?branch=master)](https://travis-ci.org/treasure-data/embulk-input-marketo)

[![Code Climate](https://codeclimate.com/github/treasure-data/embulk-input-marketo/badges/gpa.svg)](https://codeclimate.com/github/treasure-data/embulk-input-marketo)

[![Test Coverage](https://codeclimate.com/github/treasure-data/embulk-input-marketo/badges/coverage.svg)](https://codeclimate.com/github/treasure-data/embulk-input-marketo/coverage)

# Marketo input plugin for Embulk

embulk-input-marketo is the gem preparing Embulk input plugins for [Marketo](http://www.marketo.com/).

- Lead
- Activity log

This plugin uses Marketo SOAP API.

## Overview

Required Embulk version >= 0.6.13.

* **Plugin type**: input
* **Resume supported**: no
* **Cleanup supported**: no
* **Guess supported**: yes

## Install

```
$ embulk gem install embulk-input-marketo
```

## Configuration

### API

Below parameters are shown in "Admin" > "Web Services" page in Marketo.

- **endpoint** SOAP endpoint URL for your account (string, required)
- **wsdl** SOAP endpoint URL for your account (string, default: endpoint + "?WSDL")
- **user_id** Your user id (string, reqiured)
- **encryption_key** Your encryption key (string, reqiured)
- **last_updated_at** Limit datetime that a lead has been updated (this plugin fetches leads updated after this datetime) (string, required)

### Selecting plugin type

You should specify the plugin type as `marketo/type` for `type` configuration.

If you want to Lead plugin, you should specify "marketo/lead", or If you want to Activity log plugin, you should specify "marketo/activity_log".


## Example

For lead, you have `partial-config.yml` like below:

```yaml
in:
  type: marketo/lead
  endpoint: https://soap-end-point.mktoapi.com/
  wsdl: https://wsdl-url.mktoapi.com/?WSDL
  user_id: user_ABC123
  encryption_key: TOPSECRET
  last_updated_at: "2015-06-30"
out:
  type: stdout
```

You can run `embulk guess partial-config.yml -o lead-config.yml` and got `lead-config.yml`. `lead-config.yml` includes a schema for Lead.

Next, you can run `embulk preview lead-config.yml` for preview and `embulk run lead-config.yml` for run.

