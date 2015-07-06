[![Build Status](https://travis-ci.org/treasure-data/embulk-input-marketo.svg?branch=master)](https://travis-ci.org/treasure-data/embulk-input-marketo)

[![Code Climate](https://codeclimate.com/github/treasure-data/embulk-input-marketo/badges/gpa.svg)](https://codeclimate.com/github/treasure-data/embulk-input-marketo)

[![Test Coverage](https://codeclimate.com/github/treasure-data/embulk-input-marketo/badges/coverage.svg)](https://codeclimate.com/github/treasure-data/embulk-input-marketo/coverage)

# Marketo input plugin for Embulk

embulk-input-marketo is the Embulk input plugin for [Marketo](http://www.marketo.com/).
This plugin uses Marketo SOAP API.

## Overview

Required Embulk version >= 0.6.13.

* **Plugin type**: input
* **Resume supported**: no
* **Cleanup supported**: no
* **Guess supported**: yes

## Configuration

Below parameters are shown in "Admin" > "Web Services" page in Marketo.

- **endpoint** SOAP endpoint URL for your account (string, required)
- **wsdl** SOAP endpoint URL for your account (string, default: endpoint + "?WSDL")
- **user_id** Your user id (string, reqiured)
- **encryption_key** Your encryption key (string, reqiured)
- **last_updated_at** Target datetime when a lead is updated at (string, required)

## Example

```yaml
in:
  type: marketo
  endpoint: https://soap-end-point.mktoapi.com/
  wsdl: https://wsdl-url.mktoapi.com/?WSDL
  user_id: user_ABC123
  encryption_key: TOPSECRET
  last_updated_at: "2015-06-30"
```


## Build

```
$ rake
```
