# Lkqd input plugin for Embulk

Loads reporting data from LKQD API.

## Overview

* **Plugin type**: input
* **Resume supported**: no
* **Cleanup supported**: no
* **Guess supported**: yes

## Configuration

- **secret_key_id**: API Secret Key ID (string, required)
- **secret_key**: API Secret Key (string, required)
- **endpoint**: API endpoint (string, default: `'https://api.lkqd.com/reports'`)
- **report_parameters**: Report parameters documented in https://wiki.lkqd.com/display/API/LKQD+API (hash, default: `{}`)

## Example

```yaml
in:
  type: lkqd
  secret_key_id: 
  secret_key: 
  report_parameters:
    timeDimension: "HOURLY"
    reportType: [""]
    reportFormat: "CSV"
    startDate: "2017-08-01"
    endDate: "2017-08-02"
    startHour: 0
    endHour: 23
    timezone: "UTC"
    metrics: [""]
```


## Build

```
$ rake
```
