# Lkqd input plugin for Embulk

Loads reporting data from LKQD API.

## Overview

* **Plugin type**: input
* **Resume supported**: no
* **Cleanup supported**: no
* **Guess supported**: yes

## Configuration

- **secret_key_id**: API Secret Key ID (required string, required)
- **secret_key**: API Secret Key (required string, required)
- **endpoint**: API endpoint (optional string, default: `'https://api.lkqd.com/reports'`)
- **report_parameters**: Report parameters documented in https://wiki.lkqd.com/display/API/LKQD+API (required hash, default: `{}`)
- **measurable_impressions**: calculate and inject `Measurable Impressions` column, required `"IMPRESSIONS"`, `"VIEWABILITY_MEASURED_RATE"` specified in "metrics" (optional boolean, default: false)
- **viewable_impressions**: calculate and inject `Viewable Impressions` column, required `"IMPRESSIONS"`, `"VIEWABILITY_MEASURED_RATE"`, `"VIEWABILITY_RATE"` specified in "metrics" (optional boolean, default: false)

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
