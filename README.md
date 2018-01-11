Elasticsearch Drift Plugin
==========================

Time series aggregation for flow records.

|   Drift Plugin  | Elasticsearch     | Release date |
|-----------------|-------------------|:------------:|
| 1.0.0           | 6.1.1             |  TBD         |


Usage
-----

```json
{
        "size": 0,
        "query": {
                "bool": {
                        "filter": [
                        {
                                "range": {
                                        "netflow.last_switched": {
                                                "gte": 1515369000000,
                                                "format": "epoch_millis"
                                        }
                                }
                        }]
                }
        },
        "aggs": {
                "bytes_over_time": {
                        "proportional_sum": {
                                "fields": ["netflow.first_switched", "netflow.last_switched", "netflow.bytes"],
                                "interval": "60m",
                                "start": 1515369600000,
                                "end": 1515456000019
                        }
                },
               "bytes_total" : { "sum" : { "field" : "netflow.bytes" } }
        }
}
```

