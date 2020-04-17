# Overview
## What is this?

A distributed solution for retrieving images from a list of URLs fed to a 
message queue. Async workers consume messages from this queue and 
store the images at the configured resolution in S3. The workers also scrape
metadata, including image resolution and EXIF tags, and publishes the 
results back to the queue.

Performance is horizontally scalable; workers can be run in any number of 
processes on any number of machines. Be sure to adjust the Kafka topic partition
count to at least the total number of worker processes across all machines.

## How do I start it?
`docker-compose up --build`

## How do I run the tests?
```
cd worker
pipenv install
pipenv shell
PYTHONPATH=. pytest
```
Use `pytest -s` to include debug logs.

## How do I feed images to it?
See `dummy_producer.py` for an example.

If you are running `docker-compose`, you must run the producer from within 
the docker-compose network. Enter the worker container and run it from
there.


```
docker exec -it image_get_worker_1 /bin/bash
pipenv run python dummy_producer.py
```

# Input and output topics
Input and output into the cluster is controlled through message topics. 

## Input topics

### `inbound_images` topic

`inbound_images` is the point of entry for scheduling image downloads.

For rate limiting and safety purposes, *sources that do not appear in https://api.creativecommons.engineering/v1/sources will not be crawled.*

The cluster expects a JSON message with the following structure:
```
{
    'url': 'https://example.gov/example.jpg',
    'uuid': '7563efd4-58d0-41eb-9a4f-3903d36a5225',
    'source': 'example'
}
```

*url*: The URL of the image

*uuid*: Our unique identifier for an image.

*source*: The source of an image (in our case, this should match the `provider` field in our data schema). This is used to determine what rate limit policy should apply to the URL.

## Output topics

### `resolution_updates`

The `resolution_updates` topic contains resolution metadata discovered from crawled images.

```
{
    "height": 1024,
    "width": 768,
    "identifier": "7563efd4-58d0-41eb-9a4f-3903d36a5225"
}
```

### `exif_updates` topic

The `exif_updates` topic contains EXIF metadata discovered from crawled images.

# Monitoring the crawl

The `crawl_monitor` logs useful information about the crawl in a machine-friendly format. There are several different types of events that will appear in the logger.

## `monitoring_update`
`monitoring_update` is the most common event, and will appear every 5 seconds.
```
{
   "event" : "monitoring_update",
   "time" : "2020-04-17T20:22:56.837232",
   "general" : {
      "global_max_rps" : 193.418869804698,
      "error_rps" : 0,
      "processing_rate" : 0,
      "success_rps" : 0,
      "circuit_breaker_tripped" : [],
      "num_resized" : 13224,
      "resize_errors" : 0,
      "split_rate" : 0
   },
   "specific" : {
      "flickr" : {
         "successful" : 13188,
         "last_50_statuses" : {
            "200" : 50
         },
         "rate_limit" : 178.375147633876,
         "error" : 0
      },
      "animaldiversity" : {
         "last_50_statuses" : {
            "200" : 18
         },
         "successful" : 18,
         "error" : 0,
         "rate_limit" : 0.206215440554406
      },
      "phylopic" : {
         "rate_limit" : 0.2,
         "error" : 0,
         "successful" : 18,
         "last_50_statuses" : {
            "200" : 18
         }
      }
   }
}
```

Desciption of non-obvious keys:

General statistics give the operator an idea of how crawling is progressing globally across all workers and domains.

*global_max_rps* is the theoretical maximum throughput of the cluster within scheduled rate limits.

*processing_rate* is the rate of completing image processing tasks.

*error_rps* is the rate of HTTP errors occurring over the last monitoring interval.

*circuit_breaker_tripped* lists sources that the cluster has stopped crawling due to a surge in errors. See the "Configuration and Operation" section for more details.

*num_resized* is the total number of successfully resized images.

*split_rate* is the rate that the `inbound_urls` topic is being split into separate topics for scheduling. See the "Architecture" section for more details. This statistic is only updated once for every 1000 URLs inserted.

The specific statistics are statistics that are specific to a given source; the key names are generally self-explanatory.

## `crawl_halted`

`crawl_halted` events indicate that crawling has stopped, temporarily or permanently, for a single source. Temporary halts are resolved automatically, while permanent halts require intervention from an operator. See "The error circuit breaker" section for details.

An example `crawl_halted` message:
```
{
   "time" : "2020-04-17T16:57:52.135155",
   "type" : "temporary",
   "event" : "crawl_halted",
   "msg" : "example tripped temporary halt. Response codes: {\"b'500\": 11}",
   "source" : "example"
}
```

# Configuration and Operation

Configuring the crawl can be achieved by setting the corresponding keys in Redis.

## Overriding automatically computed rate limits

Rate limits are set in proportion to the number of artifacts we are crawling from a given source. The logic behind this is that sites with more images have more infrastructure in place for serving high traffic. If the automatic crawl rate isn't satisfactory for whatever reason, it can be manually overridden.

```
redis-cli
> set override-rate:example 10
# Sets rate limit for `example` to 10 requests per second
```

## The error circuit breaker

The crawler is designed to operate with minimal manual intervention; if everything has been set up properly, inserting URLs into the `inbound_urls` queue should kick off crawling. In situations where there are brief upticks in errors, crawling will be temporarily halted for 60 seconds before resuming automatically.

One exception where operators must intervene is if a source trips the error circuit breaker. If any source has 50 failed requests in a row, all crawling for that source will be stopped automatically. To resume crawling, the operator should inspect the `crawl_monitor` logs to find the nature of the error message, manually lower the rate limit for the source if necessary, and reset the circuit breaker using the Redis client.

For instance, if the source "example" has tripped the circuit breaker, this is the procedure for resuming crawling:
```
redis-cli
# View which sources have tripped the circuit breaker
> smembers halted
example
# Manually override rate limit to 5 requests per second
> set override_rate:example 5
# Clear error window for the source
> del statuslast50req:example
# Remove the source from the halted set
> srem halted example
```

# Technical Architecture
