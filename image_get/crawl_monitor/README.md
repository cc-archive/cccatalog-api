The crawl monitor is responsible for dynamically adjusting rate limits and 
logging the progress of crawls. This is achieved by exchanging data with 
workers through Redis.

If the crawl monitor isn't running, workers cannot make progress.
