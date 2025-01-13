========================
Rate limiting strategies
========================


Fixed Window
============

This is the most memory efficient strategy to use as it maintains one counter
per resource and rate limit. It does however have its drawbacks as it allows
bursts within each window - thus allowing an 'attacker' to by-pass the limits.
The effects of these bursts can be partially circumvented by enforcing multiple
granularities of windows per resource.

For example, if you specify a ``100/minute`` rate limit on a route, this strategy will
allow 100 hits in the last second of one window and a 100 more in the first
second of the next window. To ensure that such bursts are managed, you could add a second rate limit
of ``2/second`` on the same route.


Fixed Window with Elastic Expiry
================================

This strategy works almost identically to the Fixed Window strategy with the exception
that each hit results in the extension of the window. This strategy works well for
creating large penalties for breaching a rate limit.

For example, if you specify a ``100/minute`` rate limit on a route and it is being
attacked at the rate of 5 hits per second for 2 minutes - the attacker will be locked
out of the resource for an extra 60 seconds after the last hit. This strategy helps
circumvent bursts.


Moving Window
=============

.. warning:: The moving window strategy is not implemented for the ``memcached``
    and ``etcd`` storage backends.

This strategy is the most effective for preventing bursts from by-passing the
rate limit as the window for each limit is not fixed at the start and end of each time unit
(i.e. N/second for a moving window means N in the last 1000 milliseconds). There is
however a higher memory cost associated with this strategy as it requires ``N`` items to
be maintained in memory per resource and rate limit.


Sliding Window Counter
======================

.. warning:: The sliding window counter strategy is not implemented for the ``mongodb``
    storage backend.

This strategy is an approximation of the moving window strategy, with less memory use.
It approximates the behavior of a moving window by maintaining counters for two adjacent
fixed windows: the current window, and the previous window.

At the first hit, the current window counter increases and the sampling period begins. Then,
at the end of the sampling period, the window counter and expiration are moved to the previous window,
and new requests will still increase the current window counter.

To determine if a request should be allowed, we assume the requests in the previous window
were distributed evenly over its duration (eg: if it received 5 requests in a period of 10 seconds, 
we consider it has received one request every two seconds).

Depending on how much time has elapsed since the current window was moved, a weight is applied:

weighted_count =  previous_count * (expiration_period - time_elapsed_since_shift) / expiration_period + current_count

For example, for a sampling period of 10 seconds and if the window has shifted 2 seconds ago,
the :

weighted_count = previous_count * (10 - 2) / 10 + current_count

Contrary to the moving window strategy, at most two counters per rate limiter are needed,
which dramatically reduce the memory usage.

Limitation: with a very low sampling period and limit (e.g., '1 per 1 second'),
the rate limiter may trigger prematurely because even slight fluctuations in request timing
can disproportionately affect the weighted count.