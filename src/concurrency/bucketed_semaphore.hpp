#ifndef CONCURRENCY_BUCKETED_SEMAPHORE_HPP_
#define CONCURRENCY_BUCKETED_SEMAPHORE_HPP_

#include "concurrency/cond_var.hpp"
#include "containers/intrusive_list.hpp"

// A bucketed semaphore obeys first-in-line/first-acquisition semantics for acquirers
// that use the same bucket.  Also, if at a given point in time there are no
// acquirers for a given bucket, the next acquirer for that bucket will never be
// blocked.
//
// This is useful when you have multiple "zones of usage" (buckets) of the same
// resource, such that each zone of usage always needs to be able to achieve
// progress, no matter what the others are doing.  Also, fortunately, for when you
// only need to maintain fifo semantics on a per-bucket basis.
//
// This type was made for the situation of caches on the same core using memory.  The
// resource is memory and the buckets are caches.  Caches must always allow at least
// *one* query to run without waiting.
//
// A worse alternative to bucketed_semaphore_t would be to have a 'force-acquire'
// option on new_semaphore_acq_t.  That would be bad because you'd give up fifo
// ordering (which would be incorrect for cache transactions) or make an arbitrarily
// long list of preceding waiters get force-acquired too (which would consume the
// resource more excessively).  The relaxed, per-bucket fifo ordering seen here
// avoids that problem.

class bucketed_semaphore_acq_t;
class bucketed_semaphore_bucket_t;

class bucketed_semaphore_t {
public:
    ~bucketed_semaphore_t();
    explicit bucketed_semaphore_t(int64_t capacity);

    int64_t capacity() const { return capacity_; }
    int64_t current() const { return current_; }

    void set_capacity(int64_t new_capacity);

private:
    void pulse_waiters();

    // Normally, current_ <= capacity_, and capacity_ doesn't change.  current_ can
    // exceed capacity_ for three reasons.
    //   1. A call to change_count could force it to overflow.
    //   2. An acquirer will never be blocked while current_ is 0.
    //   3. capacity_ could be manually adjusted by set_capacity.
    int64_t capacity_;
    int64_t current_;

    // All of these buckets have a non-zero number of waiters.  Thus we can have a
    // zillion inactive buckets but do O(1) work per acquisition.
    intrusive_list_t<bucketed_semaphore_bucket_t> buckets_;
    DISABLE_COPYING(bucketed_semaphore_t);
};

class bucketed_semaphore_bucket_t
    : public intrusive_list_node_t<bucketed_semaphore_bucket_t> {
public:
    ~bucketed_semaphore_bucket_t();
    explicit bucketed_semaphore_bucket_t(bucketed_semaphore_t *sem);
private:
    bucketed_semaphore_t *const semaphore_;
    intrusive_list_t<bucketed_semaphore_acq_t> waiters_;
    DISABLE_COPYING(bucketed_semaphore_bucket_t);
};

class bucketed_semaphore_acq_t
    : public intrusive_list_node_t<bucketed_semaphore_acq_t> {
public:
    ~bucketed_semaphore_acq_t();
    // Construction is non-blocking, it gets you in line for the semaphore.  You need
    // to call acquisition_signal()->wait() in order to wait for your acquisition of
    // the semaphore.  Acquirers receive the semaphore in the same order that they
    // "got in line" for it.
    bucketed_semaphore_acq_t();
    bucketed_semaphore_acq_t(bucketed_semaphore_bucket_t *bucket, int64_t count);
    bucketed_semaphore_acq_t(bucketed_semaphore_acq_t &&movee);

    int64_t count() const;

private:
    // The bucket whose semaphore this acquires (or NULL if this hasn't begun
    // acquiring a semaphore yet).
    bucketed_semaphore_bucket_t *bucket_;

    // The count of "how much" of the semaphore we've acquired (if bucket_ is
    // non-null).
    int64_t count_;

    // Gets pulsed when we have successfully acquired the semaphore.
    cond_t cond_;
    DISABLE_COPYING(bucketed_semaphore_acq_t);
};


#endif  // CONCURRENCY_BUCKETED_SEMAPHORE_HPP_
