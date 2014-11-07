#include "concurrency/bucketed_semaphore.hpp"

bucketed_semaphore_t::bucketed_semaphore_t(int64_t capacity)
    : capacity_(capacity), current_(0) {
    guarantee(capacity > 0);
}

bucketed_semaphore_t::~bucketed_semaphore_t() {
    guarantee(current_ == 0);
    guarantee(buckets_.empty());
}

void bucketed_semaphore_t::set_capacity(int64_t new_capacity) {
    guarantee(new_capacity > 0);
    capacity_ = new_capacity;
    pulse_waiters();
}

