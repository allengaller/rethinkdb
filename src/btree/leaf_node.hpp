// Copyright 2010-2014 RethinkDB, all rights reserved.
#ifndef BTREE_LEAF_NODE_HPP_
#define BTREE_LEAF_NODE_HPP_

#include <string>
#include <utility>
#include <vector>

#include "buffer_cache/types.hpp"
#include "errors.hpp"

class value_sizer_t;
struct btree_key_t;
class repli_timestamp_t;
struct store_key_t;


struct leaf_node_t;

namespace leaf_impl {

// We must maintain timestamps and deletion entries as best we can,
// with the following limitations.  The number of timestamps stored
// need not be more than the most `MANDATORY_TIMESTAMPS` recent
// timestamps.  The deletions stored need not be any more than what is
// necessary to fill `(block_size - offsetof(leaf_node_t,
// pair_offsets)) / DELETION_RESERVE_FRACTION` bytes.  For example,
// with a 4084 block size, if the five most recent operations were
// deletions of 250-byte keys, we would only be required to store the
// 2 most recent deletions and the 2 most recent timestamps.
//
// These parameters are in the header because some unit tests are
// based on them.
static const int MANDATORY_TIMESTAMPS = 5;
static const int DELETION_RESERVE_FRACTION = 10;

}  // namespace leaf_internal

// RSI: Remove this and have main btree and sindex btree types.
struct orig_btree_t;

template <class btree_type>
struct leaf {
    class iterator;
    class reverse_iterator;

    static iterator begin(const leaf_node_t *leaf_node);
    static iterator end(const leaf_node_t *leaf_node);

    static reverse_iterator rbegin(const leaf_node_t *leaf_node);
    static reverse_iterator rend(const leaf_node_t *leaf_node);

    static iterator inclusive_lower_bound(const btree_key_t *key, const leaf_node_t *leaf_node);
    static reverse_iterator inclusive_upper_bound(const btree_key_t *key, const leaf_node_t *leaf_node);






    static std::string strprint_leaf(value_sizer_t *sizer, const leaf_node_t *node);

    static void print(FILE *fp, value_sizer_t *sizer, const leaf_node_t *node);

    class key_value_fscker_t {
    public:
        key_value_fscker_t() { }

        // Returns true if there are no problems.
        virtual bool fsck(value_sizer_t *sizer, const btree_key_t *key,
                          const void *value, std::string *msg_out) = 0;

    protected:
        virtual ~key_value_fscker_t() { }

        DISABLE_COPYING(key_value_fscker_t);
    };

    static bool fsck(value_sizer_t *sizer, const btree_key_t *left_exclusive_or_null, const btree_key_t *right_inclusive_or_null, const leaf_node_t *node, key_value_fscker_t *fscker, std::string *msg_out);

    static void validate(value_sizer_t *sizer, const leaf_node_t *node);

    static void init(value_sizer_t *sizer, leaf_node_t *node);

    static bool is_empty(const leaf_node_t *node);

    static bool is_full(value_sizer_t *sizer, const leaf_node_t *node, const btree_key_t *key, const void *value);

    static bool is_underfull(value_sizer_t *sizer, const leaf_node_t *node);

    static void split(value_sizer_t *sizer, leaf_node_t *node, leaf_node_t *sibling,
                      store_key_t *median_out);

    static void merge(value_sizer_t *sizer, leaf_node_t *left, leaf_node_t *right);

    // The pointers in `moved_values_out` point to positions in `node` and
    // will be valid as long as `node` remains unchanged.
    static bool level(value_sizer_t *sizer, int nodecmp_node_with_sib, leaf_node_t *node,
                      leaf_node_t *sibling, btree_key_t *replacement_key_out,
                      std::vector<const void *> *moved_values_out);

    static bool is_mergable(value_sizer_t *sizer, const leaf_node_t *node, const leaf_node_t *sibling);

    static bool find_key(const leaf_node_t *node, const btree_key_t *key, int *index_out);

    static bool lookup(value_sizer_t *sizer, const leaf_node_t *node, const btree_key_t *key, void *value_out);

    static void insert(value_sizer_t *sizer, leaf_node_t *node, const btree_key_t *key, const void *value, repli_timestamp_t tstamp);

    static void remove(value_sizer_t *sizer, leaf_node_t *node, const btree_key_t *key, repli_timestamp_t tstamp);

    static void erase_presence(value_sizer_t *sizer, leaf_node_t *node, const btree_key_t *key);

    class entry_reception_callback_t {
    public:
        /* Note: If any of these callbacks throw exceptions, then
           `dump_entries_since_time()` must pass the exception up and not leak memory
           or anything. */

        // Says that the timestamp was too early, and we can't send accurate deletion history.
        virtual void lost_deletions() = 0;

        // Sends a deletion in the deletion history.
        virtual void deletion(const btree_key_t *k, repli_timestamp_t tstamp) = 0;

        // Sends the key/value pairs in the leaf.
        virtual void keys_values(const std::vector<const btree_key_t *> &ks, const std::vector<const void *> &values, const std::vector<repli_timestamp_t> &tstamps) = 0;

    protected:
        virtual ~entry_reception_callback_t() { }
    };

    static void dump_entries_since_time(value_sizer_t *sizer, const leaf_node_t *node, repli_timestamp_t minimum_tstamp, repli_timestamp_t maximum_possible_timestamp,  entry_reception_callback_t *cb);

    class iterator {
    public:
        iterator();
        iterator(const leaf_node_t *node, int index);
        std::pair<const btree_key_t *, const void *> get() const;
        void step();
        void step_backwards();
        bool operator==(const iterator &other) const;
        bool operator!=(const iterator &other) const;
    private:
        const leaf_node_t *node_;
        int index_;
    };

    class reverse_iterator {
    public:
        reverse_iterator();
        reverse_iterator(const leaf_node_t *node, int index);
        std::pair<const btree_key_t *, const void *> get() const;
        void step();
        bool operator==(const reverse_iterator &other) const;
        bool operator!=(const reverse_iterator &other) const;
    private:
        iterator inner_;
    };
};


#endif  // BTREE_LEAF_NODE_HPP_
