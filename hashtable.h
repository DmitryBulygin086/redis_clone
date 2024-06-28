#pragma once

#include <stddef.h>
#include <stdint.h>


// hashtable node, should be embedded into the payload
struct HNode {
    HNode *next = NULL;
    uint64_t hcode = 0;
};

// a simple fixed-sized hashtable
struct HTab {
    HNode **tab = NULL;
    size_t mask = 0;
    size_t size = 0;
};

// the real hashtable interface.
// it uses 2 hashtables for progressive resizing.
/**
 * @brief A struct representing a hashmap.
 *
 * This struct contains two hashtables (ht1 and ht2) for progressive resizing.
 * It also keeps track of the current resizing position (resizing_pos).
 */
struct HMap {
    HTab ht1;   ///< newer hashtable
    HTab ht2;   ///< older hashtable
    size_t resizing_pos = 0;  ///< current resizing position
};

/**
 * @brief Look up a node in the hashmap.
 *
 * @param hmap The hashmap to search in.
 * @param key The key to search for.
 * @param eq A function pointer to a function that compares two nodes for equality.
 * @return A pointer to the found node, or NULL if not found.
 */
HNode *hm_lookup(HMap *hmap, HNode *key, bool (*eq)(HNode *, HNode *));

/**
 * @brief Insert a node into the hashmap.
 *
 * @param hmap The hashmap to insert into.
 * @param node The node to insert.
 */
void hm_insert(HMap *hmap, HNode *node);

/**
 * @brief Remove and return a node from the hashmap.
 *
 * @param hmap The hashmap to remove from.
 * @param key The key to search for.
 * @param eq A function pointer to a function that compares two nodes for equality.
 * @return A pointer to the removed node, or NULL if not found.
 */
HNode *hm_pop(HMap *hmap, HNode *key, bool (*eq)(HNode *, HNode *));

/**
 * @brief Get the number of nodes in the hashmap.
 *
 * @param hmap The hashmap to get the size of.
 * @return The number of nodes in the hashmap.
 */
size_t hm_size(HMap *hmap);

/**
 * @brief Destroy the hashmap and free all allocated memory.
 *
 * @param hmap The hashmap to destroy.
 */
void hm_destroy(HMap *hmap);

HNode *hm_lookup(HMap *hmap, HNode *key, bool (*eq)(HNode *, HNode *));
void hm_insert(HMap *hmap, HNode *node);
HNode *hm_pop(HMap *hmap, HNode *key, bool (*eq)(HNode *, HNode *));
size_t hm_size(HMap *hmap);
void hm_destroy(HMap *hmap);
