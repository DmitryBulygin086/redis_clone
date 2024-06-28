#pragma once

#include <stddef.h>
#include <stdint.h>


/**
 * @brief Structure representing an item in a heap.
 * 
 * This structure is used to store a value and a reference to a size_t variable.
 * The value is used for comparison in the heap operations, while the reference
 * is updated when the item's position in the heap changes.
 */
struct HeapItem {
    uint64_t val = 0;  ///< The value used for comparison in the heap.
    size_t *ref = NULL;  ///< A reference to a size_t variable that is updated when the item's position in the heap changes.
};

/**
 * @brief Updates the position of an item in a heap.
 * 
 * This function is used to update the position of an item in a heap. It compares
 * the item's value with its parent's value and swaps them if necessary, then
 * recursively applies the same process to the updated item's position.
 * 
 * @param a A pointer to the array of HeapItem objects representing the heap.
 * @param pos The position of the item to be updated in the heap.
 * @param len The length of the heap.
 */
void heap_update(HeapItem *a, size_t pos, size_t len);
