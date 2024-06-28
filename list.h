#pragma once

#include <stddef.h>


/**
 * @brief A doubly linked list node.
 *
 * This struct represents a node in a doubly linked list. Each node contains
 * pointers to the previous and next nodes in the list.
 */
struct DList {
    DList *prev;  //!< Pointer to the previous node in the list.
    DList *next;  //!< Pointer to the next node in the list.
};

/**
 * @brief Initializes a doubly linked list node.
 *
 * This function initializes a doubly linked list node by setting its previous and next pointers to itself.
 * This effectively creates a circular doubly linked list with a single node.
 *
 * @param node  A pointer to the doubly linked list node to be initialized.
 */
inline void dlist_init(DList *node) {
    node->prev = node->next = node;
}

/**
 * @brief Checks if a doubly linked list is empty.
 *
 * This function checks if a doubly linked list is empty by comparing the next pointer of the given node
 * with the node itself. If they are equal, it means the list is empty.
 *
 * @param node  A pointer to the doubly linked list node to be checked.
 * @return      True if the doubly linked list is empty, false otherwise.
 */
inline bool dlist_empty(DList *node) {
    return node->next == node;
}

/**
 * @brief Detaches a node from a doubly linked list.
 *
 * This function detaches a given node from a doubly linked list by updating the previous and next pointers
 * of the surrounding nodes. After calling this function, the node will no longer be part of the list.
 *
 * @param node  A pointer to the doubly linked list node to be detached.
 */
inline void dlist_detach(DList *node) {
    DList *prev = node->prev;
    DList *next = node->next;
    prev->next = next;
    next->prev = prev;
}

/**
 * @brief Inserts a node before the target node in a doubly linked list.
 *
 * This function inserts a new node (rookie) into a doubly linked list before the target node.
 * It updates the previous and next pointers of the surrounding nodes to maintain the correct
 * order in the list.
 *
 * @param target  A pointer to the target node where the new node will be inserted before.
 * @param rookie  A pointer to the new node to be inserted into the list.
 */
inline void dlist_insert_before(DList *target, DList *rookie) {
    DList *prev = target->prev;
    prev->next = rookie;
    rookie->prev = prev;
    rookie->next = target;
    target->prev = rookie;
}