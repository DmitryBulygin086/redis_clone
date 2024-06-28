#pragma once

#include <stdint.h>
#include <stddef.h>


/**
 * @brief This macro is used to get the address of the structure that contains a given member.
 *
 * @param ptr Pointer to the member within the structure.
 * @param type Type of the structure.
 * @param member Name of the member within the structure.
 *
 * @return A pointer to the structure that contains the given member.
 *
 * @note This macro is useful when you have a pointer to a member of a structure and you want to get a pointer to the structure itself.
 *
 * @example
 * struct MyStruct {
 *     int my_member;
 * };
 *
 * MyStruct my_struct;
 * int* my_member_ptr = &my_struct.my_member;
 * MyStruct* my_struct_ptr = container_of(my_member_ptr, MyStruct, my_member);
 */
#define container_of(ptr, type, member) ({                  \
    const typeof( ((type *)0)->member ) *__mptr = (ptr);    \
    (type *)( (char *)__mptr - offsetof(type, member) );})


inline uint64_t str_hash(const uint8_t *data, size_t len) {
    uint32_t h = 0x811C9DC5;
    for (size_t i = 0; i < len; i++) {
        h = (h + data[i]) * 0x01000193;
    }
    return h;
}

enum {
    SER_NIL = 0,
    SER_ERR = 1,
    SER_STR = 2,
    SER_INT = 3,
    SER_DBL = 4,
    SER_ARR = 5,
};
