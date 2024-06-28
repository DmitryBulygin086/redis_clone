#include <assert.h>
#include <math.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>
#include <time.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/ip.h>
#include <string>
#include <vector>
// proj
#include "hashtable.h"
#include "zset.h"
#include "list.h"
#include "heap.h"
#include "thread_pool.h"
#include "common.h"


/**
 * Prints a message to the standard error stream.
 *
 * @param msg The message to be printed.
 */
static void msg(const char *msg) {
    fprintf(stderr, "%s\n", msg);
}

/**
 * @brief Prints an error message and terminates the program.
 *
 * This function prints an error message to the standard error stream,
 * including the error number and the provided message. It then calls the
 * abort() function to terminate the program.
 *
 * @param msg The error message to be printed.
 */
static void die(const char *msg) {
    int err = errno;
    fprintf(stderr, "[%d] %s\n", err, msg);
    abort();
}


/**
 * @brief Get the current time in microseconds since the Unix epoch.
 *
 * This function uses the CLOCK_MONOTONIC clock source, which represents the
 * time elapsed since the system booted, and does not include time spent
 * while the system was idle.
 *
 * @return The current time in microseconds.
 */
static uint64_t get_monotonic_usec() {
    timespec tv = {0, 0};
    clock_gettime(CLOCK_MONOTONIC, &tv);
    return uint64_t(tv.tv_sec) * 1000000 + tv.tv_nsec / 1000;
}

/**
 * Sets the file descriptor to non-blocking mode.
 *
 * @param fd The file descriptor to be set.
 *
 * @note This function uses the fcntl() system call to set the file descriptor
 * to non-blocking mode. If the fcntl() call fails, the function will terminate
 * the program with an error message.
 */
static void fd_set_nb(int fd) {
    errno = 0;
    int flags = fcntl(fd, F_GETFL, 0);
    if (errno) {
        die("fcntl error");
        return;
    }

    // sets the O_NONBLOCK flag for the file descriptor. This flag makes the file I/O operations 
    // non-blocking, meaning that the program will not wait for the I/O operation to complete before 
    // proceeding to the next line of code.
    // By setting the O_NONBLOCK flag, the server can handle multiple client connections 
    // simultaneously without blocking the main thread. 

    flags |= O_NONBLOCK; // set flags to non-blocking mode

    errno = 0;
    (void)fcntl(fd, F_SETFL, flags);
    if (errno) {
        die("fcntl error");
    }
}

struct Conn;

// global variables
/**
 * @brief Global data structure for the server.
 *
 * This structure contains all the necessary data for the server to function.
 * It includes the database, client connections, timers, and thread pool.
 */
static struct {
    HMap db;  //!< The database, a map of all key-value pairs.
    std::vector<Conn *> fd2conn;  //!< A map of all client connections, keyed by fd.
    DList idle_list;  //!< Timers for idle connections.
    std::vector<HeapItem> heap;  //!< Timers for TTLs.
    TheadPool tp;  //!< The thread pool for asynchronous tasks.
} g_data;

/**
 * @brief Maximum size of a message that can be processed.
 *
 * This constant defines the maximum size of a message that can be processed.
 * If a message exceeds this size, it will be considered invalid.
 * The value is set to 4096 bytes.
 */
const size_t k_max_msg = 4096;

/**
 * @brief Connection state enum
 *
 * This enum defines the possible states of a connection.
 * The states are used to control the flow of data processing.
 */
enum {
    STATE_REQ = 0,
    STATE_RES = 1,
    STATE_END = 2,  // mark the connection for deletion
};

/**
 * @brief Structure representing a connection.
 *
 * This structure holds all necessary information for a connection,
 * including file descriptor, state, read and write buffers,
 * idle start time, and idle list for timer management.
 */
struct Conn {
    int fd = -1;  ///< File descriptor for the connection. -1 indicates no connection.

    uint32_t state = 0;  ///< State of the connection. Either STATE_REQ or STATE_RES.

    // buffer for reading
    size_t rbuf_size = 0;  ///< Size of data in the read buffer.
    uint8_t rbuf[4 + k_max_msg];  ///< Buffer for reading data.

    // buffer for writing
    size_t wbuf_size = 0;  ///< Size of data in the write buffer.
    size_t wbuf_sent = 0;  ///< Size of data sent from the write buffer.
    uint8_t wbuf[4 + k_max_msg];  ///< Buffer for writing data.

    uint64_t idle_start = 0;  ///< Time when the connection entered the idle state.

    // timer
    DList idle_list;  ///< List node for timer management.
};

/**
 * @brief Adds a connection to the fd2conn vector.
 *
 * This function adds a given connection to the fd2conn vector.
 * It ensures that the vector is resized if necessary to accommodate the new connection.
 *
 * @param fd2conn A reference to the vector of connections.
 * @param conn A pointer to the connection to be added.
 *
 * @return void
 *
 * @note This function assumes that the connection's file descriptor (fd) is valid.
 *
 * @note This function does not handle any errors or exceptions.
 *
 * @note This function does not check if the connection already exists in the vector.
 */
static void conn_put(std::vector<Conn *> &fd2conn, struct Conn *conn) {
    if (fd2conn.size() <= (size_t)conn->fd) {
        fd2conn.resize(conn->fd + 1);
    }
    fd2conn[conn->fd] = conn;
}

static int32_t accept_new_conn(int fd) {
    // accept
    struct sockaddr_in client_addr = {};
    socklen_t socklen = sizeof(client_addr);
    int connfd = accept(fd, (struct sockaddr *)&client_addr, &socklen);
    if (connfd < 0) {
        msg("accept() error");
        return -1;  // error
    }

    // set the new connection fd to nonblocking mode
    fd_set_nb(connfd);
    // creating the struct Conn
    struct Conn *conn = (struct Conn *)malloc(sizeof(struct Conn));
    if (!conn) {
        close(connfd);
        return -1;
    }
    conn->fd = connfd;
    conn->state = STATE_REQ;
    conn->rbuf_size = 0;
    conn->wbuf_size = 0;
    conn->wbuf_sent = 0;
    conn->idle_start = get_monotonic_usec();
    dlist_insert_before(&g_data.idle_list, &conn->idle_list);
    conn_put(g_data.fd2conn, conn);
    return 0;
}

static void state_req(Conn *conn);
static void state_res(Conn *conn);

const size_t k_max_args = 1024;

/**
 * Parses a Redis protocol request.
 *
 * @param data The input data.
 * @param len The length of the input data.
 * @param out The parsed command.
 *
 * @return 0 on success, -1 on error.
 */
static int32_t parse_req(
    const uint8_t *data, size_t len, std::vector<std::string> &out)
{
    if (len < 4) {
        return -1;
    }

    uint32_t n = 0;
    memcpy(&n, &data[0], 4);

    if (n > k_max_args) {
        return -1;
    }

    size_t pos = 4;
    while (n--) {
        if (pos + 4 > len) {
            return -1;
        }

        uint32_t sz = 0;
        memcpy(&sz, &data[pos], 4);

        if (pos + 4 + sz > len) {
            return -1;
        }

        out.push_back(std::string((char *)&data[pos + 4], sz));
        pos += 4 + sz;
    }

    if (pos != len) {
        return -1;  // trailing garbage
    }

    return 0;
}

enum {
    T_STR = 0,
    T_ZSET = 1,
};

// the structure for the key
struct Entry {
    struct HNode node;
    std::string key;
    std::string val;
    uint32_t type = 0;
    ZSet *zset = NULL;
    // for TTLs
    size_t heap_idx = -1;
};

static bool entry_eq(HNode *lhs, HNode *rhs) {
    struct Entry *le = container_of(lhs, struct Entry, node);
    struct Entry *re = container_of(rhs, struct Entry, node);
    return le->key == re->key;
}

enum {
    ERR_UNKNOWN = 1,
    ERR_2BIG = 2,
    ERR_TYPE = 3,
    ERR_ARG = 4,
};

static void out_nil(std::string &out) {
    out.push_back(SER_NIL);
}

static void out_str(std::string &out, const char *s, size_t size) {
    out.push_back(SER_STR);
    uint32_t len = (uint32_t)size;
    out.append((char *)&len, 4);
    out.append(s, len);
}

static void out_str(std::string &out, const std::string &val) {
    return out_str(out, val.data(), val.size());
}

static void out_int(std::string &out, int64_t val) {
    out.push_back(SER_INT);
    out.append((char *)&val, 8);
}

static void out_dbl(std::string &out, double val) {
    out.push_back(SER_DBL);
    out.append((char *)&val, 8);
}

static void out_err(std::string &out, int32_t code, const std::string &msg) {
    out.push_back(SER_ERR);
    out.append((char *)&code, 4);
    uint32_t len = (uint32_t)msg.size();
    out.append((char *)&len, 4);
    out.append(msg);
}

/**
 * @brief Appends an array reply to the output string.
 *
 * This function takes a reference to a string (out) and an unsigned integer (n) as parameters.
 * It appends a serialized array reply to the output string.
 * The serialized array reply consists of a single byte representing the type of reply (SER_ARR),
 * followed by a 4-byte unsigned integer representing the number of elements in the array (n).
 *
 * @param out A reference to the string where the serialized array reply will be appended.
 * @param n An unsigned integer representing the number of elements in the array.
 *
 * @return void
 */
static void out_arr(std::string &out, uint32_t n) {
    out.push_back(SER_ARR);
    out.append((char *)&n, 4);
}

static void *begin_arr(std::string &out) {
    out.push_back(SER_ARR);
    out.append("\0\0\0\0", 4);          // filled in end_arr()
    return (void *)(out.size() - 4);    // the `ctx` arg
}

static void end_arr(std::string &out, void *ctx, uint32_t n) {
    size_t pos = (size_t)ctx;
    assert(out[pos - 1] == SER_ARR);
    memcpy(&out[pos], &n, 4);
}

static void do_get(std::vector<std::string> &cmd, std::string &out) {
    Entry key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());

    HNode *node = hm_lookup(&g_data.db, &key.node, &entry_eq);
    if (!node) {
        return out_nil(out);
    }

    Entry *ent = container_of(node, Entry, node);
    if (ent->type != T_STR) {
        return out_err(out, ERR_TYPE, "expect string type");
    }
    return out_str(out, ent->val);
}

static void do_set(std::vector<std::string> &cmd, std::string &out) {
    Entry key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());

    HNode *node = hm_lookup(&g_data.db, &key.node, &entry_eq);
    if (node) {
        Entry *ent = container_of(node, Entry, node);
        if (ent->type != T_STR) {
            return out_err(out, ERR_TYPE, "expect string type");
        }
        ent->val.swap(cmd[2]);
    } else {
        Entry *ent = new Entry();
        ent->key.swap(key.key);
        ent->node.hcode = key.node.hcode;
        ent->val.swap(cmd[2]);
        hm_insert(&g_data.db, &ent->node);
    }
    return out_nil(out);
}

// set or remove the TTL
static void entry_set_ttl(Entry *ent, int64_t ttl_ms) {
    if (ttl_ms < 0 && ent->heap_idx != (size_t)-1) {
        // erase an item from the heap
        // by replacing it with the last item in the array.
        size_t pos = ent->heap_idx;
        g_data.heap[pos] = g_data.heap.back();
        g_data.heap.pop_back();
        if (pos < g_data.heap.size()) {
            heap_update(g_data.heap.data(), pos, g_data.heap.size());
        }
        ent->heap_idx = -1;
    } else if (ttl_ms >= 0) {
        size_t pos = ent->heap_idx;
        if (pos == (size_t)-1) {
            // add an new item to the heap
            HeapItem item;
            item.ref = &ent->heap_idx;
            g_data.heap.push_back(item);
            pos = g_data.heap.size() - 1;
        }
        g_data.heap[pos].val = get_monotonic_usec() + (uint64_t)ttl_ms * 1000;
        heap_update(g_data.heap.data(), pos, g_data.heap.size());
    }
}

/**
 * @brief Converts a string to a 64-bit signed integer.
 *
 * This function attempts to convert a given string to a 64-bit signed integer.
 * It uses the strtoll() function to perform the conversion, and checks if the conversion was successful.
 *
 * @param s The string to be converted.
 * @param out A reference to a 64-bit signed integer to store the result.
 *
 * @return True if the conversion was successful and the entire string was consumed, false otherwise.
 *
 * @note The function assumes that the input string is well-formed and does not handle any errors or exceptions.
 *
 * @note The function does not handle any leading or trailing whitespace in the input string.
 *
 * @note The function does not handle any overflow or underflow conditions.
 *
 * @note The function does not handle any invalid characters in the input string.
 */
static bool str2int(const std::string &s, int64_t &out) {
    char *endp = NULL;
    out = strtoll(s.c_str(), &endp, 10);
    return endp == s.c_str() + s.size();
}

/**
 * @brief Sets the expiration time for a key in the database.
 *
 * This function takes a vector of strings representing the command and a string reference to store the response.
 * It extracts the key and the expiration time from the command, looks up the key in the database, and sets the expiration time for the key.
 * If the key is found in the database, the expiration time is updated, and the function returns 1.
 * If the key is not found in the database, the function returns 0.
 *
 * @param cmd A vector of strings representing the command.
 *            The first element of the vector should be "pexpire", and the second element should be the key.
 *            The third element should be the expiration time in milliseconds.
 * @param out A string reference to store the response.
 *            The response is formatted as a Redis integer reply.
 *
 * @return void
 *
 * @note The function assumes that the command is well-formed and the database is in a valid state.
 *       It does not handle any errors or exceptions.
 *
 * @note The function uses the str2int() function to convert the expiration time from a string to an integer.
 *       It also uses the entry_set_ttl() function to update the expiration time for the key.
 */
static void do_expire(std::vector<std::string> &cmd, std::string &out) {
    int64_t ttl_ms = 0;
    if (!str2int(cmd[2], ttl_ms)) {
        return out_err(out, ERR_ARG, "expect int64");
    }

    Entry key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());

    HNode *node = hm_lookup(&g_data.db, &key.node, &entry_eq);
    if (node) {
        Entry *ent = container_of(node, Entry, node);
        entry_set_ttl(ent, ttl_ms);
    }
    return out_int(out, node ? 1: 0);
}

/**
 * @brief Calculates the time-to-live (TTL) of a key in the database.
 *
 * This function retrieves the TTL of a given key from the database.
 * If the key does not exist, it returns -2.
 * If the key exists but has no TTL set, it returns -1.
 * If the key exists and has a TTL set, it calculates and returns the remaining TTL in milliseconds.
 *
 * @param cmd A vector of strings representing the command.
 *            The first element of the vector should be "pttl".
 *            The second element of the vector should be the key.
 * @param out A string reference to store the response.
 *            The response is formatted as a Redis integer reply.
 */
static void do_ttl(std::vector<std::string> &cmd, std::string &out) {
    Entry key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());

    HNode *node = hm_lookup(&g_data.db, &key.node, &entry_eq);
    if (!node) {
        return out_int(out, -2);
    }

    Entry *ent = container_of(node, Entry, node);
    if (ent->heap_idx == (size_t)-1) {
        return out_int(out, -1);
    }

    uint64_t expire_at = g_data.heap[ent->heap_idx].val;
    uint64_t now_us = get_monotonic_usec();
    return out_int(out, expire_at > now_us ? (expire_at - now_us) / 1000 : 0);
}

// deallocate the key immediately
static void entry_destroy(Entry *ent) {
    switch (ent->type) {
    case T_ZSET:
        zset_dispose(ent->zset);
        delete ent->zset;
        break;
    }
    delete ent;
}

static void entry_del_async(void *arg) {
    entry_destroy((Entry *)arg);
}

// dispose the entry after it got detached from the key space
static void entry_del(Entry *ent) {
    entry_set_ttl(ent, -1);

    const size_t k_large_container_size = 10000;
    bool too_big = false;
    switch (ent->type) {
    case T_ZSET:
        too_big = hm_size(&ent->zset->hmap) > k_large_container_size;
        break;
    }

    if (too_big) {
        thread_pool_queue(&g_data.tp, &entry_del_async, ent);
    } else {
        entry_destroy(ent);
    }
}

static void do_del(std::vector<std::string> &cmd, std::string &out) {
    Entry key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());

    HNode *node = hm_pop(&g_data.db, &key.node, &entry_eq);
    if (node) {
        entry_del(container_of(node, Entry, node));
    }
    return out_int(out, node ? 1 : 0);
}

/**
 * @brief Scans a hash table and applies a function to each node.
 *
 * This function iterates over the hash table and applies a given function to each node.
 * It handles the case where the hash table is empty by returning immediately.
 *
 * @param tab The hash table to be scanned.
 * @param f The function to be applied to each node.
 * @param arg An additional argument to be passed to the function.
 *
 * @note The function assumes that the hash table is not modified during the scan.
 *       If the hash table is modified, the behavior of this function is undefined.
 *
 * @note The function does not handle any errors or exceptions.
 *       It assumes that the hash table and the function are valid.
 */
static void h_scan(HTab *tab, void (*f)(HNode *, void *), void *arg) {
    if (tab->size == 0) {
        return;
    }
    for (size_t i = 0; i < tab->mask + 1; ++i) {
        HNode *node = tab->tab[i];
        while (node) {
            f(node, arg);
            node = node->next;
        }
    }
}

/**
 * @brief Callback function for scanning the hash table.
 *
 * This function is used as a callback when scanning the hash table.
 * It appends the key of each node to the output string.
 *
 * @param node The hash table node.
 * @param arg A pointer to the output string.
 *
 * @note The function assumes that the output string is already initialized.
 */
static void cb_scan(HNode *node, void *arg) {
    std::string &out = *(std::string *)arg;
    out_str(out, container_of(node, Entry, node)->key);
}

/**
 * @brief Processes the "keys" command and generates a response.
 *
 * This function takes a vector of strings representing the command and a string reference to store the response.
 * It retrieves all the keys from the database and generates a response containing the number of keys and the keys themselves.
 *
 * @param cmd A vector of strings representing the command.
 *            The first element of the vector should be "keys".
 * @param out A string reference to store the response.
 *            The response is formatted as a Redis array reply.
 *
 * @return void
 *
 * @note The function does not handle any errors or exceptions.
 *       It assumes that the command is well-formed and the database is in a valid state.
 *
 * @note The function uses the h_scan() function to iterate over the hash table and the cb_scan() function to process each node.
 *
 * @note The function does not handle the case where the database is empty.
 *       It simply returns an empty array reply in that case.
 */
static void do_keys(std::vector<std::string> &cmd, std::string &out) {
    (void)cmd;  // unused parameter
    out_arr(out, (uint32_t)hm_size(&g_data.db));  // number of keys
    h_scan(&g_data.db.ht1, &cb_scan, &out);  // keys from the first hash table
    h_scan(&g_data.db.ht2, &cb_scan, &out);  // keys from the second hash table
}

static bool str2dbl(const std::string &s, double &out) {
    char *endp = NULL;
    out = strtod(s.c_str(), &endp);
    return endp == s.c_str() + s.size() && !isnan(out);
}

// zadd zset score name
static void do_zadd(std::vector<std::string> &cmd, std::string &out) {
    double score = 0;
    if (!str2dbl(cmd[2], score)) {
        return out_err(out, ERR_ARG, "expect fp number");
    }

    // look up or create the zset
    Entry key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
    HNode *hnode = hm_lookup(&g_data.db, &key.node, &entry_eq);

    Entry *ent = NULL;
    if (!hnode) {
        ent = new Entry();
        ent->key.swap(key.key);
        ent->node.hcode = key.node.hcode;
        ent->type = T_ZSET;
        ent->zset = new ZSet();
        hm_insert(&g_data.db, &ent->node);
    } else {
        ent = container_of(hnode, Entry, node);
        if (ent->type != T_ZSET) {
            return out_err(out, ERR_TYPE, "expect zset");
        }
    }

    // add or update the tuple
    const std::string &name = cmd[3];
    bool added = zset_add(ent->zset, name.data(), name.size(), score);
    return out_int(out, (int64_t)added);
}

static bool expect_zset(std::string &out, std::string &s, Entry **ent) {
    Entry key;
    key.key.swap(s);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
    HNode *hnode = hm_lookup(&g_data.db, &key.node, &entry_eq);
    if (!hnode) {
        out_nil(out);
        return false;
    }

    *ent = container_of(hnode, Entry, node);
    if ((*ent)->type != T_ZSET) {
        out_err(out, ERR_TYPE, "expect zset");
        return false;
    }
    return true;
}

// zrem zset name
static void do_zrem(std::vector<std::string> &cmd, std::string &out) {
    Entry *ent = NULL;
    if (!expect_zset(out, cmd[1], &ent)) {
        return;
    }

    const std::string &name = cmd[2];
    ZNode *znode = zset_pop(ent->zset, name.data(), name.size());
    if (znode) {
        znode_del(znode);
    }
    return out_int(out, znode ? 1 : 0);
}

// zscore zset name
static void do_zscore(std::vector<std::string> &cmd, std::string &out) {
    Entry *ent = NULL;
    if (!expect_zset(out, cmd[1], &ent)) {
        return;
    }

    const std::string &name = cmd[2];
    ZNode *znode = zset_lookup(ent->zset, name.data(), name.size());
    return znode ? out_dbl(out, znode->score) : out_nil(out);
}

// zquery zset score name offset limit
static void do_zquery(std::vector<std::string> &cmd, std::string &out) {
    // parse args
    double score = 0;
    if (!str2dbl(cmd[2], score)) {
        return out_err(out, ERR_ARG, "expect fp number");
    }
    const std::string &name = cmd[3];
    int64_t offset = 0;
    int64_t limit = 0;
    if (!str2int(cmd[4], offset)) {
        return out_err(out, ERR_ARG, "expect int");
    }
    if (!str2int(cmd[5], limit)) {
        return out_err(out, ERR_ARG, "expect int");
    }

    // get the zset
    Entry *ent = NULL;
    if (!expect_zset(out, cmd[1], &ent)) {
        if (out[0] == SER_NIL) {
            out.clear();
            out_arr(out, 0);
        }
        return;
    }

    // look up the tuple
    if (limit <= 0) {
        return out_arr(out, 0);
    }
    ZNode *znode = zset_query(ent->zset, score, name.data(), name.size());
    znode = znode_offset(znode, offset);

    // output
    void *arr = begin_arr(out);
    uint32_t n = 0;
    while (znode && (int64_t)n < limit) {
        out_str(out, znode->name, znode->len);
        out_dbl(out, znode->score);
        znode = znode_offset(znode, +1);
        n += 2;
    }
    end_arr(out, arr, n);
}

/**
 * @brief Checks if a given word is the same as a command.
 *
 * This function compares a given word with a command using a case-insensitive comparison.
 * It returns true if the word is the same as the command, and false otherwise.
 *
 * @param word The word to be compared with the command.
 * @param cmd The command to be compared with the word.
 *
 * @return True if the word is the same as the command, false otherwise.
 */
static bool cmd_is(const std::string &word, const char *cmd) {
    return 0 == strcasecmp(word.c_str(), cmd);
}

/**
 * @brief Processes a request and generates a response.
 *
 * This function takes a vector of strings representing the command and a string reference to store the response.
 * It checks the command and parameters to determine the appropriate action to take.
 * If the command is recognized, it calls the appropriate function to perform the action and generates the response.
 * If the command is not recognized, it generates an error response.
 *
 * @param cmd A vector of strings representing the command.
 * @param out A string reference to store the response.
 */
static void do_request(std::vector<std::string> &cmd, std::string &out) {
    if (cmd.size() == 1 && cmd_is(cmd[0], "keys")) {
        do_keys(cmd, out);
    } else if (cmd.size() == 2 && cmd_is(cmd[0], "get")) {
        do_get(cmd, out);
    } else if (cmd.size() == 3 && cmd_is(cmd[0], "set")) {
        do_set(cmd, out);
    } else if (cmd.size() == 2 && cmd_is(cmd[0], "del")) {
        do_del(cmd, out);
    } else if (cmd.size() == 3 && cmd_is(cmd[0], "pexpire")) {
        do_expire(cmd, out);
    } else if (cmd.size() == 2 && cmd_is(cmd[0], "pttl")) {
        do_ttl(cmd, out);
    } else if (cmd.size() == 4 && cmd_is(cmd[0], "zadd")) {
        do_zadd(cmd, out);
    } else if (cmd.size() == 3 && cmd_is(cmd[0], "zrem")) {
        do_zrem(cmd, out);
    } else if (cmd.size() == 3 && cmd_is(cmd[0], "zscore")) {
        do_zscore(cmd, out);
    } else if (cmd.size() == 6 && cmd_is(cmd[0], "zquery")) {
        do_zquery(cmd, out);
    } else {
        // cmd is not recognized
        out_err(out, ERR_UNKNOWN, "Unknown cmd");
    }
}

/**
 * @brief Tries to process one request from the connection's buffer.
 *
 * This function attempts to parse a request from the connection's read buffer.
 * If the request is successfully parsed, it generates a response and packs the response into the connection's write buffer.
 * If the request is not fully available in the buffer, the function returns false and will retry in the next iteration.
 * If the request is invalid or the response is too big, the function sets the connection's state to STATE_END.
 *
 * @param conn The connection from which the request is to be processed.
 *
 * @return True if the request was fully processed and the connection's state is still STATE_REQ.
 *         False if the request was not fully available in the buffer or the request was invalid or the response was too big.
 *
 * @note This function uses a loop to continuously try to read data until no more data can be read.
 *       This is done to handle the case where the buffer is not large enough to hold all the data that is available.
 *
 * @note The function also handles the case where the read operation is interrupted by a signal,
 *       and it continues to try to read data until it succeeds or encounters an error.
 *
 * @note The function does not handle the case where the connection is in the response state.
 *       It only handles the case where the connection is in the request state.
 */
static bool try_one_request(Conn *conn) {
    // try to parse a request from the buffer
    if (conn->rbuf_size < 4) {
        // not enough data in the buffer. Will retry in the next iteration
        return false;
    }
    uint32_t len = 0;
    memcpy(&len, &conn->rbuf[0], 4);
    if (len > k_max_msg) {
        msg("too long");
        conn->state = STATE_END;
        return false;
    }
    if (4 + len > conn->rbuf_size) {
        // not enough data in the buffer. Will retry in the next iteration
        return false;
    }

    // parse the request
    std::vector<std::string> cmd;
    if (0 != parse_req(&conn->rbuf[4], len, cmd)) {
        msg("bad req");
        conn->state = STATE_END;
        return false;
    }

    // got one request, generate the response.
    std::string out;
    do_request(cmd, out);

    // pack the response into the buffer
    if (4 + out.size() > k_max_msg) {
        out.clear();
        out_err(out, ERR_2BIG, "response is too big");
    }
    uint32_t wlen = (uint32_t)out.size();
    memcpy(&conn->wbuf[0], &wlen, 4);
    memcpy(&conn->wbuf[4], out.data(), out.size());
    conn->wbuf_size = 4 + wlen;

    // remove the request from the buffer.
    // note: frequent memmove is inefficient.
    // note: need better handling for production code.
    size_t remain = conn->rbuf_size - 4 - len;
    if (remain) {
        memmove(conn->rbuf, &conn->rbuf[4 + len], remain);
    }
    conn->rbuf_size = remain;

    // change state
    conn->state = STATE_RES;
    state_res(conn);

    // continue the outer loop if the request was fully processed
    return (conn->state == STATE_REQ);
}

/**
 * @brief Tries to fill the read buffer of a connection.
 *
 * This function attempts to read data from the client's socket into the connection's read buffer.
 * It continues to read data until either the buffer is full, an error occurs, or the client closes the connection.
 *
 * @param conn The connection for which the read buffer is to be filled.
 *
 * @return True if the connection is still in the request state and there is more data to be read.
 *         False if the connection is no longer in the request state or there is no more data to be read.
 *
 * @note This function uses a loop to continuously try to read data until no more data can be read.
 *       This is done to handle the case where the buffer is not large enough to hold all the data that is available.
 *
 * @note The function also handles the case where the read operation is interrupted by a signal,
 *       and it continues to try to read data until it succeeds or encounters an error.
 *
 * @note The function does not handle the case where the connection is in the response state.
 *       It only handles the case where the connection is in the request state.
 */
static bool try_fill_buffer(Conn *conn) {
    // try to fill the buffer
    assert(conn->rbuf_size < sizeof(conn->rbuf));
    ssize_t rv = 0;
    do {
        size_t cap = sizeof(conn->rbuf) - conn->rbuf_size;
        rv = read(conn->fd, &conn->rbuf[conn->rbuf_size], cap);
    } while (rv < 0 && errno == EINTR);
    if (rv < 0 && errno == EAGAIN) {
        // got EAGAIN, stop.
        return false;
    }
    if (rv < 0) {
        msg("read() error");
        conn->state = STATE_END;
        return false;
    }
    if (rv == 0) {
        if (conn->rbuf_size > 0) {
            msg("unexpected EOF");
        } else {
            msg("EOF");
        }
        conn->state = STATE_END;
        return false;
    }

    conn->rbuf_size += (size_t)rv;
    assert(conn->rbuf_size <= sizeof(conn->rbuf));

    // Try to process requests one by one.
    while (try_one_request(conn)) {}
    return (conn->state == STATE_REQ);
}

/**
 * @brief Handles the I/O for a connection in the request state.
 *
 * This function is called when the operating system indicates that a connection
 * is ready for I/O and the connection is in the request state. It updates the idle timer for the connection,
 * and then calls the appropriate state function to handle the I/O.
 *
 * @param conn The connection for which I/O is to be handled.
 *
 * @note This function uses a while loop to continuously try to fill the buffer until no more data can be read.
 *       This is done to handle the case where the buffer is not large enough to hold all the data that is available.
 *
 * @note The function also handles the case where the read operation is interrupted by a signal,
 *       and it continues to try to read data until it succeeds or encounters an error.
 *
 * @note The function does not handle the case where the connection is in the response state.
 *       It only handles the case where the connection is in the request state.
 */
static void state_req(Conn *conn) {
    while (try_fill_buffer(conn)) {}
}

static bool try_flush_buffer(Conn *conn) {
    ssize_t rv = 0;
    do {
        size_t remain = conn->wbuf_size - conn->wbuf_sent;
        rv = write(conn->fd, &conn->wbuf[conn->wbuf_sent], remain);
    } while (rv < 0 && errno == EINTR);
    if (rv < 0 && errno == EAGAIN) {
        // got EAGAIN, stop.
        return false;
    }
    if (rv < 0) {
        msg("write() error");
        conn->state = STATE_END;
        return false;
    }
    conn->wbuf_sent += (size_t)rv;
    assert(conn->wbuf_sent <= conn->wbuf_size);
    if (conn->wbuf_sent == conn->wbuf_size) {
        // response was fully sent, change state back
        conn->state = STATE_REQ;
        conn->wbuf_sent = 0;
        conn->wbuf_size = 0;
        return false;
    }
    // still got some data in wbuf, could try to write again
    return true;
}

/**
 * @brief Handles the I/O for a connection in the response state.
 *
 * This function is called when the operating system indicates that a connection
 * is ready for I/O and the connection is in the response state. It updates the idle timer for the connection,
 * and then calls the appropriate state function to handle the I/O.
 *
 * @param conn The connection for which I/O is to be handled.
 *
 * @note This function uses a while loop to continuously try to flush the buffer until no more data can be written.
 *       This is done to handle the case where the buffer is not large enough to hold all the data that is available.
 *
 * @note The function also handles the case where the write operation is interrupted by a signal,
 *       and it continues to try to write data until it succeeds or encounters an error.
 *
 * @note The function does not handle the case where the connection is in the request state.
 *       It only handles the case where the connection is in the response state.
 */
static void state_res(Conn *conn) {
    while (try_flush_buffer(conn)) {}
}

/**
 * @brief Handles the I/O for a connection.
 *
 * This function is called when the operating system indicates that a connection
 * is ready for I/O. It updates the idle timer for the connection, and then calls
 * the appropriate state function to handle the I/O.
 *
 * @param conn The connection for which I/O is to be handled.
 */
static void connection_io(Conn *conn) {
    // waked up by poll, update the idle timer
    // by moving conn to the end of the list.
    conn->idle_start = get_monotonic_usec();
    dlist_detach(&conn->idle_list);
    dlist_insert_before(&g_data.idle_list, &conn->idle_list);

    // do the work
    if (conn->state == STATE_REQ) {
        state_req(conn);
    } else if (conn->state == STATE_RES) {
        state_res(conn);
    } else {
        assert(0);  // not expected
    }
}

const uint64_t k_idle_timeout_ms = 5 * 1000;

/**
 * @brief Calculate the next timer in milliseconds.
 *
 * This function calculates the time until the next timer event, either an idle timer or a TTL timer.
 * It considers both the idle timers and TTL timers, and returns the time until the next timer event.
 *
 * @return The time until the next timer event in milliseconds.
 *         If there are no timers, it returns 10000 milliseconds.
 */
static uint32_t next_timer_ms() {
    uint64_t now_us = get_monotonic_usec();
    uint64_t next_us = (uint64_t)-1;

    // idle timers
    if (!dlist_empty(&g_data.idle_list)) {
        Conn *next = container_of(g_data.idle_list.next, Conn, idle_list);
        next_us = next->idle_start + k_idle_timeout_ms * 1000;
    }

    // ttl timers
    if (!g_data.heap.empty() && g_data.heap[0].val < next_us) {
        next_us = g_data.heap[0].val;
    }

    if (next_us == (uint64_t)-1) {
        return 10000;   // no timer, the value doesn't matter
    }

    if (next_us <= now_us) {
        // missed?
        return 0;
    }
    return (uint32_t)((next_us - now_us) / 1000);
}

/**
 * @brief Function to handle the closure of a connection.
 *
 * This function is responsible for cleaning up and freeing resources associated with a connection.
 * It removes the connection from the fd2conn array, closes the socket, detaches the connection from the idle list,
 * and finally frees the memory allocated for the connection.
 *
 * @param conn The connection to be closed and cleaned up.
 */
static void conn_done(Conn *conn) {
    g_data.fd2conn[conn->fd] = NULL;
    (void)close(conn->fd);
    dlist_detach(&conn->idle_list);
    free(conn);
}

static bool hnode_same(HNode *lhs, HNode *rhs) {
    return lhs == rhs;
}

static void process_timers() {
    // the extra 1000us is for the ms resolution of poll()
    uint64_t now_us = get_monotonic_usec() + 1000;

    // idle timers
    while (!dlist_empty(&g_data.idle_list)) {
        Conn *next = container_of(g_data.idle_list.next, Conn, idle_list);
        uint64_t next_us = next->idle_start + k_idle_timeout_ms * 1000;
        if (next_us >= now_us) {
            // not ready
            break;
        }

        printf("removing idle connection: %d\n", next->fd);
        conn_done(next);
    }

    // TTL timers
    const size_t k_max_works = 2000;
    size_t nworks = 0;
    while (!g_data.heap.empty() && g_data.heap[0].val < now_us) {
        Entry *ent = container_of(g_data.heap[0].ref, Entry, heap_idx);
        HNode *node = hm_pop(&g_data.db, &ent->node, &hnode_same);
        assert(node == &ent->node);
        entry_del(ent);
        if (nworks++ >= k_max_works) {
            // don't stall the server if too many keys are expiring at once
            break;
        }
    }
}

int main() {
    // prepare the listening socket
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        die("socket()");
    }

    int val = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

    // bind
    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = ntohs(1234);
    addr.sin_addr.s_addr = ntohl(0);    // wildcard address 0.0.0.0
    int rv = bind(fd, (const sockaddr *)&addr, sizeof(addr));
    if (rv != 0) {
        die("bind()");
    }

    // listen
    rv = listen(fd, SOMAXCONN);
    if (rv != 0) {
        die("listen()");
    }

    // set the listen fd to nonblocking mode
    fd_set_nb(fd);

    // some initializations
    dlist_init(&g_data.idle_list);
    thread_pool_init(&g_data.tp, 4);

    // the event loop
    std::vector<struct pollfd> poll_args;
    while (true) {
        // prepare the arguments of the poll()
        poll_args.clear();
        // for convenience, the listening fd is put in the first position
        struct pollfd pfd = {fd, POLLIN, 0};
        poll_args.push_back(pfd);
        // connection fds
        for (Conn *conn : g_data.fd2conn) {
            if (!conn) {
                continue;
            }
            struct pollfd pfd = {};
            pfd.fd = conn->fd;
            pfd.events = (conn->state == STATE_REQ) ? POLLIN : POLLOUT;
            pfd.events = pfd.events | POLLERR;
            poll_args.push_back(pfd);
        }

        // poll for active fds
        int timeout_ms = (int)next_timer_ms();
        int rv = poll(poll_args.data(), (nfds_t)poll_args.size(), timeout_ms);
        if (rv < 0) {
            die("poll");
        }

        // process active connections
        for (size_t i = 1; i < poll_args.size(); ++i) {
            if (poll_args[i].revents) {
                Conn *conn = g_data.fd2conn[poll_args[i].fd];
                connection_io(conn);
                if (conn->state == STATE_END) {
                    // client closed normally, or something bad happened.
                    // destroy this connection
                    conn_done(conn);
                }
            }
        }

        // handle timers
        process_timers();

        // try to accept a new connection if the listening fd is active
        if (poll_args[0].revents) {
            (void)accept_new_conn(fd);
        }
    }

    return 0;
}
