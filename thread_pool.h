#pragma once

#include <stdint.h>
#include <stddef.h>
#include <pthread.h>
#include <vector>
#include <deque>


struct Work {
    void (*f)(void *) = NULL;
    void *arg = NULL;
};

/**
 * @brief Structure representing a thread pool.
 *
 * This structure contains the necessary data members to manage a thread pool.
 * It includes a vector of pthread_t to store the thread IDs, a deque of Work to store the jobs,
 * and two pthread synchronization primitives: a mutex and a condition variable.
 */
struct TheadPool {
    std::vector<pthread_t> threads;  // Vector to store thread IDs
    std::deque<Work> queue;         // Deque to store jobs
    pthread_mutex_t mu;             // Mutex for thread-safe access to the queue
    pthread_cond_t not_empty;       // Condition variable to signal when the queue is not empty
};

/**
 * @brief Initialize a thread pool.
 *
 * This function initializes a thread pool with the specified number of threads.
 * It creates the required number of threads and initializes the necessary synchronization primitives.
 *
 * @param tp A pointer to the thread pool structure to be initialized.
 * @param num_threads The number of threads to create in the thread pool.
 *
 * @return void
 *
 * @note This function assumes that the thread pool structure has been properly allocated.
 * @note The thread pool is not started until thread_pool_start() is called.
 * @note The thread pool is not destroyed until thread_pool_destroy() is called.
 *
 * @warning This function does not check for errors in creating the threads or initializing the synchronization primitives.
 * @warning It is the responsibility of the caller to handle any errors that may occur during thread creation or initialization.
 */
void thread_pool_init(TheadPool *tp, size_t num_threads);

/**
 * \brief Adds a new job to the thread pool's queue.
 *
 * This function adds a new job to the thread pool's queue. The job is represented by a function pointer
 * and an argument. The function pointer will be called with the argument in a separate thread.
 *
 * \param tp A pointer to the thread pool.
 * \param f A pointer to the function that will be executed as a job.
 * \param arg The argument that will be passed to the job function.
 *
 * \return void
 *
 * \note This function is thread-safe and can be called from multiple threads concurrently.
 *
 * \warning The caller must ensure that the function pointer and argument are valid until the job is
 * completed.
 */
void thread_pool_queue(TheadPool *tp, void (*f)(void *), void *arg);
