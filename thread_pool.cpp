#include <assert.h>
#include "thread_pool.h"


/**
 * @brief The worker function that runs in each thread of the thread pool.
 *
 * This function is responsible for fetching work from the thread pool's queue,
 * executing the work, and then waiting for more work if the queue is empty.
 *
 * @param arg A pointer to the ThreadPool object.
 * @return NULL, as required by pthread_create.
 */
static void *worker(void *arg) {
    TheadPool *tp = (TheadPool *)arg;
    while (true) {
        pthread_mutex_lock(&tp->mu);

        // Wait for the condition: a non-empty queue
        while (tp->queue.empty()) {
            pthread_cond_wait(&tp->not_empty, &tp->mu);
        }

        // Got the job
        Work w = tp->queue.front();
        tp->queue.pop_front();

        pthread_mutex_unlock(&tp->mu);

        // Do the work
        w.f(w.arg);
    }
    return NULL;
}


void thread_pool_init(TheadPool *tp, size_t num_threads) {
    assert(num_threads > 0);

    // Mutex initialization
    // Mutexes are used to protect shared data from simultaneous access by multiple threads.
    int rv = pthread_mutex_init(&tp->mu, NULL);
    assert(rv == 0);

    rv = pthread_cond_init(&tp->not_empty, NULL);
    assert(rv == 0);

    tp->threads.resize(num_threads);
    for (size_t i = 0; i < num_threads; ++i) {
        int rv = pthread_create(&tp->threads[i], NULL, &worker, tp);
        assert(rv == 0);
    }
}

void thread_pool_queue(TheadPool *tp, void (*f)(void *), void *arg) {
    Work w;
    w.f = f;
    w.arg = arg;

    pthread_mutex_lock(&tp->mu);
    tp->queue.push_back(w);
    pthread_cond_signal(&tp->not_empty);
    pthread_mutex_unlock(&tp->mu);
}
