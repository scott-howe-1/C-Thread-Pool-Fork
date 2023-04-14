/* ********************************
 * Author:       Johan Hanssen Seferidis
 * License:	     MIT
 * Description:  Library providing a threading pool where you can add
 *               work. For usage, check the thpool.h file or README.md
 *
 *//** @file thpool.h *//*
 *
 ********************************/

#if defined(__APPLE__)
#include <AvailabilityMacros.h>
#else
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif
#endif
#include <unistd.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <errno.h>
#include <time.h>
#if defined(__linux__)
#include <sys/prctl.h>
#endif

#include "thpool.h"

#ifdef THPOOL_DEBUG
#define THPOOL_DEBUG 1
#else
#define THPOOL_DEBUG 0
#endif

#if !defined(DISABLE_PRINT) || defined(THPOOL_DEBUG)
#define err(str) fprintf(stderr, str)
#else
#define err(str)
#endif

//TODO:(captured) How deal with these when I have multiple thread pools, each one needing to be individually controlled????
//		Move into thpool struct???
static volatile int threads_keepalive;
static volatile int threads_on_hold;


//TODO DUMPING GROUND
//===================
//TODO:(captured) Better solution for all the debug messages
//TODO:(captured) add queue metrics
//TODO:(captured) add queue size limit (or maybe just a warning that a threshold has been exceeded)
//TODO:(captured) Some\all structs need to move to .h file, so available to users of primary thpool apis.
//TODO: How handle duplicate job_uuid's, if at all?
//		Should this ever happen using UUID's?
//		Only look for this in queue_out?  What do?
//		Or, just allow it for now.  Future "queue_out monitor thread" can remove "aged-out" jobs.
//TODO: AFTER INTEGRATION: all printf() and err() calls must be replaced will appropriate logging function calls
//TODO: Review mutex usage.
//		Some thread-shared "volatile" struct params are read outside of mutex locks.
//			Should locks be added?

/* ========================== STRUCTURES ============================ */


/* Binary semaphore */
typedef struct bsem {
	pthread_mutex_t mutex;
	pthread_cond_t   cond;
	int v;
} bsem;

// typedef struct job_metrics {
// 	int    age_queue_in;
// 	int    age_queue_out;
// }job_metrics;

/* Job */
typedef struct job{
	struct job*  prev;           /* pointer to previous job   */

//TODO: If keep, need to sort out different function pointer prototypes scattered across test code.
	th_func_p    function;       /* function pointer          */
	void*        arg;            /* function's argument       */	//fd
//	void*        arg2;           /* function's argument       */	//cmd_nvme

	int          uuid;           /* job identifier            */
	int          result;         /* job result code           */
//	int          age_queue;      /* generic age for either queue?  Later put in metrics struct? */

// 	struct job_metrics     metrics;
} job;

/* Job queue */
typedef struct jobqueue{
	pthread_mutex_t rwmutex;             /* used for queue r/w access */
	job  *front;                         /* pointer to front of queue */
	job  *rear;                          /* pointer to rear  of queue */
	bsem *has_jobs;                      /* flag as binary semaphore  */
	volatile int len;                    /* number of jobs in queue   */
} jobqueue;


/* Thread */
//TODO: Add a flushing state to the thread (for when a task requestor goes away unexpectedly)
typedef struct thread{
	int       id;                        /* friendly id               */
	pthread_t pthread;                   /* pointer to actual thread  */
	struct thpool_* thpool_p;            /* access to thpool          */
} thread;

/* Threadpool */
typedef struct thpool_{
	thread**   threads;                  /* pointer to threads        */
	volatile int num_threads_alive;      /* threads currently alive   */
	volatile int num_threads_working;    /* threads currently working */
	pthread_mutex_t  thcount_lock;       /* used for thread count etc */
	pthread_cond_t  threads_all_idle;    /* signal to thpool_wait     */
	jobqueue  queue_in;                  /* queue for pending jobs    */
	jobqueue  queue_out;                 /* queue for completed jobs  */
} thpool_;


#define MAX_QUEUE_SIZE_WITHOUT_WARNING      100


/* ========================== PROTOTYPES ============================ */


static int   thread_init(thpool_* thpool_p, struct thread** thread_p, int id);
static void* thread_do(struct thread* thread_p);
static void  thread_hold(int sig_id);
static void  thread_destroy(struct thread* thread_p);

static int   jobqueue_init(jobqueue* jobqueue_p);
static void  jobqueue_clear(jobqueue* jobqueue_p);
static void  jobqueue_push(jobqueue* jobqueue_p, struct job* newjob_p);
static struct job* jobqueue_pull_front(jobqueue* jobqueue_p);
static struct job* jobqueue_pull_by_uuid(jobqueue* jobqueue_p, int job_uuid);
static void  jobqueue_destroy(jobqueue* jobqueue_p);

static int   bsem_init(struct bsem *bsem_p, int value);
static void  bsem_reset(struct bsem *bsem_p);
static void  bsem_post(struct bsem *bsem_p);
static void  bsem_post_all(struct bsem *bsem_p);
static void  bsem_wait(struct bsem *bsem_p);





/* ========================== THREADPOOL ============================ */


/* Initialise thread pool */
struct thpool_* thpool_init(int num_threads){

	threads_on_hold   = 0;
	threads_keepalive = 1;

	if (num_threads < 0){
		num_threads = 0;
	}

	/* Make new thread pool */
	thpool_* thpool_p;
	thpool_p = (struct thpool_*)malloc(sizeof(struct thpool_));
	if (thpool_p == NULL){
		err("thpool_init(): Could not allocate memory for thread pool\n");
		return NULL;
	}
	thpool_p->num_threads_alive   = 0;
	thpool_p->num_threads_working = 0;

	/* Initialise the job queue */
	if (jobqueue_init(&thpool_p->queue_in) == -1){
		err("thpool_init(): Could not allocate memory for input job queue\n");
		free(thpool_p);
		return NULL;
	}

	if (jobqueue_init(&thpool_p->queue_out) == -1){
		err("thpool_init(): Could not allocate memory for output job queue\n");
		jobqueue_destroy(&thpool_p->queue_in);
		free(thpool_p);
		return NULL;
	}

	/* Make threads in pool */
	thpool_p->threads = (struct thread**)malloc(num_threads * sizeof(struct thread *));
	if (thpool_p->threads == NULL){
		err("thpool_init(): Could not allocate memory for threads\n");
		jobqueue_destroy(&thpool_p->queue_out);
		jobqueue_destroy(&thpool_p->queue_in);
		free(thpool_p);
		return NULL;
	}

	pthread_mutex_init(&(thpool_p->thcount_lock), NULL);
	pthread_cond_init(&thpool_p->threads_all_idle, NULL);

	/* Thread init */
	int ret;
	int n;
	for (n=0; n<num_threads; n++){
		ret = thread_init(thpool_p, &thpool_p->threads[n], n);
		if (ret) {
			thpool_destroy(thpool_p);
			return NULL;
		}
#if THPOOL_DEBUG
		printf("THPOOL_DEBUG: Created thread %d in pool \n", n);
#endif
	}

	/* Wait for threads to initialize */
	struct timespec ts;
	int wait_count = 0;

	ts.tv_sec  = 0;
	ts.tv_nsec = 100;
	while (thpool_p->num_threads_alive != num_threads){
		nanosleep(&ts, &ts);
		wait_count++;
		if (wait_count > 100000000){//Kludge to give 10 sec max wait
#if THPOOL_DEBUG
			printf("THPOOL_DEBUG: Timeout waiting for all pool threads to start\n");
#endif
			thpool_destroy(thpool_p);
			return NULL;
		}
	}

	return thpool_p;
}


/* Add work to the thread pool */
int thpool_add_work(thpool_* thpool_p, int job_uuid, th_func_p func_p, void* arg_p){
	job* newjob;

	newjob=(struct job*)malloc(sizeof(struct job));
	if (newjob==NULL){
		err("thpool_add_work(): Could not allocate memory for new job\n");
		return -1;
	}

	/* add function and argument */
	newjob->function=func_p;
	newjob->arg=arg_p;

	newjob->prev=NULL;
	newjob->uuid=job_uuid;

	/* add job to queue */
	jobqueue_push(&thpool_p->queue_in, newjob);

	return 0;
}

/* Extract result from thread pool */
int thpool_find_result(thpool_* thpool_p, int job_uuid, int retry_count_max, int retry_interval_ns, int* result_p){

	struct timespec ts;
	job* completed_job;
	int retry_count = 0;
	int result_found = 0;

	ts.tv_sec  = 0;
	ts.tv_nsec = retry_interval_ns;

	// printf("thpool_find_result(): getting result for uuid %d\n", job_uuid);
	while(retry_count < retry_count_max){

		completed_job = jobqueue_pull_by_uuid(&thpool_p->queue_out, job_uuid);
		if (completed_job){
		// 	printf("thpool_find_result(): retrieved job: uuid %d, %p\n", job_uuid, completed_job);
			*result_p = completed_job->result;
			free(completed_job);
			result_found = 1;
			break;
		}
		else{
		// 	printf("thpool_find_result(): No job found: uuid %d.  Sleeping\n", job_uuid);
			nanosleep(&ts, &ts);
		}
		retry_count++;
	}

	if (result_found) return 0;
	else              return -1;
}


/* Wait until all jobs have finished */
//TODO: Hardcoded for "thpool_p->queue_in".
//		Can "thpool_p->queue_out" even use this concept?
//				(queue_out NOT GUARENTEED to be emptied out via thpool_find_results())
//			If NOT, rename function?
void thpool_wait(thpool_* thpool_p){
	pthread_mutex_lock(&thpool_p->thcount_lock);
	while (thpool_p->queue_in.len || thpool_p->num_threads_working) {
		pthread_cond_wait(&thpool_p->threads_all_idle, &thpool_p->thcount_lock);
	}
	pthread_mutex_unlock(&thpool_p->thcount_lock);
}


/* Destroy the threadpool */
/* Retrieve any desired output before calling this destroy */
void thpool_destroy(thpool_* thpool_p){
	/* No need to destroy if it's NULL */
	if (thpool_p == NULL) return ;

	volatile int threads_total = thpool_p->num_threads_alive;

	/* End each thread 's infinite loop */
	threads_keepalive = 0;

	/* Give one second to kill idle threads */
	double TIMEOUT = 1.0;
	time_t start, end;
	double tpassed = 0.0;
	time (&start);
	while (tpassed < TIMEOUT && thpool_p->num_threads_alive){
		bsem_post_all(thpool_p->queue_in.has_jobs);
		time (&end);
		tpassed = difftime(end,start);
	}

	/* Poll remaining threads */
	while (thpool_p->num_threads_alive){
		bsem_post_all(thpool_p->queue_in.has_jobs);
		sleep(1);
	}

	/* Job queue cleanup */
	jobqueue_destroy(&thpool_p->queue_out);
	jobqueue_destroy(&thpool_p->queue_in);
	/* Deallocs */
	int n;
	for (n=0; n < threads_total; n++){
		thread_destroy(thpool_p->threads[n]);
	}
	free(thpool_p->threads);
	free(thpool_p);
}


/* Pause all threads in threadpool */
void thpool_pause(thpool_* thpool_p) {
	int n;
	for (n=0; n < thpool_p->num_threads_alive; n++){
		pthread_kill(thpool_p->threads[n]->pthread, SIGUSR1);
	}
}


/* Resume all threads in threadpool */
void thpool_resume(thpool_* thpool_p) {
    // resuming a single threadpool hasn't been
    // implemented yet, meanwhile this suppresses
    // the warnings
    (void)thpool_p;

	threads_on_hold = 0;
}


int thpool_num_threads_working(thpool_* thpool_p){
	return thpool_p->num_threads_working;
}


int thpool_queue_out_len(thpool_* thpool_p){
	return thpool_p->queue_out.len;
}




/* ============================ THREAD ============================== */


/* Initialize a thread in the thread pool
 *
 * @param thread        address to the pointer of the thread to be created
 * @param id            id to be given to the thread
 * @return 0 on success, -1 otherwise.
 */
//TODO: Change thread id to set internally???
static int thread_init (thpool_* thpool_p, struct thread** thread_p, int id){

	*thread_p = (struct thread*)malloc(sizeof(struct thread));
	if (*thread_p == NULL){
		err("thread_init(): Could not allocate memory for thread\n");
		return -1;
	}

	(*thread_p)->thpool_p = thpool_p;
	(*thread_p)->id       = id;

	pthread_create(&(*thread_p)->pthread, NULL, (void * (*)(void *)) thread_do, (*thread_p));
	pthread_detach((*thread_p)->pthread);
	return 0;
}


/* Sets the calling thread on hold */
static void thread_hold(int sig_id) {
    (void)sig_id;
	threads_on_hold = 1;
	while (threads_on_hold){
		sleep(1);
	}
}


/* What each thread is doing
*
* In principle this is an endless loop. The only time this loop gets interuppted is once
* thpool_destroy() is invoked or the program exits.
*
* @param  thread        thread that will run this function
* @return nothing
*/
static void* thread_do(struct thread* thread_p){

	/* Set thread name for profiling and debugging */
	char thread_name[16] = {0};
	snprintf(thread_name, 16, "thpool-%d", thread_p->id);
//TODO: Set thread "id" here instead (using pthread_self())????  Use both??
#if THPOOL_DEBUG
	printf("THPOOL_DEBUG: Starting Thread #%u\n", (int)pthread_self());
#endif

#if defined(__linux__)
	/* Use prctl instead to prevent using _GNU_SOURCE flag and implicit declaration */
	prctl(PR_SET_NAME, thread_name);
#elif defined(__APPLE__) && defined(__MACH__)
	pthread_setname_np(thread_name);
#else
	err("thread_do(): pthread_setname_np is not supported on this system");
#endif

	/* Assure all threads have been created before starting serving */
	thpool_* thpool_p = thread_p->thpool_p;

	/* Register signal handler */
	struct sigaction act;
	sigemptyset(&act.sa_mask);
	act.sa_flags = 0;
	act.sa_handler = thread_hold;
	if (sigaction(SIGUSR1, &act, NULL) == -1) {
		err("thread_do(): cannot handle SIGUSR1");
	}

	/* Mark thread as alive (initialized) */
	pthread_mutex_lock(&thpool_p->thcount_lock);
	thpool_p->num_threads_alive += 1;
	pthread_mutex_unlock(&thpool_p->thcount_lock);

	struct timespec ts;
	ts.tv_sec  = 0;
	ts.tv_nsec = 1;

	while(threads_keepalive){

		bsem_wait(thpool_p->queue_in.has_jobs);

		if (threads_keepalive){

			pthread_mutex_lock(&thpool_p->thcount_lock);
			thpool_p->num_threads_working++;
			pthread_mutex_unlock(&thpool_p->thcount_lock);

			/* Read job from queue and execute it */
			th_func_p func_buff;
			void*  arg_buff;
			job* job_p = jobqueue_pull_front(&thpool_p->queue_in);
			if (job_p) {
				func_buff     = job_p->function;
				arg_buff      = job_p->arg;
				job_p->result = func_buff(arg_buff);
				jobqueue_push(&thpool_p->queue_out, job_p);
			}

			pthread_mutex_lock(&thpool_p->thcount_lock);
			thpool_p->num_threads_working--;
			if (!thpool_p->num_threads_working) {
				pthread_cond_signal(&thpool_p->threads_all_idle);
			}
			pthread_mutex_unlock(&thpool_p->thcount_lock);

			nanosleep(&ts, &ts);     /* Allow other threads CPU time */
		}
	}
	pthread_mutex_lock(&thpool_p->thcount_lock);
	thpool_p->num_threads_alive --;
	pthread_mutex_unlock(&thpool_p->thcount_lock);

	return NULL;
}


/* Frees a thread  */
static void thread_destroy (thread* thread_p){
	free(thread_p);
}





/* ============================ JOB QUEUE =========================== */


/* Initialize queue */
static int jobqueue_init(jobqueue* jobqueue_p){
	int ret = -1;

	jobqueue_p->len = 0;
	jobqueue_p->front = NULL;
	jobqueue_p->rear  = NULL;

	jobqueue_p->has_jobs = (struct bsem*)malloc(sizeof(struct bsem));
	if (jobqueue_p->has_jobs == NULL){
		return ret;
	}

	pthread_mutex_init(&(jobqueue_p->rwmutex), NULL);
	ret = bsem_init(jobqueue_p->has_jobs, 0);

	return ret;
}


/* Clear the queue */
static void jobqueue_clear(jobqueue* jobqueue_p){

	while(jobqueue_p->len){
		free(jobqueue_pull_front(jobqueue_p));
	}

	jobqueue_p->front = NULL;
	jobqueue_p->rear  = NULL;
	bsem_reset(jobqueue_p->has_jobs);
	jobqueue_p->len = 0;
}


/* Add (allocated) job to queue
 */
static void jobqueue_push(jobqueue* jobqueue_p, struct job* newjob){

	// printf("          push: start: job(%p) to queue(%p) on thread #%u\n", newjob, jobqueue_p, (int)pthread_self());
	pthread_mutex_lock(&jobqueue_p->rwmutex);
	newjob->prev = NULL;

	switch(jobqueue_p->len){

		case 0:  /* if no jobs in queue */
			jobqueue_p->front = newjob;
			jobqueue_p->rear  = newjob;
			break;

		default: /* if jobs in queue */
			// printf("          push: dft: job(%p) to queue(%p) on thread #%u with rear(%p)\n", newjob, jobqueue_p, (int)pthread_self(), jobqueue_p->rear);
			jobqueue_p->rear->prev = newjob;
			jobqueue_p->rear = newjob;
	}
	jobqueue_p->len++;
	if (jobqueue_p->len > MAX_QUEUE_SIZE_WITHOUT_WARNING)
		printf("THPOOL_DEBUG: WARNING: queue len > %d\n",
		       MAX_QUEUE_SIZE_WITHOUT_WARNING);

	bsem_post(jobqueue_p->has_jobs);
	pthread_mutex_unlock(&jobqueue_p->rwmutex);
	// printf("          push: end: job(%p) to queue(%p) on thread #%u\n", newjob, jobqueue_p, (int)pthread_self());
	// printf("               push: job uuid %d\n", newjob->uuid);
}


/* Get first job from queue(removes it from queue)
 * Notice: Caller MUST hold a mutex
 */
static struct job* jobqueue_pull_front(jobqueue* jobqueue_p){

	pthread_mutex_lock(&jobqueue_p->rwmutex);
	job* job_p = jobqueue_p->front;

	switch(jobqueue_p->len){

		case 0:  /* if no jobs in queue */
			break;

		case 1:  /* if one job in queue */
			jobqueue_p->front = NULL;
			jobqueue_p->rear  = NULL;
			jobqueue_p->len = 0;
			break;

		default: /* if >1 jobs in queue */
			jobqueue_p->front = job_p->prev;
			jobqueue_p->len--;
			if (jobqueue_p->len > MAX_QUEUE_SIZE_WITHOUT_WARNING)
				printf("THPOOL_DEBUG: WARNING: queue len > %d\n",
				       MAX_QUEUE_SIZE_WITHOUT_WARNING);
			/* more than one job in queue -> post it */
			bsem_post(jobqueue_p->has_jobs);
	}

	pthread_mutex_unlock(&jobqueue_p->rwmutex);
	// printf("          pull_front: job(%p) from queue(%p)\n", job_p, jobqueue_p);
	// if (job_p)
	// 	printf("               pull_front: job uuid %d\n", job_p->uuid);
	return job_p;
}


/* Search for job uuid
 * Notice: Caller MUST hold a mutex
 */
// returned NULL indicates NOT FOUND
static struct job* jobqueue_pull_by_uuid(jobqueue* jobqueue_p, int job_uuid){

/*
TODO: Do I want to implement "trylock" like I did in POC?  If so, how?
		Pass in returned job pointer as param instead
		Use return value for error code from trylock
	Leave this work for AFTER it's in Propeller?
	WILL NEED: cuz drives may "vanish" unexpectedly.
		Don't want to endlessly wait for a drive that's no longer there.
*/
	pthread_mutex_lock(&jobqueue_p->rwmutex);

	job* curr_job_p = jobqueue_p->front;
	job* last_job_p = NULL;//Equivalent of curr_job_p->next, if double-linked list implemented. Code is scanning linked list "backwards"

	// printf("          main: pull_by_uuid: search start: uuid %d in queue(%p) with len %d\n", uuid, jobqueue_p, jobqueue_p->len);
	while (curr_job_p){
		// printf("                                             main: pull_by_uuid: curr_job_p(%p), curr_job_p->prev(%p), last_job_p(%p)\n", curr_job_p, curr_job_p->prev, last_job_p);
		if (curr_job_p->uuid == job_uuid){
			// printf("          main: pull_by_uuid: found uuid %d, %p\n", curr_job_p->uuid, curr_job_p);
			break;
		}
		last_job_p = curr_job_p;
		curr_job_p = curr_job_p->prev;
	}

	if (curr_job_p){
		switch (jobqueue_p->len){

			case 0:  /* if no jobs in queue */
				break;

			case 1:  /* if one job in queue */
				jobqueue_p->front = NULL;
				jobqueue_p->rear  = NULL;
				jobqueue_p->len = 0;
				break;

			default: /* if >1 jobs in queue */
				if (!last_job_p) {
					/* Current job at queue front */
					jobqueue_p->front = curr_job_p->prev;
				}
				else if (!curr_job_p->prev){
					/* Current job at queue rear */
					jobqueue_p->rear = last_job_p;
					last_job_p->prev = NULL;
				}
				else {
					/* Current job somewhere in the middle */
					last_job_p->prev = curr_job_p->prev;
				}

				jobqueue_p->len--;
				if (jobqueue_p->len > MAX_QUEUE_SIZE_WITHOUT_WARNING)
					printf("THPOOL_DEBUG: WARNING: queue len > %d\n",
					       MAX_QUEUE_SIZE_WITHOUT_WARNING);
				/* more than one job in queue -> post it */
				bsem_post(jobqueue_p->has_jobs);
		}
	}

	pthread_mutex_unlock(&jobqueue_p->rwmutex);
	// printf("          main: pull_by_uuid: job(%p) from queue(%p)\n", curr_job_p, jobqueue_p);
	// if (curr_job_p)
	// 	printf("               main: pull_by_uuid: job uuid %d\n", curr_job_p->uuid);
	return curr_job_p;
}


/* Free all queue resources back to the system */
static void jobqueue_destroy(jobqueue* jobqueue_p){
	jobqueue_clear(jobqueue_p);
	free(jobqueue_p->has_jobs);
}





/* ======================== SYNCHRONISATION ========================= */


/* Init semaphore to 1 or 0 */
static int bsem_init(bsem *bsem_p, int value) {
	if (value < 0 || value > 1) {
		err("bsem_init(): Binary semaphore can take only values 1 or 0");
		return -1;
	}
	pthread_mutex_init(&(bsem_p->mutex), NULL);
	pthread_cond_init(&(bsem_p->cond), NULL);
	bsem_p->v = value;

	return 0;
}


/* Reset semaphore to 0 */
static void bsem_reset(bsem *bsem_p) {
	bsem_init(bsem_p, 0);
}


/* Post to at least one thread */
static void bsem_post(bsem *bsem_p) {
	pthread_mutex_lock(&bsem_p->mutex);
	bsem_p->v = 1;
	pthread_cond_signal(&bsem_p->cond);
	pthread_mutex_unlock(&bsem_p->mutex);
}


/* Post to all threads */
static void bsem_post_all(bsem *bsem_p) {
	pthread_mutex_lock(&bsem_p->mutex);
	bsem_p->v = 1;
	pthread_cond_broadcast(&bsem_p->cond);
	pthread_mutex_unlock(&bsem_p->mutex);
}


/* Wait on semaphore until semaphore has value 0 */
static void bsem_wait(bsem* bsem_p) {
	pthread_mutex_lock(&bsem_p->mutex);
	while (bsem_p->v != 1) {
		pthread_cond_wait(&bsem_p->cond, &bsem_p->mutex);
	}
	bsem_p->v = 0;
	pthread_mutex_unlock(&bsem_p->mutex);
}
