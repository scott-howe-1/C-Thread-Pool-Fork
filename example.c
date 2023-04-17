/*
 * WHAT THIS EXAMPLE DOES
 *
 * We create a pool of 4 threads and then add 40 tasks to the pool(20 task1
 * functions and 20 task2 functions). task1 and task2 simply print which thread is running them.
 *
 * As soon as we add the tasks to the pool, the threads will run them. It can happen that
 * you see a single thread running all the tasks (highly unlikely). It is up the OS to
 * decide which thread will run what. So it is not an error of the thread pool but rather
 * a decision of the OS.
 *
 * CLI compile:
 * gcc example.c thpool.c -D THPOOL_DEBUG -pthread -o example
 * */

#include <stdio.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <time.h>
#include "thpool.h"

int task(void *arg){

	int result = (int)(intptr_t)arg  + 100;

	printf("%s: (pthread:%u) uuid:%d, ret:%d\n",
	       __func__, (int)pthread_self(), (int)(intptr_t)arg, result);

	return result;
}

#define NUM_LOOPS 100
#define NUM_TASKS 100

void gen_random_numbers(int array[], int len, int min, int max){
	int i;
	for (i = 0; i < len; i++)
		array[i] = rand() % (max - min + 1) + min;
}

void gen_numbers(int array[], int len){
	int i;
	for (i = 0; i < len; i++)
		array[i] = i;
}

#define QUEUE_OUT_JOB_UUID_SEARCH_COUNT_MAX	10000
#define QUEUE_OUT_JOB_UUID_WAIT_INTERVAL_NSEC	10000

int main(){
	int arr_uuid[NUM_TASKS];
	int num_errs=0;
	int num_mismatches=0;
	int ret;

	srand( time(NULL) );

	gen_random_numbers(arr_uuid, NUM_TASKS, 1, NUM_TASKS);
	// gen_numbers(arr_uuid, NUM_TASKS);

	printf("%s: pthread:%u\n", __func__, (int)pthread_self());
	int j;
	for (j=0; j<NUM_LOOPS; j++){
		printf("%s: Making threadpool with 4 threads\n", __func__);
		threadpool thpool = thpool_init(4);
		if (thpool == NULL){
			printf("%s: Threadpool init failed on loop %d", __func__, j);
			return -1;
		}

		printf("\n%s: Adding tasks to threadpool\n", __func__);
		int i;
		int result;
		for (i=0; i<NUM_TASKS; i++){
			ret = thpool_add_work(thpool, arr_uuid[i], task, (void*)(uintptr_t)arr_uuid[i]);
			if (ret){
				// printf("%s: Threadpool add work failed on task %d", __func__, i);
				return -1;
			}
		};

		// thpool_wait(thpool);

		printf("\n%s: Retreiving results from threadpool\n", __func__);
		for (i=0; i<NUM_TASKS; i++){
			ret = thpool_find_result(thpool,
			                        arr_uuid[i],
			                        QUEUE_OUT_JOB_UUID_SEARCH_COUNT_MAX,
			                        QUEUE_OUT_JOB_UUID_WAIT_INTERVAL_NSEC,
			                        &result);
			printf("%s: received result %d for uuid %d with ret %d\n",
			       __func__, result, arr_uuid[i], ret);
			if (ret){
				num_errs++;
			}
			else if (result != task((void*)(intptr_t)arr_uuid[i])){
				num_mismatches++;
			}
		}

		thpool_wait(thpool);
		printf("\n%s: Killing threadpool\n", __func__);
		thpool_destroy(thpool);

		printf("%s: completed loop %d\n", __func__, j);
	}
	printf("%s: num_mismatches = %d\n", __func__, num_mismatches);
	printf("%s: num_errs = %d\n", __func__, num_errs);

	return 0;
}
