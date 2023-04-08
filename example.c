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
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "thpool.h"

int task(void *arg){

	int result = (int)(intptr_t)arg  + 100;

	printf("Thread #%u: uuid:%d, ret:%d\n",
	       (int)pthread_self(), (int)(intptr_t)arg, result);

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
	int num_timeouts=0;

	srand( time(NULL) );

	gen_random_numbers(arr_uuid, NUM_TASKS, 1, NUM_TASKS);
	// gen_numbers(arr_uuid, NUM_TASKS);

	printf("Main Thread #%u\n", (int)pthread_self());
	int j;
	for (j=0; j<NUM_LOOPS; j++){
		puts("Making threadpool with 4 threads");
		threadpool thpool = thpool_init(4);

		puts("\nAdding tasks to threadpool");
		int i;
		int result;
		for (i=0; i<NUM_TASKS; i++){
			thpool_add_work(thpool, arr_uuid[i], task, (void*)(uintptr_t)arr_uuid[i]);
		};

		// thpool_wait(thpool);

		puts("\nRetreiving results from threadpool");
		int ret;
		for (i=0; i<NUM_TASKS; i++){
			ret = thpool_get_result(thpool,
			                        arr_uuid[i],
			                        QUEUE_OUT_JOB_UUID_SEARCH_COUNT_MAX,
			                        QUEUE_OUT_JOB_UUID_WAIT_INTERVAL_NSEC,
			                        &result);
			printf("main: received result %d for uuid %d with ret %d\n", result, arr_uuid[i], ret);
			if (result != arr_uuid[i]+100){
				if (ret == -1) num_timeouts++;
				else           num_errs++;
			}
		}

		thpool_wait(thpool);
		puts("\nKilling threadpool");
		thpool_destroy(thpool);

		printf("main: completed loop %d\n", j);
		// if (num_errs || num_timeouts)
		// 	break;
	}
	printf("main: num_timeouts = %d\n", num_timeouts);
	printf("main: num_errs = %d\n", num_errs);

	return 0;
}
