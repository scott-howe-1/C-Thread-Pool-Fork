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
 * */

#include <stdio.h>
#include <pthread.h>
#include <stdint.h>
#include "thpool.h"

int task(void *arg){

	int result = (int)(intptr_t)arg  + 10;

	printf("Thread #%u: parm:%d, ret:%d\n",
	       (int)pthread_self(), (int)(intptr_t)arg, result);

	return result;
}


int main(){

	puts("Making threadpool with 4 threads");
	threadpool thpool = thpool_init(4);

	puts("\nAdding 40 tasks to threadpool");
	int i;
	int result;
	for (i=0; i<40; i++){
		thpool_add_work(thpool, i, task, (void*)(uintptr_t)i);
	};

	thpool_wait(thpool);

	puts("\nRetreiving 40 task results");
	for (i=0; i<40; i++){
		thpool_get_result(thpool, i, &result);
		printf("uuid:%d, result:%d\n", i, result);
	};

	thpool_wait(thpool);
	puts("\nKilling threadpool");
	thpool_destroy(thpool);

	return 0;
}
