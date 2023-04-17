#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include <stdlib.h>
#include "../../thpool.h"

#define INCREMENT_SIZE 1

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
int sum_threads=0;


int increment() {
	pthread_mutex_lock(&mutex);
	sum_threads += INCREMENT_SIZE;
	pthread_mutex_unlock(&mutex);
	return INCREMENT_SIZE;
}


int main(int argc, char *argv[]){

	char* p;
	if (argc != 5){
		puts("This testfile needs exactly 4 arguments");
		exit(1);
	}
	int num_jobs    = strtol(argv[1], &p, 10);
	int num_threads = strtol(argv[2], &p, 10);
	int use_results = strtol(argv[3], &p, 10);
	int use_wait    = strtol(argv[4], &p, 10);

	threadpool thpool = thpool_init(num_threads);

	int n;
	for (n=0; n<num_jobs; n++){
		thpool_add_work(thpool, n, (void*)increment, NULL);
	}

	if (use_wait)
		thpool_wait(thpool);

	if (use_results){
		int result;
		int sum_results=0;
		int ret;
		for (n=0; n<num_jobs; n++){
			ret = thpool_find_result(thpool, n, 10000, 10000, &result);
			printf("ret=%d\n", ret);
			sum_results += result;
		}

		printf("%d\n", sum_results);
	}
	printf("%d\n", sum_threads);

	return 0;
}
