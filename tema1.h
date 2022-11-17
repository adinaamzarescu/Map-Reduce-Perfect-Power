#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <errno.h>
#include <libgen.h>
#include <math.h>

#define __DEBUG__
#define MAX_LEN             256
#define ERROR_SUCCESS       0
#define THREAD_ROLE_MAPPER  0
#define THREAD_ROLE_REDUCER 1
#define ONE_IS_PERFECT_POWER    

typedef struct app_data_t* p_app_data_t;

struct thread_data_t
{
    int         index;              // thread index [0, number_of_mapper] and [0, number_of_reducer]
    int         role;               // THREAD_ROLE_MAPPER or THREAD_ROLE_REDUCER
    int         file_count;         // count of files to be processed. in some case, single mapper thread might handle multiple input data file 
    char        **file_names;       // array of file name
    int         total_numbers;      // total number of input numbers
    int         exponents;          // max exponents (its determined by number_of_reducers + 2) 
    struct linked_list_t *perfect_powers; // linked list to store all perfect numbers
    pthread_t   p_thread;           // pthread
    p_app_data_t app;               // pointer to app data
};

struct app_data_t
{
    int         number_of_mappers;      // number of mapper threads
    int         number_of_reducers;     // number of reducer threads
    int         number_of_threads;      // total number of threads (number_of_mappers + number_of_reducers)
    char        *file_name;             // test file name
    int         number_of_files;
    char        **file_links;
    
    // data structure for both of mapper and reducer threads
    struct thread_data_t* threads;

    // mutex and condition variable to synchronize mapper and reducer threads
    int             mapper_count;       // number of terminated mapper threads
    pthread_mutex_t count_mutex;        // mutex for count variable
    pthread_mutex_t condition_mutex;    // mutex for condition variable
    pthread_cond_t  condition_cond;     // condition variable
};

// Functions
int _pow(int a, int b);
int check_perfect_power_fast(int num);
void mark_duplicates(int *nums, int n, int start, int end);
int mapper_func(struct thread_data_t* td);
int reducer_func(struct thread_data_t* td);
void* thread_func(void * input);
int allocate_maps_and_reducers(struct app_data_t *app);
int allocate_files_to_maps(struct app_data_t *app);
void thread_init(struct thread_data_t* td, int index, int role);
void thread_free(struct thread_data_t* td);
struct app_data_t *app_alloc(int number_of_mappers, int number_of_reducers, char *file_name);
void app_free(struct app_data_t *app);
