#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <errno.h>
#include <libgen.h>
#include <math.h>

extern int errno;

#define __DEBUG__
#define MAX_LEN             256
#define ERROR_SUCCESS       0
#define THREAD_ROLE_MAPPER  0
#define THREAD_ROLE_REDUCER 1
#define ONE_IS_PERFECT_POWER     

typedef struct app_data_t* p_app_data_t;

struct linked_list_t
{
    struct linked_list_t *next;
    int data;
};

/*
    the main data structure for both of mapper and reducer thread
    let me explain the data structure for example in README.md

    Example of execution:
    For test0:

    /a.out 3 5 test.txt
    => there are 3 map threads
    => there are 5 reducer threads (for the 2,3,4,5,6 exponents)

    • Map0 → in1.txt
    • Map1 → in2.txt
    • Map2 → in3.txt, in4.txt

    So each Map will have 5 lists, one for each exponent

    • Map0 → {9, 81}, {27}, {81}, {243, 243, 243}, {}
    • Map1 → {81, 9}, {27, 27, 27}, {81}, {243}, {}
    • Map2 → {9, 9, 81, 9, 1}, {27, 1}, {81, 1}, {243, 243, 243, 1}, {1}

    Then the reducers will have:

    • Reduce0 → {9, 81, 81, 9, 9, 9, 81, 9, 1}
    • Reduce1 → {27, 27, 27, 27, 27, 1}
    • Reduce2 → {81, 81, 81, 1}
    • Reduce3 → {243, 243, 243, 243, 243, 243, 243, 1}
    • Reduce4 → {1}

    - every mapper thread have array of perfect powers for given exponential 
        thread (map 0) has 5 lists for exponent 2, 3, 4, 5, 6 
           perfect_powers[0] -> || -> ||  -> NULL               ... perfect numbers for exponential 2  
                                9     81
           perfect_powers[1] -> || -> NULL                      ... perfect numbers for exponential 3                        
                                27   
           perfect_powers[2] -> || -> || -> NULL                ... perfect numbers for exponential 4
                                81
           perfect_powers[3] -> || -> || -> || -> NULL          ... perfect numbers for exponential 5
                                243   243   243
           perfect_powers[4] -> NULL                            ... perfect numbers for exponential 6

        thread (map 1) also has 5 lists for exponent 2, 3, 4, 5, 6 
           perfect_powers[0] -> || -> || -> NULL                 
                                81    9
           perfect_powers[1] -> || -> || -> || -> NULL                        
                                27    27    27
           perfect_powers[2] -> || -> || -> NULL
                                81
           perfect_powers[3] -> || -> || -> || -> NULL
                                243   243   243
           perfect_powers[4] -> NULL

    - every reducer thread have pointers to perfect powers of mapper thead  
        thread (reducer 0) has 3 lists for each mapper (in this case, 3 mappers)]
        in that way, 
                                mapper[0]
                                    |
           perfect_powers[0] -> perfect_powers[0]

                                mapper[1]
                                    |
           perfect_powers[1] -> perfect_powers[0]

                                mapper[2]
                                    |
           perfect_powers[2] -> perfect_powers[0]

*/
struct thread_data_t
{
    int         index;              // thread index [0, number_of_mapper] and [0, number_of_reducer]
    int         role;               // THREAD_ROLE_MAPPER or THREAD_ROLE_REDUCER
    int         file_count;         // count of file to be processed. in some case, single mapper thread might handle multiple input data file 
    char        **file_names;       // array of file name
    int         total_numbers;      // total number of input numbers
    int         exponents;          // max exponents (its determined by number_of_reducers + 2.) 
    struct linked_list_t *perfect_powers; // linked list to store all perfect numbers
    pthread_t   p_thread;           // pthread
    p_app_data_t app;               // pointer to app data
};

struct app_data_t
{
    int         number_of_mappers;      // number of mapper threads
    int         number_of_reducers;     // number of reducer threads
    int         number_of_threads;      // total number of threads, it's just same to (number_of_mappers + number_of_reducers)
    char        *file_name;             // test file name
    int         number_of_files;
    char        **file_links;
    
    // data structure for both of mapper and reducer threads
    struct thread_data_t* threads;

    // mutex and condition variable to synchronize mapper and reducer threads
    // 
    int             mapper_count;       // number of mapper threads which is terminated
    pthread_mutex_t count_mutex;        // mutex for count variable
    pthread_mutex_t condition_mutex;    // mutex for condition variable
    pthread_cond_t  condition_cond;     // condition variable
};

// function prototype
int get_number_of_files(const char *file_name);
int mapper_func(struct thread_data_t* td);
int reducer_func(struct thread_data_t* td);
int check_perfect_power(int num);
void* thread_func(void * input);
int allocate_maps_and_reducers(struct app_data_t *app);
int allocate_files_to_maps(struct app_data_t *app);
int read_files(struct app_data_t* app);
void thread_init(struct thread_data_t* td, int index, int role);
void map_free(struct thread_data_t* td);
struct app_data_t *app_alloc(int number_of_mappers, int number_of_reducers, char *file_name);
void app_free(struct app_data_t *app);

/*
char *basename(char const *path)
{
    char *s = strrchr(path, '/');
    if (!s)
        return strdup(path);
    else
        return strdup(s + 1);
}
*/

/*
    Get number of files and file names from input file 
    file_name: input file name
*/
int get_number_of_files(const char *file_name)
{
    int value = 0;
    
    FILE *test_file;
   
    test_file = fopen(file_name,"r");
    
    char first_line[100];
    
    if (test_file == NULL)
    {
#ifdef __DEBUG__
        int errnum = errno;
        printf("get_number_of_files(): File not found %s, error %d, %s!\n", file_name, errnum, strerror( errnum ));
        perror("Error printed by perror");
#endif
        return -1;
    }
    
    int count = 0;
    
    //Reading test file for only 1st line and getting no of file links and saving file links in array
    while((fgets(first_line, 100, test_file) != NULL) && count < 1)
    {
        if(count < 1)
        {
            value = atoi(first_line);
        }
       
        count++;
    }
    
    fclose(test_file);
    
    return value;
}

/*
*/
int read_numbers_from_file(char* file_name, int* nums, int size)
{
    FILE *input_file = NULL;
    char line[100];
    
    input_file = fopen(file_name, "r");
    if (input_file == NULL)
    {
        return -1;
    }

    //Reading from line by line from file and storing links in an int array
    int count = 0, start = 0;
    
    while (fgets(line, 100, input_file) != NULL && count <= size)
    {
        if (line[strlen(line)] == '\n')
        {
            line[strlen(line)] = '\0'; 
        }
        if (line[strlen(line) - 1] == '\n')
        {
            line[strlen(line) - 1] = '\0'; 
        }
        if(start == 0)
        {
            start++;
        }
        else
        {
            nums[count] = atoi(line);
            count++;
        }
    }

    fclose(input_file);

    return count;
}

int write_numbers_to_file(char* file_name, int num)
{
    FILE *output_file = NULL;
    
    output_file = fopen(file_name, "w");
    if (output_file == NULL)
    {
        return -1;
    }

    fprintf(output_file, "%d", num);
    fclose(output_file);

    return 0;
}

void prime_factors(int num)  
{  
    int count;  
  
    printf("\nPrime Factors of %d are ..\n", num);  
    for(count = 2; num > 1; count++)  
    {  
        while(num % count == 0)  
        {  
            printf("%d ", count);  
            num = num / count;  
        }  
    }  
    printf("\n");  
}  

int _pow(int a, int b)
{
    long n = 1;
    for (int i = 0; i < b; i++) n *= a;
    return n;
}

int check_perfect_power_fast(int num)
{
#ifdef ONE_IS_PERFECT_POWER
    if (num == 1) return 1;
#endif

    double range = log(num) / log(2);
    long n = (long)(range) + 1;

    for (int k = n - 1; 1 < k; k--)
    {
        long a = exp(log(num) / k);
        for (int b = a - 1; b < a + 2; b++)
        {
            if (_pow(b, k) == num)
            {
                return k;
            }
        }
    } 

    return 0;
}

/*
*/
int check_perfect_power(int num)
{
#ifdef ONE_IS_PERFECT_POWER
    if (num == 1) return 1;
#endif

    //Checking if the number is perfect power
    for (long i = 2; i * i <= num ; i++)
    {
        if (i == num)
        {
            return 1;
        }
        long j = i;
        int k = 1;
        while (j <= num)
        {
            j *= i;
            k++;
            if (j == num)
            {
                return k;
            }
        }
    }
    
    return 0;
}

void list_print(struct linked_list_t* list)
{
    if (list == NULL) return;

    printf("{[%d]: ", list->data);

    struct linked_list_t *item = list->next;
    while (item)
    {
        printf("(%d),", item->data);
        item = item->next;
    }
    printf("}");
}

struct linked_list_t* list_append(struct linked_list_t* list, int data)
{
    if (list == NULL) return NULL;

    struct linked_list_t* item = list;
    struct linked_list_t* new_item = (struct linked_list_t*)malloc(sizeof(struct linked_list_t));

    new_item->next = NULL;
    new_item->data = data;

    while (item->next)
        item = item->next;
    item->next = new_item;

    // increase count
    list->data++;

    return new_item;
}

void list_free(struct linked_list_t* list)
{
    if (list == NULL) return;

    struct linked_list_t* item = list->next;
    while (item)
    {
        struct linked_list_t* p = item->next;
        free(item);
        item = p;
    }
}

/*
    mark duplicated items as deleted (-1)
*/
void mark_duplicates(int *nums, int n, int start, int end)
{
    //Setting all same nos to -1
    for (int k = start; k < end; k++)
    {
        if (nums[k] == n)
        {
            nums[k] = -1; //setting the number as -1 in case its not a perfect power
        }
    }
}

/*
*/
int mapper_func(struct thread_data_t* td)
{
    // process each input file for this mapper
    int total_numbers = 0;
    int count = 0;
    for (int i = 0; i < td->file_count; i++)
    {
        count = get_number_of_files(td->file_names[i]);
        if (count == -1)
        {
#ifdef __DEBUG__
            printf("mapper %d: error in reading file #%d %s\n", td->index, i, td->file_names[i]);
#endif
        }
        else
        {
            total_numbers += count;
        }
    }

    int *nums = malloc(sizeof(int) * total_numbers);
    int final_count = 0;

    for (int i = 0; i < td->file_count; i++)
    {
        final_count += read_numbers_from_file(td->file_names[i], &nums[final_count], total_numbers - final_count);
    }

    // testing array
    for (int j = 0; j < final_count; j++)
    {
        if (nums[j] == -1)
        {
            continue;
        }

        // there are 2 function which check perfect number  
        // please use check_perfect_power_fast() instead of check_perfect_power() since its more fast
        // we store perfect powers in the linked list instead of array
        // because there are only few perfect powers in lot of data.
        // its more efficient than array.
        int power_check = check_perfect_power_fast(nums[j]);
        if (!power_check)
        {
            continue;
        }
        else
        {
            for (int n = 2; n < td->exponents; n++)
            {
#ifdef ONE_IS_PERFECT_POWER
                if (power_check == 1)
                {
                    list_append(&(td->perfect_powers[n]), nums[j]);
                }
#endif                
                if (power_check % n == 0)
                {
                    list_append(&(td->perfect_powers[n]), nums[j]);
                }
            }
        }
    }
    
    // reduce_func(nums, final_count);
#ifdef __DEBUG__
    printf("mapper %d: -------------\n", td->index);
    for (int i = 2; i < td->exponents; i++)
    {
        printf("#exp %d ", i);
        list_print(&(td->perfect_powers[i]));
        printf("\n");
    }
#endif

    free(nums);

    return 0;
}

/*
    every reducer thread will check list of perfect powers which are found in mapper threads

    params:
        td: pointer of struct thread_data_t
    return:
        0 - if success, otherwise error code
*/
int reducer_func(struct thread_data_t* td)
{
    // calculating count of unique numbers in array
    int final_count = 0;

    // get total count of perfect powers for a given exponent 
    for (int i = 0; i < td->app->number_of_mappers; i++)
    {
        int exp = td->index + 2;    // exponent for the reducer id
        final_count += td->perfect_powers[i].next[exp].data;
    }

    // allocate array for perfect powers and move data from the list in mapper to the array
    int *nums = malloc(sizeof(int) * final_count);
    int count = 0;
    for (int i = 0; i < td->app->number_of_mappers; i++)
    {
        int exp = td->index + 2;
        struct linked_list_t *list = &(td->perfect_powers[i].next[exp]);
        list = list->next;
        while (list)
        {
            nums[count] = list->data; 
            count++;
            list = list->next;
        }
    }

#ifdef __DEBUG__
    if (final_count != count)
    {
        printf("reducer %d: get %d numbers, expecting %d\n", td->index, count, final_count);
    }
#endif

    // remove all duplicates from array and count unique numbers
    // mark duplicated numbers as -1
    count = 0;
    for(int i = 0; i < final_count; i++)
    {
        if(nums[i] == -1)
        {
            continue;;
        }
        count++;
        int num = nums[i];
        //Replacing all other values than num as -1
        for(int j = i + 1; j < final_count; j++)
        {
            if (nums[j] == -1)
            {
                continue;
            }
            if (num == nums[j])
            {
                nums[j] = -1;
            }
        }
    }

#ifdef __DEBUG__
    printf("reducer %d: exp %d count %d (%d)\n", td->index, td->index + 2, count, final_count);
    for (int i = 0; i < final_count; i++)
    {
        if (nums[i] == -1)
        {
            continue;
        }
        printf("%d ", nums[i]);
    }
    printf("\n");
#endif

    // print out result
    char file_name[MAX_LEN];
    
    snprintf(file_name, MAX_LEN, "out%d.txt", td->index + 2);
    write_numbers_to_file(file_name, count);

    free(nums);

    return 0;
}

/*
    run mapper or reducer function upon role of the thread
    please refer to https://www.cs.cmu.edu/afs/cs/academic/class/15492-f07/www/pthreads.html (Condition Variables) about multi thread synchronization

- Mapper thread:
    All mapper threads find a perfect powers from input data and store them to linked list.
    These data will be used by reducer threads later.

- Reducer thread:
    All reducer threads can process data once all mapper threads done their job.
    So they wait for condition which the value of mapper_count reach to number_of_mappers.
    Every mapper thread increase mapper_count by 1 when it finished, so mapper_count will be number_of_mappers (for example) 
    when all mapper threads are finished.   
    Each reducer will process perfect numbers for its own exponential.
    
    For example,
        reducer[0] will process perfect numbers for exponential 2 
        reducer[1] for exponential 3, reducer[2] for exponential 3 and so on 
    They don't need to have a list for them.  

    input: thread_data_t

*/
void* thread_func(void * input)
{
    int ret = 0;
    struct thread_data_t* td = (struct thread_data_t*)input;
   
    if (td->role == THREAD_ROLE_MAPPER)
    {
        // process input data and store them in linked list
        ret = mapper_func(td);

        // increase count of finished mapper
        pthread_mutex_lock( &(td->app->count_mutex) );
        {
            td->app->mapper_count++; 
        }
        pthread_mutex_unlock( &(td->app->count_mutex) );        

        // broadcast signal to wake up all reducer threads if value of mapper_count reach to number_of_mappers
        pthread_mutex_lock( &(td->app->condition_mutex) );
        if (td->app->mapper_count == td->app->number_of_mappers)
        {
            pthread_cond_broadcast( &(td->app->condition_cond) );
        }
        pthread_mutex_unlock( &(td->app->condition_mutex) );
    }
    else if (td->role == THREAD_ROLE_REDUCER)
    {
        // wait the signal for wait condition.
        // in this case, the reducer wait till mapper_count reach to number_of_mappers   
        pthread_mutex_lock( &(td->app->condition_mutex) );
        while( td->app->mapper_count < td->app->number_of_mappers )
        {
            pthread_cond_wait( &(td->app->condition_cond), &(td->app->condition_mutex) );
        }
        pthread_mutex_unlock( &(td->app->condition_mutex) );

        // get data from linked list of mapper thread and process it and save result to file
        ret = reducer_func(td);
    }

    pthread_exit(&ret);
}

/*
    params: 
        app:
    return:
*/
int allocate_maps_and_reducers(struct app_data_t *app)
{
    for(int i = 0; i < app->number_of_threads; i++)
    {
        pthread_create(&app->threads[i].p_thread, NULL, thread_func, (void *)(&app->threads[i]));
    }

    for (int i = 0; i < app->number_of_threads; i++)
    {
        pthread_join(app->threads[i].p_thread, NULL);
    }    

    return 0;
}

/*
*/
int allocate_files_to_maps(struct app_data_t *app)
{
    // Setting files per map based on number of files
    int files_per_map = app->number_of_files <= app->number_of_mappers ? 1 : app->number_of_files / app->number_of_mappers;
    int files_remain = app->number_of_files % app->number_of_mappers;

    // Defining arrays to be passed to Maps for processing
    int file_count = 0;
    int map_index = 0;

    app->threads[map_index].file_count = 0;
    app->threads[map_index].file_names = malloc(sizeof(char *) * files_per_map);
 
    for (int i = 0; i < app->number_of_files; i++)
    {
        if (file_count < files_per_map)
        {
            app->threads[map_index].file_names[file_count] = app->file_links[i];
        }

        if (file_count == (files_per_map - 1))
        {
            app->threads[map_index].file_count = files_per_map; 
            file_count = 0;
            map_index++;

            if (map_index < app->number_of_mappers)
            {
                if ((map_index + files_remain) == app->number_of_mappers)
                {
                    files_per_map++;
                }

                app->threads[map_index].file_count = 0;
                app->threads[map_index].file_names = malloc(sizeof(char *) * files_per_map);
            }
        }
        else
        {
            file_count++;
        }
    }

    return 0;
}

/*
    Read input file and get file links to be processed
    
    params:
        file_name:
        number_of_files: 
        file_links: 

    return:   
*/
int read_files(struct app_data_t* app)
{
    FILE *test_file = NULL;
   
    test_file = fopen(app->file_name, "r");
    if (test_file == NULL)
    {
        printf("Input file not found...terminating!\n");
        return -1;
    }
    
    char line[100] = "";
    int count = 0, start = 0;
    
    // Reading from line by line from file and storing links in an array file_links
    while (fgets(line, 100, test_file) != NULL && count < app->number_of_files)
    {
        if (line[strlen(line)] == '\n')
        {
            line[strlen(line)] = '\0'; 
        }
        if (line[strlen(line) - 1] == '\n')
        {
            line[strlen(line) - 1] = '\0'; 
        }

        if (start == 0)
        {
            start++;
        }
        else
        {
            strcpy(app->file_links[count], line);
            count++;
        }
    }
      
    fclose(test_file);

    return ERROR_SUCCESS;
}

/*
*/
void thread_init(struct thread_data_t* td, int index, int role)
{
    td->index = index;
    td->role = role;
    td->file_count = 0;
    td->file_names = NULL;
    td->perfect_powers = NULL;
}

void thread_free(struct thread_data_t* td)
{
    if (td->role == THREAD_ROLE_MAPPER)
    {
        list_free(td->perfect_powers);
    }
    free(td->perfect_powers);
}

/*
    params:
        number_of_mappers: number of mappers to be run
        number_of_reducers: number of reducers to be run
    result:    
*/
struct app_data_t *app_alloc(int number_of_mappers, int number_of_reducers, char *file_name)
{
    struct app_data_t* app = NULL;
    
    char *file_path = strdup(file_name);

    // get number of files from input file    
    int number_of_files = get_number_of_files(file_name);
    if (number_of_files == -1) {
        printf("invalid number of files or corrupted input file\n");
        return NULL;
    }

    app = malloc(sizeof(struct app_data_t));

    // initialize mutex & condition
    app->count_mutex     = PTHREAD_MUTEX_INITIALIZER;
    app->condition_mutex = PTHREAD_MUTEX_INITIALIZER;
    app->condition_cond  = PTHREAD_COND_INITIALIZER;
    app->mapper_count    = 0;

    int exponents = number_of_reducers + 2;

    // allocate thread data 
    int number_of_threads = number_of_mappers + number_of_reducers;
    app->threads = malloc(sizeof(struct thread_data_t) * number_of_threads);

    // initialize thread data of mapper and reducer
    for (int i = 0; i < number_of_threads; i++)
    {
        struct thread_data_t *td = &(app->threads[i]); 

        // if index < number_of_mappers, it's mapper thread 
        if (i < number_of_mappers)
        {
            // initialize mapper thread with index and THREAD_ROLE_MAPPER 
            thread_init(td, i, THREAD_ROLE_MAPPER);

            // allocated list of perfect numbers for exponent and initialize them
            td->perfect_powers = malloc(sizeof(struct linked_list_t) * exponents);
            for (int j = 0; j < exponents; j++)
            {
                td->perfect_powers[j].next = NULL;
                td->perfect_powers[j].data = 0;    // count of list
            }
        }
        // otherwise it's reducer thread 
        else
        {
            // initialize reducer thread with index and THREAD_ROLE_REDUCER  
            thread_init(td, i - number_of_mappers, THREAD_ROLE_REDUCER);

            // allocated list of perfect numbers for mappers and initialize them
            td->perfect_powers = malloc(sizeof(struct linked_list_t) * number_of_mappers);
            for (int j = 0; j < number_of_mappers; j++)
            {
                // point to list of perfect numbers of mapper thread
                td->perfect_powers[j].next = app->threads[j].perfect_powers;
                td->perfect_powers[j].data = 0;
            }
        }     
        
        td->exponents = exponents;
        td->app = app;
   }

    // initialize file_links
    app->file_links = malloc(sizeof(char*) * number_of_files);
    for (int i = 0; i < number_of_files; i++)
    {
        app->file_links[i] = (char*)malloc(MAX_LEN);
    }

    app->number_of_mappers = number_of_mappers;
    app->number_of_reducers = number_of_reducers;
    app->number_of_threads = number_of_threads;
    app->file_name = file_name;
    app->number_of_files = number_of_files;

    return app;
}

/*
*/
void app_free(struct app_data_t *app)
{
    for (int i = 0; i < app->number_of_files; i++)
    {
        free(app->file_links[i]);
    }
    free(app->file_links);
 
    for (int i = 0; i < app->number_of_threads; i++)
    {
        thread_free(&app->threads[i]);
    }
    free(app->threads);

    free(app);
}

int main(int argc, const char * argv[])
{
    time_t now, later;
    double seconds;
    time(&now);

    if (argc < 4)
    {
        printf("arguments: NUMBER_OF_MAPPERS NUMBER_OF_REDUCERS FILE_NAME\n");
        return -1;
    }

    int number_of_mappers = atoi(argv[1]);
    int number_of_reducers = atoi(argv[2]);
    char *file_name = (char *)argv[3];

    struct app_data_t* app = app_alloc(number_of_mappers, number_of_reducers, file_name);

    // read file and all the arguments
    int ret = read_files(app);
    if (ret == -1)
    {
        return -1;
    }

    ret = allocate_files_to_maps(app);
    
    // allocating mappers and reducers
    ret = allocate_maps_and_reducers(app);

    // free app
    app_free(app);
    
    // calculate time elapsed 
    time(&later);
    seconds = difftime(later, now);

    printf("\n%.f seconds elapsed\n", seconds);

    return 0;
}