#include "tema1.h"
#include "linked_list.h"
#include "process_files.h"

extern int errno;

int _pow(int a, int b)
{
    long n = 1;
    for (int i = 0; i < b; ++i) n *= a;
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
    mark duplicated items as deleted (-1)
*/
void mark_duplicates(int *nums, int n, int start, int end)
{
    // Setting all same nos to -1
    for (int k = start; k < end; k++)
    {
        if (nums[k] == n)
        {
            nums[k] = -1; // Setting the number as -1 in case its not a perfect power
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
    for (int i = 0; i < td->file_count; ++i)
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

    for (int i = 0; i < td->file_count; ++i)
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
        // The perfect powers are stored in the linked list instead of array
        // because there are only few perfect powers in lot of data.
        // It is more efficient than array.
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
#ifdef __DEBUG__
    printf("mapper %d: -------------\n", td->index);
    for (int i = 2; i < td->exponents; ++i)
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
    Every reducer thread will check list of perfect powers which are found in mapper threads

    params:
        td: pointer of struct thread_data_t
    return:
        0 - if success, otherwise error code
*/
int reducer_func(struct thread_data_t* td)
{
    // Calculating count of unique numbers in array
    int final_count = 0;

    // Get total count of perfect powers for a given exponent 
    for (int i = 0; i < td->app->number_of_mappers; ++i)
    {
        int exp = td->index + 2;    // exponent for the reducer id
        final_count += td->perfect_powers[i].next[exp].data;
    }

    // Allocate array for perfect powers and move data from the list in mapper to the array
    int *nums = malloc(sizeof(int) * final_count);
    int count = 0;
    for (int i = 0; i < td->app->number_of_mappers; ++i)
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

    // Remove all duplicates from array and count unique numbers
    // mark duplicated numbers as -1
    count = 0;
    for(int i = 0; i < final_count; ++i)
    {
        if(nums[i] == -1)
        {
            continue;;
        }
        count++;
        int num = nums[i];
        // Replacing all other values than num as -1
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
    for (int i = 0; i < final_count; ++i)
    {
        if (nums[i] == -1)
        {
            continue;
        }
        printf("%d ", nums[i]);
    }
    printf("\n");
#endif

    // Print out result
    char file_name[MAX_LEN];
    
    snprintf(file_name, MAX_LEN, "out%d.txt", td->index + 2);
    write_numbers_to_file(file_name, count);

    free(nums);

    return 0;
}

/*
    run mapper or reducer function upon role of the thread
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

        // broadcast signal to wake up all reducer threads if value of mapper_count reaches the number_of_mappers
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
        // in this case, the reducer will wait util mapper_count reaches the number_of_mappers
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
    for(int i = 0; i < app->number_of_threads; ++i)
    {
        pthread_create(&app->threads[i].p_thread, NULL, thread_func, (void *)(&app->threads[i]));
    }

    for (int i = 0; i < app->number_of_threads; ++i)
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
 
    for (int i = 0; i < app->number_of_files; ++i)
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
        number_of_mappers: number of mappers to run
        number_of_reducers: number of reducers to run
    result:    
*/
struct app_data_t *app_alloc(int number_of_mappers, int number_of_reducers, char *file_name)
{
    struct app_data_t* app = NULL;

    // get number of files from input file    
    int number_of_files = get_number_of_files(file_name);
    if (number_of_files == -1) {
        printf("Invalid number of files or corrupted input file\n");
        return NULL;
    }

    app = malloc(sizeof(struct app_data_t));

    // initialize mutex & condition
    app->count_mutex     = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;
    app->condition_mutex = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;
    app->condition_cond  = (pthread_cond_t)PTHREAD_COND_INITIALIZER;
    app->mapper_count    = 0;

    int exponents = number_of_reducers + 2;

    // allocate thread data 
    int number_of_threads = number_of_mappers + number_of_reducers;
    app->threads = malloc(sizeof(struct thread_data_t) * number_of_threads);

    // initialize thread data of mapper and reducer
    for (int i = 0; i < number_of_threads; ++i)
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
    for (int i = 0; i < number_of_files; ++i)
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
    for (int i = 0; i < app->number_of_files; ++i)
    {
        free(app->file_links[i]);
    }
    free(app->file_links);
 
    for (int i = 0; i < app->number_of_threads; ++i)
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
