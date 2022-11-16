#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#define __DEBUG__
#define MAX_LEN             100
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

struct thread_data_t
{
    int         index;              // thread index
    int         role;               // 0: mapper, 1: reducer
    int         file_count;         // count of file to be processed
    char        **file_names;       // array of file name
    int         total_numbers;      // total number of input numbers
    int         exponents;          // max exponents
    struct linked_list_t *perfect_powers;       // array of input numbers
    pthread_t   p_thread;           // pthread
    p_app_data_t app;
};

struct app_data_t
{
    int         number_of_mappers;
    int         number_of_reducers;
    int         number_of_threads;
    const char  *file_name;
    int         number_of_files;
    char        **file_links;
    int         mapper_count;
    struct thread_data_t* threads;
    pthread_mutex_t count_mutex;
    pthread_mutex_t condition_mutex;
    pthread_cond_t  condition_cond;    
};

// function prototype
int get_number_of_files(const char *file_name);
int mapper_func(struct thread_data_t* td);
int reducer_func(struct thread_data_t* td);
int check_perfect_power(int num);
void* thread_func(void * input);
// int map_process(char **map_array, int number_of_mappers, int final_count);
int allocate_maps_and_reducers(struct app_data_t *app);
int allocate_files_to_maps(struct app_data_t *app);
int read_files(struct app_data_t* app);
void thread_init(struct thread_data_t* td, int index, int role);
void map_free(struct thread_data_t* td);
struct app_data_t *app_alloc(int number_of_mappers, int number_of_reducers, const char *file_name);
void app_free(struct app_data_t *app);

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
    
    if(test_file == NULL)
    {
        printf("File not found ~%s...!\n",file_name);
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
    int count = 0, start = 0, numbers = 0;
    
    while (fgets(line, 100, input_file) != NULL && count <= size)
    {
        //Skipping the first line
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
*/
int mapper_func(struct thread_data_t* td)
{
#ifdef __DEBUG__
    printf("mapper %d:\n", td->index);
#endif

    // get total numbers
    int total_numbers = 0;
    int count = 0;
    for (int i = 0; i < td->file_count; i++)
    {
        count = get_number_of_files(td->file_names[i]);
        total_numbers += count;
    }

    int *nums = malloc(sizeof(int) * total_numbers);
    int final_count = 0;

    for (int i = 0; i < td->file_count; i++)
    {
        final_count += read_numbers_from_file(td->file_names[i], &nums[final_count], total_numbers - final_count);
    }

    //Testing array
    for (int j = 0; j < final_count; j++)
    {
        if (nums[j] == -1)
        {
            continue;
        }
        int power_check = check_perfect_power(nums[j]);
        if (!power_check)
        {
            //Setting all same nos to -1
            for (int k = 1; k < count; k++)
            {
                if (k == j)
                {
                    continue;
                }
                if (nums[k] == nums[j])
                {
                    nums[k] = -1; //setting the number as -1 in case its not a perfect power
                }
            }
            nums[j]  = -1;
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
    printf("mapper %d:\n", td->index);
    for (int i = 2; i < td->exponents; i++)
    {
        printf("#exp %d ", i);
        list_print(&(td->perfect_powers[i]));
        printf("\n");
    }
#endif

    free(nums);
}

int reducer_func(struct thread_data_t* td)
{
    // Calculating count of unique nos in array
    int final_count = 0;

    for (int i = 0; i < td->app->number_of_mappers; i++)
    {
        int exp = td->index + 2;
        final_count += td->perfect_powers[i].next[exp].data;
    }

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
    printf("reducer %d: [%d] (", td->index, count);
    for (int i = 0; i < count; i++)
    {
        printf("%d,", nums[i]);
    }
    printf(")\n");
#endif

    // mark duplicated numbers as -1
    count = 0;
    for(int i = 0; i < final_count; i++)
    {
        if(nums[i] == -1)
        {
            continue;;
        }
        else
        {
            count++;
        }
        int num = nums[i];
        //Replacing all other values than num as -1
        for(int j = 0; j < final_count; j++)
        {
            if (nums[j] == -1 || i == j)
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
    printf("reducer %d: count %d\n", td->index, count);
#endif

    char file_name[MAX_LEN];
    
    sprintf(file_name, "out%d.txt", td->index + 2);
    write_numbers_to_file(file_name, count);

    free(nums);
}

/*
    input: map_data_t
*/
void* thread_func(void * input)
{
    FILE *input_file;
    int ret = 0;
    struct thread_data_t* td = (struct thread_data_t*)input;
   
    if (td->role == THREAD_ROLE_MAPPER)
    {
        ret = mapper_func(td);

        // increase count of finished mapper
        pthread_mutex_lock( &(td->app->count_mutex) );
        {
            td->app->mapper_count++; 
            printf("mapper %d: mapper_counter %d\n", td->index, td->app->mapper_count);
        }
        pthread_mutex_unlock( &(td->app->count_mutex) );        

        pthread_mutex_lock( &(td->app->condition_mutex) );
        if (td->app->mapper_count == td->app->number_of_mappers)
        {
            pthread_cond_broadcast( &(td->app->condition_cond) );
        }
        pthread_mutex_unlock( &(td->app->condition_mutex) );
    }
    else if (td->role == THREAD_ROLE_REDUCER)
    {
        // wait until all mapper threads exit
        printf("reducer %d: waiting... mapper_counter %d\n", td->index, td->app->mapper_count);

        pthread_mutex_lock( &(td->app->condition_mutex) );
        while( td->app->mapper_count < td->app->number_of_mappers )
        {
            pthread_cond_wait( &(td->app->condition_cond), &(td->app->condition_mutex) );
        }
        pthread_mutex_unlock( &(td->app->condition_mutex) );

        pthread_mutex_lock( &(td->app->count_mutex) );
        {
            printf("reducer %d: current mapper_counter %d\n", td->index, td->app->mapper_count);
        }
        pthread_mutex_unlock( &(td->app->count_mutex) );        

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
        // app->threads[i].list_mutex = PTHREAD_MUTEX_INITIALIZER;
        pthread_create(&app->threads[i].p_thread, NULL, thread_func, (void *)(&app->threads[i]));
#ifdef __DEBUG__
    printf("thread %d started\n", i);
#endif
    }

    for (int i = 0; i < app->number_of_threads; i++)
    {
        pthread_join(app->threads[i].p_thread, NULL);
#ifdef __DEBUG__
        printf("thread %d terminated\n", i);
#endif
    }    

#ifdef __DEBUG__
    printf("ending...\n");
#endif

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

#ifdef __DEBUG__
    for (int i = 0; i < app->number_of_mappers; i++)
    {
        for (int j = 0; j < app->threads[i].file_count; j++)
        {
            printf("map[%d] file[%d]: %s\n", i, j, app->threads[i].file_names[j]);
        }
    }
#endif

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
       // printf("line is %s",line);
        if (start == 0)
        {
            start++;
        }
        else
        {
            if (count == app->number_of_files - 1)
            {
                line[strlen(line)] = '\0';
            }
            else{
                line[strlen(line)-1] = '\0';
            }
            
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
#ifdef __DEBUG__
    printf("thread_free: %d\n", td->index);
#endif

    list_free(td->perfect_powers);
    if (td->perfect_powers)
    {
        printf("%d %p\n", td->index, td->perfect_powers);
        if (td->role == THREAD_ROLE_MAPPER)
        {
            free(td->perfect_powers);
        } 
    }
}

/*
*/
struct app_data_t *app_alloc(int number_of_mappers, int number_of_reducers, const char *file_name)
{
    struct app_data_t* app = NULL;

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

    // initialize thread data of mapper
    for (int i = 0; i < number_of_threads; i++)
    {
        struct thread_data_t *td = &(app->threads[i]); 

        if (i < number_of_mappers)
        {
            // initialize mapper thread
            thread_init(td, i, THREAD_ROLE_MAPPER);

            // set max exponents and initialize list of perfect numbers 
            td->perfect_powers = malloc(sizeof(struct linked_list_t) * exponents);
            for (int j = 0; j < exponents; j++)
            {
                td->perfect_powers[j].next = NULL;
                td->perfect_powers[j].data = 0;    // count of list
            }
        }
        else
        {
            thread_init(td, i - number_of_mappers, THREAD_ROLE_REDUCER);

            // set max exponents and initialize list of perfect numbers 
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
    // free(app->threads);

    free(app);
}

void test_linked_list()
{
    struct linked_list_t *list = (struct linked_list_t *)malloc(sizeof(struct linked_list_t));
    
    list->next = NULL;
    list->data = 0;

    list_append(list, 3);
    list_append(list, 9);
    list_append(list, 111);

    list_print(list);

    list_free(list);
   
    free(list);
}

int main(int argc, const char * argv[])
{
    if (argc < 4)
    {
        printf("arguments: NUMBER_OF_MAPPERS NUMBER_OF_REDUCERS FILE_NAME\n");
        return -1;
    }

    int number_of_mappers = atoi(argv[1]);
    int number_of_reducers = atoi(argv[2]);
    const char *file_name = argv[3];

    struct app_data_t* app = app_alloc(number_of_mappers, number_of_reducers, file_name);

    // read file and all the arguments
    int ret = read_files(app);

    ret = allocate_files_to_maps(app);
    
    //Allocating Maps & reducers & Sending for further actions
    ret = allocate_maps_and_reducers(app);

    // free app
    app_free(app);

    return 0;
}