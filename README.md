# Map-Reduce Perfect Power
#### Copyright Adina-Maria Amzarescu

__________________________________________________________

Map-Reduce program to implement a parallel program in 
Pthreads for finding numbers greater than 0 that are
perfect powers from a set of files and counting unique 
values for each exponent.

__________________________________________________________

There are 3 structures in this program.

The first one is a linked list used to store the 
perfect powers for each exponent.

The second one is thread_data_t used for the thread.

   * role is used to check if the thread will be used
     for a map or for a reducer

The last one is app_data_t, used to solve the mapper
and reducer threads. Mutex and condition variable are
used to synchronize the threads. 

__________________________________________________________

### Flow and logic:

The the main() function will call the read_files() function.
There each line will be stored in the file_links array, used
to find each file to be processed.

Then each file will be sent to maps based on the number of files.
The app_aloc() function decides if the thread is a mapper or
a reducer. Here the mutex is initialized. If the index is 
less than the number of mappers then it is a mapper thread.
Otherwise it is a reducer thread. For each one of those threads,
the role will be either 0 (mapper) or 1 (reducer). The exponents
variable is equal to the number_of_reducers + 2 because the
exponents will start from 2.

Then the threads will start in the allocate_maps_and_reducers() 
function. I used a for() to create the threads and then another
for() to join them. By deciding their role all threads start
at the same time.

The thread_func() will check the role of the thread (0-mapper 
or 1-reducer). If it is a mapper thread then the mapper_func()
is called and count of finised mapper is increased.
When the value of mapper_count reaches the number_of_mappers
the reducer threads will be unlocked.
If it is a reducer function then it will wait until mappers
have finished and then call the reducer_func().

The files are sent to maps in the allocate_files_to_maps() 
function. Here depending on the number of files and the 
number of mappers, in order to distribute them evenly the program
will check if the distribution can be made 1:1. If there are
more files than mappers then the files_per_map will be 
number_of_files/number_of_mappers.

Then each file will be sent to a mapper depending on the
number_of_files and files_per_map. Each mapper will get
a file until file_count reaches the last file_per_map.
Then if there are remaining files those will be distributed
to mappers as well and the number of files_per_map will increase.

To calculate the time I used time_t to check the difference between
the start the end of the program.

__________________________________________________________

### `Mapper thread`

*General Idea:*

All mapper threads find the perfect powers from input data 
and store them to linked list.
This data will be used by reducer threads later.

*Code explanation:*

The mapper_func() function will get the number of files.
Since a single mapper might handle multiple files the
total_numbers variable will be the final number.

Then the final_count variable will store how many
numbers there are in each file.

For testing the perfect powers are stored in the linked list
because it is more efficient. Since 1 is a perfect power
there is a separate list_append() call for this case. Then
each perfect power is added to the list of the associated 
exponent.

__________________________________________________________

### `Reducer thread`

*General Idea:*

All reducer threads can process data once all mapper 
threads have done their job.
So they wait for condition which the value of mapper_count 
reach to number_of_mappers.
Every mapper thread increases mapper_count by 1 when it has
finished, so mapper_count will be number_of_mappers (for example) 
when all mapper threads are finished.
Each reducer will process perfect numbers for its own exponential.

For example,
    reducer[0] will process perfect numbers for exponential 2 
    reducer[1] for exponential 3, reducer[2] for exponential 3 and so on.
        
They don't need to have a list for them.

*Code explanation:*

The reducer_func() function will get the total count of perfect
powers for a given exponent. The nums array is used to store
the data from the linked_list. At first the array will store all
perfect powers, with duplicates. Then all duplicates will be removed
by replacing them with -1.

For counting the final number (unique values for each exponent)
the array will be processed again and for each value different
than -1 count will increase. Then count will be printed in
a representative output file for each exponent.

__________________________________________________________

Resources:

   1. [OCW](https://ocw.cs.pub.ro/courses/apd/laboratoare/02)

   2. [POSIX thread](https://www.cs.cmu.edu/afs/cs/academic/class/15492-f07/www/pthreads.html)

   3. [MapReduce:Theory and Implementation](https://courses.cs.washington.edu/courses/cse490h/08au/lectures/mapred.pdf)

__________________________________________________________
