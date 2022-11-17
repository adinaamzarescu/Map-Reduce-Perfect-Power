# Map-Reduce Perfect Power
#### Copyright Adina-Maria Amzarescu

__________________________________________________________

There are 3 structures in this program.

The first one is a linked list used to store the 
perfect powers for each exponential.

The second one is thread_data_t used for the thread.
    * role is used to check if the thread will be used
        for a map or for a reducer

The last one is app_data_t, used to solve the mapper
and reducer threads. Mutex and condition variable are
used to synchronize the threads. 

__________________________________________________________

`Mapper thread`

All mapper threads find a perfect powers from input data 
and store them to linked list.
These data will be used by reducer threads later.
__________________________________________________________

`Reducer thread`

All reducer threads can process data once all mapper 
threads done their job.
So they wait for condition which the value of mapper_count 
reach to number_of_mappers.
Every mapper thread increase mapper_count by 1 when it 
finished, so mapper_count will be number_of_mappers (for example) 
when all mapper threads are finished.   
Each reducer will process perfect numbers for its own exponential.
    
For example,
    reducer[0] will process perfect numbers for exponential 2 
    reducer[1] for exponential 3, reducer[2] for exponential 3 and so on.
        
They don't need to have a list for them.
__________________________________________________________

Resources:

   1. [OCW](https://ocw.cs.pub.ro/courses/apd/laboratoare/02)

   2. [POSIX thread](https://www.cs.cmu.edu/afs/cs/academic/class/15492-f07/www/pthreads.html)

   3. [MapReduce:Theory and Implementation](https://courses.cs.washington.edu/courses/cse490h/08au/lectures/mapred.pdf)

__________________________________________________________
