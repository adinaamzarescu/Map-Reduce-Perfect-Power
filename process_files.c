#include "process_files.h"

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
    
    if (!test_file)
    {
#ifdef __DEBUG__
        int errnum = errno;
        printf("get_number_of_files(): File not found %s, error %d, %s!\n", file_name, errnum, strerror( errnum ));
        perror("Error printed by perror");
#endif
        return -1;
    }
    
    int count = 0;
    
    // Reading test file for only 1st line and getting no of file links and saving file links in array
    while((fgets(first_line, 100, test_file)) && count < 1)
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

int read_numbers_from_file(char* file_name, int* nums, int size)
{
    FILE *input_file = NULL;
    char line[100];
    
    input_file = fopen(file_name, "r");
    if (!input_file)
    {
        return -1;
    }

    // Reading from line by line from file and storing links in an int array
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
    if (!output_file)
    {
        return -1;
    }

    fprintf(output_file, "%d", num);
    fclose(output_file);

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
    if (!test_file)
    {
        printf("Input file not found...terminating!\n");
        return -1;
    }
    
    char line[100] = "";
    int count = 0, start = 0;
    
    // Reading line by line from file and storing links in an file_links array
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
