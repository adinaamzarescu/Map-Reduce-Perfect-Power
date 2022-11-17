#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <libgen.h>
#include <math.h>

#include "tema1.h"

#define __DEBUG__
#define MAX_LEN             256
#define ERROR_SUCCESS       0
#define ONE_IS_PERFECT_POWER    

int get_number_of_files(const char *file_name);
int read_numbers_from_file(char* file_name, int* nums, int size);
int write_numbers_to_file(char* file_name, int num);
int read_files(struct app_data_t* app);
