#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct linked_list_t
{
    struct linked_list_t *next;
    int data;
};

void list_print(struct linked_list_t* list);
struct linked_list_t* list_append(struct linked_list_t* list, int data);
void list_free(struct linked_list_t* list);
