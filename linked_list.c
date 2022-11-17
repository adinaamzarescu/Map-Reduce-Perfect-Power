#include "linked_list.h"

void list_print(struct linked_list_t* list)
{
    if (!list) return;

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
    if (!list) return NULL;

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
    if (!list) return;

    struct linked_list_t* item = list->next;
    while (item)
    {
        struct linked_list_t* p = item->next;
        free(item);
        item = p;
    }
}
