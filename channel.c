#include "channel.h"

// Creates a new channel with the provided size and returns it to the caller
// A 0 size indicates an unbuffered channel, whereas a positive size indicates a buffered channel
channel_t* channel_create(size_t size)
{
    /* IMPLEMENT THIS */
    channel_t* channel = (channel_t*) malloc(sizeof(channel_t));
    unsigned int messages = (unsigned int)size;
    channel->buffer = buffer_create(size);

    channel->avail = 1;
    channel->send = 0;
    channel->receive = 0;

    sem_init(&channel->mutex, 0, 1);
    sem_init(&channel->zero, 0, 1);
    sem_init(&channel->empty, 0, messages);
    sem_init(&channel->full, 0, 0);

    channel->select_send = list_create();
    channel->select_receive = list_create();

    return channel;
}

// Writes data to the given channel
// This is a blocking call i.e., the function only returns on a successful completion of send
// In case the channel is full, the function waits till the channel has space to write the new data
// Returns SUCCESS for successfully writing data to the channel,
// CLOSED_ERROR if the channel is closed, and
// GEN_ERROR on encountering any other generic error of any sort
enum channel_status channel_send(channel_t *channel, void* data)
{
    /* IMPLEMENT THIS */
    enum buffer_status buffstat;

    if (channel->avail == 0) {
        return CLOSED_ERROR;
    }

    if ((channel->buffer->capacity) > 0) {
        sem_wait(&channel->empty);
        sem_wait(&channel->mutex);
        // CRITICAL SECTION
        if (channel->avail == 0) {
            sem_post(&channel->mutex);
            sem_post(&channel->empty);
            sem_post(&channel->full);


            return CLOSED_ERROR;
        }
        else {
            buffstat = buffer_add(channel->buffer, data);
            sem_post(&channel->mutex);
            sem_post(&channel->full);


            if (buffstat != BUFFER_SUCCESS) {
                return GEN_ERROR;
            }
        }
        // CRITICAL SECTION

        return SUCCESS;
    }
    else {
        channel->send = channel->send + 1;

        sem_wait(&channel->zero);
        sem_wait(&channel->mutex);
        // CRITICAL SECTION
        if (channel->avail == 0) {
            sem_post(&channel->mutex);
            sem_post(&channel->zero);
            sem_post(&channel->full);

            return CLOSED_ERROR;
        }
        else {
            channel->data = data;
            sem_post(&channel->mutex);
            sem_post(&channel->full);
            //sem_post((sem_t *)channel->data);


            return SUCCESS;
        }
        // CRITICAL SECTION
    }
}

// Reads data from the given channel and stores it in the function’s input parameter, data (Note that it is a double pointer).
// This is a blocking call i.e., the function only returns on a successful completion of receive
// In case the channel is empty, the function waits till the channel has some data to read
// Returns SUCCESS for successful retrieval of data,
// CLOSED_ERROR if the channel is closed, and
// GEN_ERROR on encountering any other generic error of any sort
enum channel_status channel_receive(channel_t* channel, void** data)
{
    /* IMPLEMENT THIS */
    enum buffer_status buffstat;

    if (channel->avail == 0) {
        return CLOSED_ERROR;
    }

    if (channel->buffer->capacity > 0) {
        sem_wait(&channel->full);
        sem_wait(&channel->mutex);
        // CRITICAL SECTION
        if (channel->avail == 0) {
            sem_post(&channel->mutex);
            sem_post(&channel->empty);
            sem_post(&channel->full);


            return CLOSED_ERROR;
        }
        else {
            buffstat = buffer_remove(channel->buffer, data);
            sem_post(&channel->mutex);
            sem_post(&channel->empty);


            if (buffstat != BUFFER_SUCCESS) {
                return GEN_ERROR;
            }
        }
        // CRITICAL SECTION

        return SUCCESS;
    }
    else {
        channel->receive = channel->receive + 1;

        sem_wait(&channel->full);
        sem_wait(&channel->mutex);
        // CRITICAL SECTION
        if (channel->avail == 0) {
            sem_post(&channel->mutex);
            sem_post(&channel->zero);
            sem_post(&channel->full);


            return CLOSED_ERROR;
        }
        else {
            *data = channel->data;
            channel->data = NULL;

            channel->receive = channel->receive - 1;
            channel->send = channel->send - 1;

            sem_post(&channel->mutex);
            sem_post(&channel->zero);
            //sem_post((sem_t *)channel->data);

        }
        // CRITICAL SECTION

        return SUCCESS;
    }
}

// Writes data to the given channel
// This is a non-blocking call i.e., the function simply returns if the channel is full
// Returns SUCCESS for successfully writing data to the channel,
// CHANNEL_FULL if the channel is full and the data was not added to the buffer,
// CLOSED_ERROR if the channel is closed, and
// GEN_ERROR on encountering any other generic error of any sort
enum channel_status channel_non_blocking_send(channel_t* channel, void* data)
{
    /* IMPLEMENT THIS */
    enum buffer_status buffstat;

    if (channel->receive == 0 && channel->buffer->capacity == 0) {
        return CHANNEL_FULL;
    }

    if (channel->buffer->capacity > 0) {

        if ((sem_trywait(&channel->empty) != 0) || (sem_trywait(&channel->mutex))) {
            return CHANNEL_FULL;
        }

        // CRITICAL SECTION
        if (channel->avail == 0) {
            sem_post(&channel->mutex);
            sem_post(&channel->empty);
            sem_post(&channel->full);


            return CLOSED_ERROR;
        }
        else {
            buffstat = buffer_add(channel->buffer, data);
            sem_post(&channel->mutex);
            sem_post(&channel->full);



            if (buffstat != BUFFER_SUCCESS) {
                return GEN_ERROR;
            }
        }
        // CRITICAL SECTION

        return SUCCESS;
    }
    else {
        if ((sem_trywait(&channel->zero) != 0) || (sem_trywait(&channel->mutex) != 0)) {
            return CHANNEL_FULL;
        }

        // CRITICAL SECTION
        if (channel->avail == 0) {
            sem_post(&channel->mutex);
            sem_post(&channel->zero);
            sem_post(&channel->full);


            return CLOSED_ERROR;
        }
        else {
            channel->data = data;
            sem_post(&channel->mutex);
            sem_post(&channel->full);
            //sem_post((sem_t *)channel->data);

        }
        // CRITICAL SECTION

        return SUCCESS;
    }
}

// Reads data from the given channel and stores it in the function’s input parameter data (Note that it is a double pointer)
// This is a non-blocking call i.e., the function simply returns if the channel is empty
// Returns SUCCESS for successful retrieval of data,
// CHANNEL_EMPTY if the channel is empty and nothing was stored in data,
// CLOSED_ERROR if the channel is closed, and
// GEN_ERROR on encountering any other generic error of any sort
enum channel_status channel_non_blocking_receive(channel_t* channel, void** data)
{
    /* IMPLEMENT THIS */
    enum buffer_status buffstat;

    if (channel->send == 0 && channel->buffer->capacity == 0) {
        return CHANNEL_EMPTY;
    }

    if (channel->buffer->capacity > 0) {

        if ((sem_trywait(&channel->full) != 0) || (sem_trywait(&channel->mutex) != 0)) {
            return CHANNEL_EMPTY;
        }

        if (channel->avail == 0) {
            sem_post(&channel->mutex);
            sem_post(&channel->empty);
            sem_post(&channel->full);


            return CLOSED_ERROR;
        }
        else {
        ////////////////////////////////////
            if (data == NULL) {
                sem_post(&channel->mutex);
                sem_post(&channel->empty);
                //sem_post((sem_t *)channel->data);

                return CHANNEL_EMPTY;
            }
        ////////////////////////////////////
            buffstat = buffer_remove(channel->buffer, data);
            sem_post(&channel->mutex);
            sem_post(&channel->empty);


            if (buffstat != BUFFER_SUCCESS) {
                return GEN_ERROR;
            }
        }
        // CRITICAL SECTION

        return SUCCESS;
    }
    else {

        if ((sem_trywait(&channel->full) != 0) || (sem_trywait(&channel->mutex) != 0)) {
              return CHANNEL_EMPTY;
        }

        // CRITICAL SECTION
        if (channel->avail == 0) {
            sem_post(&channel->mutex);
            sem_post(&channel->empty);
            sem_post(&channel->full);


            return CLOSED_ERROR;
        }
        else {

            *data = channel->data;
            channel->data = NULL;
            if (channel->send > 0) {
                channel->send = channel->send - 1;
            }
            sem_post(&channel->mutex);
            sem_post(&channel->zero);
            //sem_post((sem_t *)channel->data);

        }
        // CRITICAL SECTION

        return SUCCESS;
    }
}

// Closes the channel and informs all the blocking send/receive/select calls to return with CLOSED_ERROR
// Once the channel is closed, send/receive/select operations will cease to function and just return CLOSED_ERROR
// Returns SUCCESS if close is successful,
// CLOSED_ERROR if the channel is already closed, and
// GEN_ERROR in any other error case
enum channel_status channel_close(channel_t* channel)
{
    /* IMPLEMENT THIS */
    if (channel->avail == 0) {
        return CLOSED_ERROR;
    }

    if ((channel != NULL) && (channel->avail == 1)) {
        sem_trywait(&channel->mutex);
        // CRITICAL SECTION
        channel->avail = 0;
        // CRITICAL SECTION
        sem_post(&channel->mutex);
        sem_post(&channel->empty);
        sem_post(&channel->full);
        sem_post(&channel->zero);


        sem_close(&channel->mutex);
        sem_close(&channel->empty);
        sem_close(&channel->full);
        sem_close(&channel->zero);
    }
    else {
        return GEN_ERROR;
    }

    return SUCCESS;
}

// Frees all the memory allocated to the channel
// The caller is responsible for calling channel_close and waiting for all threads to finish their tasks before calling channel_destroy
// Returns SUCCESS if destroy is successful,
// DESTROY_ERROR if channel_destroy is called on an open channel, and
// GEN_ERROR in any other error case
enum channel_status channel_destroy(channel_t* channel)
{
    /* IMPLEMENT THIS */
    if (channel->avail == 1) {
        return DESTROY_ERROR;
    }
    else {
        buffer_free(channel->buffer);
        sem_destroy(&channel->empty);
        sem_destroy(&channel->full);
        sem_destroy(&channel->mutex);
        sem_destroy(&channel->zero);

        list_destroy(channel->select_receive);
        list_destroy(channel->select_send);

        free(channel);

        return SUCCESS;
    }
}

void set_select_sem(select_t *channel_list, size_t channel_count, sem_t *select) {

    for (int i = 0; i < channel_count; i++) {

        sem_wait(&channel_list[i].channel->mutex);

        if (channel_list[i].dir == SEND) {
            list_insert(channel_list[i].channel->select_send, select);
        }
        else {
            list_insert(channel_list[i].channel->select_receive, select);
        }
        sem_post(&channel_list[i].channel->mutex);
    }
}

enum channel_status try_select(select_t *channel_list, size_t channel_count, size_t* selected_index) {
    for (int i = 0; i < channel_count; i++) {

        select_t *chanList = &channel_list[i];

        enum channel_status status = GEN_ERROR;

        if (chanList->dir == SEND) {
            status = channel_non_blocking_send(chanList->channel, chanList->data);
        }
        else if (chanList->dir == RECV) {
            status = channel_non_blocking_receive(chanList->channel, chanList->data);
        }

        if (status != CHANNEL_EMPTY && status != CHANNEL_FULL) {
            *selected_index = (size_t) i;
            return status;
        }
    }
    return CHANNEL_FULL;
}

void destroy_select_sem(select_t *channel_list, size_t channel_count, sem_t *select) {
    for (int i = 0; i < channel_count; i++) {
        sem_wait(&channel_list[i].channel->mutex);
        list_remove(channel_list[i].channel->select_receive, list_find(channel_list[i].channel->select_receive, select));
        list_remove(channel_list[i].channel->select_send, list_find(channel_list[i].channel->select_send, select));
        sem_post(&channel_list[i].channel->mutex);
    }
    sem_destroy(select);
}



// Takes an array of channels, channel_list, of type select_t and the array length, channel_count, as inputs
// This API iterates over the provided list and finds the set of possible channels which can be used to invoke the required operation (send or receive) specified in select_t
// If multiple options are available, it selects the first option and performs its corresponding action
// If no channel is available, the call is blocked and waits till it finds a channel which supports its required operation
// Once an operation has been successfully performed, select should set selected_index to the index of the channel that performed the operation and then return SUCCESS
// In the event that a channel is closed or encounters any error, the error should be propagated and returned through select
// Additionally, selected_index is set to the index of the channel that generated the error
enum channel_status channel_select(select_t* channel_list, size_t channel_count, size_t* selected_index)
{
    /* IMPLEMENT THIS */

    sem_t select;
    sem_init(&select, 0, 0);

    set_select_sem(channel_list, channel_count, &select);

    enum channel_status status;

    status = try_select(channel_list, channel_count, selected_index);

    while (status == CHANNEL_FULL) {
        //sem_wait(&select);
        status = try_select(channel_list, channel_count, selected_index);
    }

    destroy_select_sem(channel_list, channel_count, &select);

    return status;
}
