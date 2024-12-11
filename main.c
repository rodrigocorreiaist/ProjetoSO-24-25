// main.c
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <dirent.h>
#include <fcntl.h>
#include <string.h>
#include "constants.h"
#include "parser.h"
#include "operations.h"
#include <pthread.h>



// Defina a variável global aqui
int max_concurrent_backups;
int max_threads;

typedef struct {
    char *directory;
    char *filename;
} job_args_t;

void process_job_file(int input_fd, int output_fd, const char *job_filename, const char *directory) {
    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;
    int backup_counter = 1; // Contador de backups para este arquivo

    while (1) {
        switch (get_next(input_fd)) {
            case CMD_WRITE:
                num_pairs = parse_write(input_fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                if (num_pairs == 0) {
                    fprintf(stderr, "Invalid command. See HELP for usage\n"); // Output para .out
                    continue;
                }
                // Chama a função kvs_write que já lida com erros internos

                if (kvs_write(num_pairs, keys, values)) {
                    fprintf(stderr, "Failed to write pair\n"); // Erro no terminal
                }
                break;


            case CMD_READ:
                num_pairs = parse_read_delete(input_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                if (num_pairs == 0) {
                    fprintf(stderr, "Invalid command. See HELP for usage\n"); // Output para .out
                    continue;
                }
                // Chama a função kvs_read que já lida com erros internos
                if (kvs_read(num_pairs, keys, output_fd)) {
                    fprintf(stderr, "Failed to read pair\n"); // Erro no terminal
                }
                break;

            case CMD_DELETE:
                num_pairs = parse_read_delete(input_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                if (num_pairs == 0) {
                    fprintf(stderr, "Invalid command. See HELP for usage\n"); // Output para .out
                    continue;
                }
                // Chama a função kvs_delete que já lida com erros internos
                if (kvs_delete(num_pairs, keys, output_fd)) {
                    fprintf(stderr, "Failed to delete pair\n"); // Erro no terminal
                }
                break;

            case CMD_SHOW:
                // Redireciona a saída para o arquivo .out
                kvs_show(output_fd);
                break;

            case CMD_WAIT:
                if (parse_wait(input_fd, &delay, NULL) == -1) {
                    fprintf(stderr, "Invalid command. See HELP for usage\n"); // Erro no terminal
                    continue;
                }
                kvs_wait(delay);
                break;

            case CMD_BACKUP:
                kvs_backup(job_filename, backup_counter, directory);  // Passa o contador e diretório
                backup_counter++; // Incrementa o contador para o próximo backup
                break;

            case CMD_HELP:
                printf(
                        "Available commands:\n"
                        "  WRITE [(key,value)(key2,value2),...]\n"
                        "  READ [key,key2,...]\n"
                        "  DELETE [key,key2,...]\n"
                        "  SHOW\n"
                        "  WAIT <delay_ms>\n"
                        "  BACKUP\n" // Não implementado
                        "  HELP\n"); // Output para .out
                break;

            case CMD_INVALID:
                dprintf(output_fd, "Invalid command. See HELP for usage\n"); // Erro no terminal
                break;

            case CMD_EMPTY:
                break;

            case EOC:
                
                return;
        }
    }
}

void *process_job_thread(void *arg) {
    job_args_t *job_args = (job_args_t *)arg;
    char *directory = job_args->directory;
    char *filename = job_args->filename;

    size_t input_path_len = strlen(directory) + strlen(filename) + 2;
    char *input_path = malloc(input_path_len);
    if (!input_path) {
        perror("Error allocating memory for input_path");
        free(job_args->directory);
        free(job_args->filename);
        free(job_args);
        return NULL;
    }

    snprintf(input_path, input_path_len, "%s/%s", directory, filename);

    size_t output_path_len = input_path_len + 1;
    char *output_path = malloc(output_path_len);
    if (!output_path) {
        perror("Error allocating memory for output_path");
        free(input_path);
        free(job_args->directory);
        free(job_args->filename);
        free(job_args);
        return NULL;
    }

    strncpy(output_path, input_path, output_path_len);
    char *ext = strrchr(output_path, '.');
    if (ext) strcpy(ext, ".out");

    int input_fd = open(input_path, O_RDONLY);
    if (input_fd < 0) {
        perror("Error opening input file");
        free(input_path);
        free(output_path);
        free(job_args->directory);
        free(job_args->filename);
        free(job_args);
        return NULL;
    }

    int output_fd = open(output_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (output_fd < 0) {
        perror("Error creating output file");
        close(input_fd);
        free(input_path);
        free(output_path);
        free(job_args->directory);
        free(job_args->filename);
        free(job_args);
        return NULL;
    }

    process_job_file(input_fd, output_fd, filename, directory);

    close(input_fd);
    close(output_fd);

    free(input_path);
    free(output_path);
    free(job_args->directory);
    free(job_args->filename);
    free(job_args);

    return NULL;
}

// Função que percorre a diretoria e processa os arquivos .job
void process_job_directory(const char *directory) {
    DIR *dir = opendir(directory);
    if (!dir) {
        perror("Error opening directory");
        return;
    }

    struct dirent *entry;
    pthread_t *threads = malloc((size_t)(max_threads) * sizeof(pthread_t));
    int thread_count = 0;

    while ((entry = readdir(dir)) != NULL) {
        if (strstr(entry->d_name, ".job")) {
            job_args_t *job_args = malloc(sizeof(job_args_t));
            if (!job_args) {
                perror("Error allocating memory for job_args");
                continue;
            }
            job_args->directory = strdup(directory);
            job_args->filename = strdup(entry->d_name);

            if (!job_args->directory || !job_args->filename) {
                perror("Error duplicating strings");
                free(job_args->directory);
                free(job_args->filename);
                free(job_args);
                continue;
            }

            if (pthread_create(&threads[thread_count], NULL, process_job_thread, job_args) != 0) {
                perror("Error creating thread");
                free(job_args->directory);
                free(job_args->filename);
                free(job_args);
                continue;
            }

            printf("Thread %d created to process file: %s\n", thread_count, entry->d_name);

            thread_count++;
            if (thread_count >= max_threads) {
                for (int i = 0; i < thread_count; i++) {
                    pthread_join(threads[i], NULL);
                }
                thread_count = 0;
            }
        }
    }

    for (int i = 0; i < thread_count; i++) {
        pthread_join(threads[i], NULL);
    }

    free(threads);
    closedir(dir);
}


int main(int argc, char *argv[]) {
    if (argc == 4) { // Verificar se o número correto de argumentos foi passado
        const char *directory = argv[1];
        max_concurrent_backups = atoi(argv[2]);
        max_threads = atoi(argv[3]);

        if (max_concurrent_backups <= 0 || max_threads <= 0) {
            fprintf(stderr, "Invalid value for max_backups or max_threads. Must be greater than 0.\n");
            return 1;
            
        }

        if (kvs_init()) {
            fprintf(stderr, "Failed to initialize KVS\n");
            return 1;
        }

        process_job_directory(directory);
        kvs_wait_backup();
        kvs_terminate();

        return 0;
    } else {
        fprintf(stderr, "Usage: %s <directory> <max_concurrent_backups> <max_threads>\n", argv[0]);
        return 1;
    }
}