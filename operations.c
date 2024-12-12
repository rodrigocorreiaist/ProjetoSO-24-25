#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>
#include "kvs.h"
#include "constants.h"
#include "parser.h"

// pthread_mutex_t kvs_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t backup_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t backup_cond = PTHREAD_COND_INITIALIZER;
int backups_in_progress = 0;

// Inicializando a tabela Hash da KVS
static struct HashTable* kvs_table = NULL;

// Definição dos read-write locks
pthread_rwlock_t kvs_rwlocks[TABLE_SIZE];  // Array de read-write locks

int hash(const char *key);

// Função que inicializa os read-write locks
void init_rwlocks() {
    for (int i = 0; i < TABLE_SIZE; i++) {
        pthread_rwlock_init(&kvs_rwlocks[i], NULL);
    }
}

// Função que converte o atraso em milissegundos para uma estrutura timespec
static struct timespec delay_to_timespec(unsigned int delay_ms) {
  return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}


int kvs_init() {
  if (kvs_table != NULL) {
    fprintf(stderr, "KVS state has already been initialized\n");
    return 1;
  }
  kvs_table = create_hash_table();
  return kvs_table == NULL;
}

int kvs_terminate() {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  free_table(kvs_table);
  kvs_table = NULL; // Certifique-se de que a tabela é definida como NULL após a libertação
  return 0;
}

int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE]) {
    for (size_t i = 0; i < num_pairs; i++) {
        // Calcula o índice com base na chave
        int index = hash(keys[i]);

        // Obtem o rwlock correspondente à chave
        pthread_rwlock_t *rwlock = &kvs_rwlocks[index];
        pthread_rwlock_wrlock(rwlock);  // Lock para escrita

        if (write_pair(kvs_table, keys[i], values[i]) != 0) {
            fprintf(stderr, "Failed to write keypair (%s, %s)\n", keys[i], values[i]);
        }
        
        pthread_rwlock_unlock(rwlock);  // Desbloqueia o rwlock após a escrita
    }
    return 0;
}

int compare_keys(const void *a, const void *b) {
    const char (*key_a)[MAX_STRING_SIZE] = a;
    const char (*key_b)[MAX_STRING_SIZE] = b;
    return strcmp(*key_a, *key_b);
}

int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int output_fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  qsort(keys, num_pairs, sizeof(keys[0]), compare_keys);

  dprintf(output_fd, "[");
    for (size_t i = 0; i < num_pairs; i++) {
        // Calcula o índice com base na chave
        int index = hash(keys[i]);

        // Obtem o rwlock correspondente à chave
        pthread_rwlock_t *rwlock = &kvs_rwlocks[index];
        pthread_rwlock_rdlock(rwlock);  // Lock para leitura

        char* result = read_pair(kvs_table, keys[i]);

        pthread_rwlock_unlock(rwlock);  // Desbloqueia o rwlock após a leitura

        if (result == NULL) {
            dprintf(output_fd, "(%s,KVSERROR)", keys[i]);
        } else {
            dprintf(output_fd, "(%s,%s)", keys[i], result);
            free(result); // Liberar a memória do valor lido
        }
    }
    dprintf(output_fd, "]\n");

    return 0;
}

int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int output_fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  int aux = 0;
    for (size_t i = 0; i < num_pairs; i++) {
        // Calcula o índice com base na chave
        int index = hash(keys[i]);

        // Obtem o rwlock correspondente à chave
        pthread_rwlock_t *rwlock = &kvs_rwlocks[index];
        pthread_rwlock_wrlock(rwlock);  // Lock para escrita

        int result = delete_pair(kvs_table, keys[i]);

        pthread_rwlock_unlock(rwlock);  // Desbloqueia o rwlock após a exclusão

        if (result != 0) {
            if (!aux) {
                dprintf(output_fd, "[");
                aux = 1;
            }
            dprintf(output_fd, "(%s,KVSMISSING)", keys[i]);
        }
    }
    if (aux) {
        dprintf(output_fd, "]\n");
    }
    return 0;
}

void kvs_show(int output_fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return;
  }

  for (int i = 0; i < TABLE_SIZE; i++) {
        // Obtém o rwlock correspondente à posição na tabela
        pthread_rwlock_rdlock(&kvs_rwlocks[i]);  // Lock para leitura

        KeyNode *keyNode = kvs_table->table[i];
        while (keyNode != NULL) {
            dprintf(output_fd, "(%s, %s)\n", keyNode->key, keyNode->value);
            keyNode = keyNode->next; // Move para o próximo nó
        }

        pthread_rwlock_unlock(&kvs_rwlocks[i]);  // Desbloqueia o rwlock após a leitura
    }
}

void perform_backup(const char *backup_filename) {
    int backup_fd = open(backup_filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (backup_fd < 0) {
        perror("Error creating backup file");
        _exit(EXIT_FAILURE);
    }
    kvs_show(backup_fd);
    close(backup_fd);
    free((void *)backup_filename);  // Liberar a memória alocada para backup_filename
    _exit(EXIT_SUCCESS);
}

int kvs_backup(const char *job_filename, int backup_counter, const char *directory) {
    char *backup_filename = malloc(MAX_JOB_FILE_NAME_SIZE);
    if (!backup_filename) {
        perror("Error allocating memory for backup_filename");
        return 1;
    }

    char *dot = strrchr(job_filename, '.');
    if (dot) *dot = '\0';

    snprintf(backup_filename, MAX_JOB_FILE_NAME_SIZE, "%s/%s-%d.bck", directory, job_filename, backup_counter);

    pthread_mutex_lock(&backup_mutex);
    while (backups_in_progress >= max_concurrent_backups) {
        pthread_mutex_unlock(&backup_mutex);
        wait(NULL);  // Espera por qualquer processo filho terminar
        pthread_mutex_lock(&backup_mutex);
    }
    backups_in_progress++;
    pthread_mutex_unlock(&backup_mutex);

    pid_t pid = fork();
    if (pid == 0) {
        // Processo filho
        perform_backup(backup_filename);
        _exit(EXIT_SUCCESS);
    } else if (pid > 0) {
        // Processo pai
        int status;
        waitpid(pid, &status, 0);  // Espera o processo filho terminar

        pthread_mutex_lock(&backup_mutex);
        backups_in_progress--;
        pthread_mutex_unlock(&backup_mutex);

        free(backup_filename);  // Liberar a memória alocada para backup_filename no processo pai
    } else {
        // Erro ao criar o processo
        perror("Error creating process for backup");
        free(backup_filename);  // Liberar a memória alocada para backup_filename em caso de erro
        return 1;
    }
    return 0;
}

void kvs_wait_backup() {
    pthread_mutex_lock(&backup_mutex);
    while (backups_in_progress > 0) {
        pthread_mutex_unlock(&backup_mutex);
        wait(NULL);  // Espera por qualquer processo filho terminar
        pthread_mutex_lock(&backup_mutex);
    }
    pthread_mutex_unlock(&backup_mutex);
}

void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}