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

pthread_mutex_t kvs_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t backup_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t backup_cond = PTHREAD_COND_INITIALIZER;
int backups_in_progress = 0;

// Inicializando a tabela Hash da KVS
static struct HashTable* kvs_table = NULL;

/// Função que converte o atraso em milissegundos para uma estrutura timespec
static struct timespec delay_to_timespec(unsigned int delay_ms) {
  return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

// Declaração da função hash_function
unsigned int hash_function(const char* key);

// Função que inicializa os mutexes para as chaves
void init_key_mutexes() {
  static pthread_mutex_t key_mutexes[TABLE_SIZE];
  for (int i = 0; i < TABLE_SIZE; i++) {
    pthread_mutex_init(&key_mutexes[i], NULL);
  }
}

// Função que retorna um mutex específico para uma chave
pthread_mutex_t* get_mutex_for_key(const char* key) {
  static pthread_mutex_t key_mutexes[TABLE_SIZE];
  static pthread_once_t init_once = PTHREAD_ONCE_INIT;

  pthread_once(&init_once, init_key_mutexes);
  unsigned int hash = hash_function(key) % TABLE_SIZE;
  return &key_mutexes[hash];
}

// Definição da função hash_function
unsigned int hash_function(const char* key) {
  unsigned int hash = 0;
  while (*key) {
    hash = (hash << 5) + (unsigned int)(unsigned char)*key++;
  }
  return hash;
}

int kvs_init() {
  pthread_mutex_lock(&kvs_mutex);
  if (kvs_table != NULL) {
    pthread_mutex_unlock(&kvs_mutex);
    fprintf(stderr, "KVS state has already been initialized\n");
    return 1;
  }
  kvs_table = create_hash_table();
  pthread_mutex_unlock(&kvs_mutex);
  return kvs_table == NULL;
}

int kvs_terminate() {
  pthread_mutex_lock(&kvs_mutex);
  if (kvs_table == NULL) {
    pthread_mutex_unlock(&kvs_mutex);
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  free_table(kvs_table);
  kvs_table = NULL; // Certifique-se de que a tabela é definida como NULL após a liberação
  pthread_mutex_unlock(&kvs_mutex);
  return 0;
}

int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE]) {
  pthread_mutex_lock(&kvs_mutex);
  if (kvs_table == NULL) {
    pthread_mutex_unlock(&kvs_mutex);
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  pthread_mutex_unlock(&kvs_mutex);

  for (size_t i = 0; i < num_pairs; i++) {
    pthread_mutex_t* key_mutex = get_mutex_for_key(keys[i]);
    pthread_mutex_lock(key_mutex);
    if (write_pair(kvs_table, keys[i], values[i]) != 0) {
      fprintf(stderr, "Failed to write keypair (%s,%s)\n", keys[i], values[i]);
    }
    pthread_mutex_unlock(key_mutex);
  }

  return 0;
}

int compare_keys(const void *a, const void *b) {
    const char (*key_a)[MAX_STRING_SIZE] = a;
    const char (*key_b)[MAX_STRING_SIZE] = b;
    return strcmp(*key_a, *key_b);
}

int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int output_fd) {
  pthread_mutex_lock(&kvs_mutex);
  if (kvs_table == NULL) {
    pthread_mutex_unlock(&kvs_mutex);
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  pthread_mutex_unlock(&kvs_mutex);

  qsort(keys, num_pairs, sizeof(keys[0]), compare_keys);

  dprintf(output_fd, "[");
  for (size_t i = 0; i < num_pairs; i++) {
    pthread_mutex_t* key_mutex = get_mutex_for_key(keys[i]);
    pthread_mutex_lock(key_mutex);
    char* result = read_pair(kvs_table, keys[i]);
    pthread_mutex_unlock(key_mutex);

    if (result == NULL) {
      dprintf(output_fd, "(%s,KVSERROR)", keys[i]);
    } else {
      dprintf(output_fd, "(%s,%s)", keys[i], result);
    }
    free(result);
  }
  dprintf(output_fd, "]\n");

  return 0;
}

int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int output_fd) {
  pthread_mutex_lock(&kvs_mutex);
  if (kvs_table == NULL) {
    pthread_mutex_unlock(&kvs_mutex);
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  pthread_mutex_unlock(&kvs_mutex);
  
  int aux = 0;
  for (size_t i = 0; i < num_pairs; i++) {
    pthread_mutex_t* key_mutex = get_mutex_for_key(keys[i]);
    pthread_mutex_lock(key_mutex);
    int result = delete_pair(kvs_table, keys[i]);
    pthread_mutex_unlock(key_mutex);

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
  pthread_mutex_lock(&kvs_mutex);
  if (kvs_table == NULL) {
    pthread_mutex_unlock(&kvs_mutex);
    return;
  }

  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *keyNode = kvs_table->table[i];
    while (keyNode != NULL) {
      dprintf(output_fd, "(%s, %s)\n", keyNode->key, keyNode->value);
      keyNode = keyNode->next;
    }
  }
  pthread_mutex_unlock(&kvs_mutex);
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
        pthread_cond_wait(&backup_cond, &backup_mutex);
    }
    backups_in_progress++;
    pthread_mutex_unlock(&backup_mutex);

    pid_t pid = fork();
    if (pid == 0) {
        perform_backup(backup_filename);
    } else if (pid > 0) {
        pthread_mutex_lock(&backup_mutex);
        backups_in_progress--;
        pthread_cond_signal(&backup_cond);
        pthread_mutex_unlock(&backup_mutex);
        free(backup_filename);  // Liberar a memória alocada para backup_filename no processo pai
    } else {
        perror("Error creating process for backup");
        free(backup_filename);  // Liberar a memória alocada para backup_filename em caso de erro
        _exit(EXIT_FAILURE);
    }
    return 0;
}

void kvs_wait_backup() {
    pthread_mutex_lock(&backup_mutex);
    while (backups_in_progress > 0) {
        pthread_cond_wait(&backup_cond, &backup_mutex);
    }
    pthread_mutex_unlock(&backup_mutex);
}

void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}