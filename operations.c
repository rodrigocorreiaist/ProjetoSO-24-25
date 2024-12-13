#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>
#include <sys/wait.h> 
#include "kvs.h"
#include "constants.h"
#include "parser.h"

// Mutex para controlar backups
pthread_mutex_t backup_mutex = PTHREAD_MUTEX_INITIALIZER;
int backups_in_progress = 0;

// Inicializando a tabela Hash da KVS
static struct HashTable* kvs_table = NULL;

// Definição dos read-write locks
pthread_rwlock_t kvs_rwlocks[TABLE_SIZE];  // Array de read-write locks

// Função hash
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

// Inicializa a KVS
int kvs_init() {
  if (kvs_table != NULL) {
    fprintf(stderr, "KVS state has already been initialized\n");
    return 1;
  }
  kvs_table = create_hash_table();
  return kvs_table == NULL;
}

// Termina a KVS
int kvs_terminate() {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  // Destruir read-write locks
  for (int i = 0; i < TABLE_SIZE; i++) {
      pthread_rwlock_destroy(&kvs_rwlocks[i]);
  }

  free_table(kvs_table);
  kvs_table = NULL; // Certifique-se de que a tabela é definida como NULL após a libertação
  return 0;
}

// Compara duas chaves
int compare_keys(const void *a, const void *b) {
    const char (*key_a)[MAX_STRING_SIZE] = a;
    const char (*key_b)[MAX_STRING_SIZE] = b;
    return strcmp(*key_a, *key_b);
}

// Estrutura para armazenar pares chave-valor
typedef struct {
    char key[MAX_STRING_SIZE];
    char value[MAX_STRING_SIZE];
} KeyValuePair;

// Compara duas chaves para escrita
int compare_keys_write(const void *a, const void *b) {
    const KeyValuePair *pair_a = a;
    const KeyValuePair *pair_b = b;
    return strcmp(pair_a->key, pair_b->key);
}

// Escreve pares chave-valor na KVS
int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE]) {
    if (kvs_table == NULL) {
        fprintf(stderr, "KVS state must be initialized\n");
        return 1;
    }

    // Cria um array de KeyValuePair para armazenar as chaves e valores juntos
    KeyValuePair pairs[num_pairs];
    for (size_t i = 0; i < num_pairs; i++) {
        strncpy(pairs[i].key, keys[i], MAX_STRING_SIZE);
        strncpy(pairs[i].value, values[i], MAX_STRING_SIZE);
    }

    // Ordena os pares pela ordem das chaves
    qsort(pairs, num_pairs, sizeof(KeyValuePair), compare_keys_write);

    // Primeiro, bloqueia todas as entradas necessárias
    for (size_t i = 0; i < num_pairs; i++) {
        int index = hash(pairs[i].key);
        pthread_rwlock_wrlock(&kvs_rwlocks[index]);  // Lock para escrita
    }

    // Em seguida, realiza as operações de escrita
    for (size_t i = 0; i < num_pairs; i++) {
        if (write_pair(kvs_table, pairs[i].key, pairs[i].value) != 0) {
            fprintf(stderr, "Failed to write keypair (%s, %s)\n", pairs[i].key, pairs[i].value);
        }
    }

    // Finalmente, desbloqueia todas as entradas usadas
    for (size_t i = 0; i < num_pairs; i++) {
        int index = hash(pairs[i].key);
        pthread_rwlock_unlock(&kvs_rwlocks[index]);  // Desbloqueia o rwlock
    }

    return 0;
}

// Lê pares chave-valor da KVS
int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int output_fd) {
    if (kvs_table == NULL) {
        fprintf(stderr, "KVS state must be initialized\n");
        return 1;
    }

    qsort(keys, num_pairs, sizeof(keys[0]), compare_keys);

    // Primeiro, bloqueia todas as entradas necessárias
    for (size_t i = 0; i < num_pairs; i++) {
        int index = hash(keys[i]);
        pthread_rwlock_rdlock(&kvs_rwlocks[index]);  // Lock para leitura
    }

    dprintf(output_fd, "[");
    // Em seguida, realiza as operações de leitura
    for (size_t i = 0; i < num_pairs; i++) {
        char* result = read_pair(kvs_table, keys[i]);

        if (result == NULL) {
            dprintf(output_fd, "(%s,KVSERROR)", keys[i]);
        } else {
            dprintf(output_fd, "(%s,%s)", keys[i], result);
            free(result); // Libertar a memória do valor lido
        }
    }
    dprintf(output_fd, "]\n");

    // Finalmente, desbloqueia todas as entradas usadas
    for (size_t i = 0; i < num_pairs; i++) {
        int index = hash(keys[i]);
        pthread_rwlock_unlock(&kvs_rwlocks[index]);  // Desbloqueia o rwlock
    }

    return 0;
}

// Exclui pares chave-valor da KVS
int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int output_fd) {
    if (kvs_table == NULL) {
        fprintf(stderr, "KVS state must be initialized\n");
        return 1;
    }

    int aux = 0;

    // Ordena as chaves para garantir a ordem consistente das operações
    qsort(keys, num_pairs, sizeof(keys[0]), compare_keys);

    // Primeiro, bloqueia todas as entradas necessárias
    for (size_t i = 0; i < num_pairs; i++) {
        int index = hash(keys[i]);
        pthread_rwlock_wrlock(&kvs_rwlocks[index]);  // Lock para escrita
    }

    // Em seguida, realiza as operações de exclusão
    for (size_t i = 0; i < num_pairs; i++) {
        int result = delete_pair(kvs_table, keys[i]);

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

    // Finalmente, desbloqueia todas as entradas usadas
    for (size_t i = 0; i < num_pairs; i++) {
        int index = hash(keys[i]);
        pthread_rwlock_unlock(&kvs_rwlocks[index]);  // Desbloqueia o rwlock
    }

    return 0;
}

// Mostra todos os pares chave-valor da KVS
void kvs_show(int output_fd) {
    if (kvs_table == NULL) {
        fprintf(stderr, "KVS state must be initialized\n");
        return;
    }

    // Bloqueia todos os rwlocks para garantir a atomicidade
    for (int i = 0; i < TABLE_SIZE; i++) {
        pthread_rwlock_wrlock(&kvs_rwlocks[i]);  // Lock para escrita
    }

    // Realiza a leitura e exibição das entradas
    for (int i = 0; i < TABLE_SIZE; i++) {
        KeyNode *keyNode = kvs_table->table[i];
        while (keyNode != NULL) {
            dprintf(output_fd, "(%s, %s)\n", keyNode->key, keyNode->value);
            keyNode = keyNode->next; // Move para o próximo nó
        }
    }

    // Desbloqueia todos os rwlocks após a leitura
    for (int i = 0; i < TABLE_SIZE; i++) {
        pthread_rwlock_unlock(&kvs_rwlocks[i]);  // Desbloqueia o rwlock
    }
}

// Realiza o backup da KVS
void perform_backup(const char *backup_filename) {
    int backup_fd = open(backup_filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (backup_fd < 0) {
        perror("Error creating backup file");
        _exit(EXIT_FAILURE);
    }
    kvs_show(backup_fd);
    close(backup_fd);
    free((void *)backup_filename);  // Libertar a memória alocada para backup_filename
    _exit(EXIT_SUCCESS);
}

// Inicia o processo de backup
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
        backups_in_progress--;  // Decrementa backups_in_progress após o término do processo filho
        printf("Backup completed. Backups in progress: %d\n", backups_in_progress);
    }
    backups_in_progress++;  // Incrementa backups_in_progress ao iniciar um novo backup
    printf("Starting new backup. Backups in progress: %d\n", backups_in_progress);
    pthread_mutex_unlock(&backup_mutex);

    pid_t pid = fork();
    if (pid == 0) {
        // Processo filho
        perform_backup(backup_filename);
        _exit(EXIT_SUCCESS);
    } else if (pid > 0) {
        // Processo pai
        free(backup_filename);  // Libertar a memória alocada para backup_filename no processo pai
    } else {
        // Erro ao criar o processo
        perror("Error creating process for backup");
        free(backup_filename);  // Libertar a memória alocada para backup_filename em caso de erro
        return 1;
    }

    return 0;
}

// Espera até que todos os backups sejam concluídos
void kvs_wait_backup() {
    pthread_mutex_lock(&backup_mutex);
    while (backups_in_progress > 0) {
        pthread_mutex_unlock(&backup_mutex);
        wait(NULL);  // Espera por qualquer processo filho terminar
        pthread_mutex_lock(&backup_mutex);
        backups_in_progress--;  // Decrementa backups_in_progress após o término do processo filho
        printf("Limpeza final: Backup completed. Backups in progress: %d\n", backups_in_progress);
    }
    pthread_mutex_unlock(&backup_mutex);
    pthread_mutex_destroy(&backup_mutex);
}

// Espera por um determinado tempo em milissegundos
void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}