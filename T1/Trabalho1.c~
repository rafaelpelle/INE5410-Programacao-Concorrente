#include <unistd.h>
#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <pthread.h>
#include <semaphore.h>


// Estrutura tipo Bucket
typedef struct {
   int id;				// Identificador do bucket.
   int limiteSuperior;	// Limite da faixa de valores que esse bucket receberá.
   int limiteInferior;	// Limite da faixa de valores que esse bucket receberá.
   int tamanho;			// Quantidade de elementos dentro do bucket.
   int *dados; 			// Vetor de elementos do bucket.
   int naoFoiOrdenado;  // Flag que indica se o bucket já foi ordenado.
} Bucket;

int tamvet; 	// Tamanho do vetor de inteiros
int nbuckets;	// Número de buckets
int nthreads;	// Número de Threads
int contador_global = 0;	// Contador para que as threads não ordenem o mesmo bucket
Bucket *bucket_list;	// Vetor de buckets, ordenados de 0 à nbuckets-1
sem_t semaforo;		// Semáforo para proteger a região crítica


// Função que ordena um vetor em ordem crescente
void bubble_sort(int *vetor, int tam) {
  int i, j, temp, trocou;
  for(j = 0; j < tam - 1; j++) {
    trocou = 0;
    for(i = 0; i < tam - 1; i++) {
      if(vetor[i+1] < vetor[i]) {
        temp = vetor[i];
        vetor[i] = vetor[i+1];
        vetor[i+1] = temp ;
        trocou = 1;
      }
    }
    if(!trocou) break ;
  }
}

// Função chamada para liberar a memória alocada durante a execução.
void desalocarMemoria() {
	int i;
	for (i = 0; i < nbuckets; i++) {
		free(bucket_list[i].dados);
	}
	free(bucket_list);
}

// Função chamada para retornar os valores de cada bucket para o vetor.
void devolverDosBucketsProVetor(int *vetor) {
	int contador = 0;
	int i;
	int j;
	for (i = 0; i < nbuckets; i++) {
		for (j = 0; j < bucket_list[i].tamanho; j++) {
			vetor[contador] = bucket_list[i].dados[j];
			contador++;
		}
	}
}

// Função executada pelas Threads criadas pela main Thread.
// Percorre cada bucket "perguntando" se ele ja foi ordenado por uma outra Thread. Se ainda não, a Thread ordena o bucket.
/* A Thread seleciona um bucket para ordená-lo, dentro da região crítica incrementa o contador e ajusta o bucket para "ja foi ordenado"
   mas só imprime na tela e executa o bubble_sort depois de liberar o semáforo. */
void *vaiTrabalhar(void* id) {
	int bucketASerOrdenado = 0;
	while (contador_global < nbuckets) {
		sem_wait(&semaforo); 	// Início da região crítica
		if(contador_global < nbuckets) {
			if (bucket_list[contador_global].naoFoiOrdenado) {
				bucketASerOrdenado = contador_global;
			}
			contador_global++;
			bucket_list[bucketASerOrdenado].naoFoiOrdenado = 0;
			sem_post(&semaforo); 	// Fim da região crítica.
			printf("Thread %ld processando o bucket %u\n", (long int)id, bucketASerOrdenado);
			bubble_sort(bucket_list[bucketASerOrdenado].dados, bucket_list[bucketASerOrdenado].tamanho);

		} else {
			sem_post(&semaforo);
		}
	}
	pthread_exit(NULL);
}

// Função que cria um vetor de Threads, manda elas realizarem um trabalho e depois sincroniza todas as Threads.
void criarAsThreadsEMandaElasTrabalharem() {
	pthread_t threads[nthreads];
	long int i;
	for(i = 0; i < nthreads; i++) {
	  pthread_create(&threads[i], NULL, vaiTrabalhar, (void*)i);
	}
	for(i = 0; i < nthreads; i++) {
	  pthread_join(threads[i], NULL);
	}
}

// Dado um valor e um indice essa função poem esse valor dentro de bucket_list[indice]
// O tamanho do vetor dados, dentro do bucket, é realocado a cada incremento fazendo com que o vetor seja flexível.
void inserirNesseBucket(int valor, int indice) {
	int tam = bucket_list[indice].tamanho;
	bucket_list[indice].dados[tam] = valor;
	int *temporario = realloc(bucket_list[indice].dados, (tam+2) * sizeof(int));
	if (temporario != NULL) {
		bucket_list[indice].dados = temporario;
		bucket_list[indice].tamanho++;
	} else {
		printf("Houve um erro na hora de realocar memória.");
	}
}

// Percorre todos os buckets para ver a qual deles esse valor pertence.
void procurarBucketCorreto(int valor) {
	int i;
	for (i = 0; i < nbuckets; i++) {
		if (valor >= bucket_list[i].limiteInferior && valor <= bucket_list[i].limiteSuperior) {
			inserirNesseBucket(valor, i);
			break;
		}
	}
}

// Percorre o vetor, distribuindo seus elementos entre os buckets.
void percorrerOVetorEDistribuirOsValores(int *vetor) {
	int i;
	for (i = 0; i < tamvet; i++) {
		procurarBucketCorreto(vetor[i]);
	}
}

// Cria todos os buckets que tem faixa de valores com tamanho tamvet/nbuckets +1 e determina quais os valores cada bucket receberá.
// Caso a divisão de tamvet por nbuckets não tenha resto essa função não será executada pois todos os buckets terão o mesmo "tamanho".
void criarOsBucketsMaiores(int tamanhoDasFaixasDeValores, int qtdDeBucketsMaiores) {
	int i;
	for (i = nbuckets - qtdDeBucketsMaiores; i < nbuckets; i++) {
	  bucket_list[i].id = i;
	  bucket_list[i].limiteInferior = bucket_list[i-1].limiteSuperior + 1;
	  bucket_list[i].limiteSuperior = bucket_list[i].limiteInferior + tamanhoDasFaixasDeValores;
	  bucket_list[i].tamanho = 0;
	  bucket_list[i].dados = malloc(sizeof(int));
	  bucket_list[i].naoFoiOrdenado = 1;
	}
}

// Cria todos os buckets que tem faixa de valores com tamanho tamvet/nbuckets e determina quais os valores cada bucket receberá.
void criarOsBucketsMenores(int tamanhoDasFaixasDeValores, int qtdDeBucketsMaiores) {
	int i;
	for (i = 0; i < nbuckets - qtdDeBucketsMaiores; i++) {
	  bucket_list[i].id = i;
	  bucket_list[i].limiteInferior = i * tamanhoDasFaixasDeValores;
	  bucket_list[i].limiteSuperior = (i+1) * tamanhoDasFaixasDeValores - 1;
	  bucket_list[i].tamanho = 0;
	  bucket_list[i].dados = malloc(sizeof(int));
	  bucket_list[i].naoFoiOrdenado = 1;
	}
}

// Determina o "tamanho" de cada faixa de valores. Ex: tamanho=5 -> 0~6.
// Determina quantos buckets vão ter uma faixa de valores com um valor a mais que os outros.
// Aloca espaço na bucket_list,  manda criar os buckets e distribuir o vetor entre eles.
void distribuirOsValoresDoVetorNosBuckets(int *vetor) {
  int tamanhoDasFaixasDeValores = tamvet / nbuckets;
  int qtdDeBucketsQueSaoMaiores = tamvet % nbuckets;
  bucket_list = malloc((sizeof(Bucket)) * nbuckets);
  criarOsBucketsMenores(tamanhoDasFaixasDeValores, qtdDeBucketsQueSaoMaiores);
  if (qtdDeBucketsQueSaoMaiores > 0) {
	  criarOsBucketsMaiores(tamanhoDasFaixasDeValores, qtdDeBucketsQueSaoMaiores);
  }
  percorrerOVetorEDistribuirOsValores(vetor);
}

// Mostra o vetor na tela.
void exibirVetor(int *vetor) {
	int i;
	for(i = 0; i < tamvet; i++) {
		printf("vetor[%u] = %u\n", i, vetor[i]);
	}
}

// Gera números aleatórios para inserir no vetor.
void inserirValoresAleatoriosNoVetor(int *vetor) {
	int i;
	srand((unsigned)time(NULL));
	for(i = 0; i < tamvet; i++) {
		vetor[i] = rand() % tamvet;
	}
}


int main(int argc, char **argv) {
	tamvet = atoi(argv[1]);
	nbuckets = atoi(argv[2]);
	nthreads = atoi(argv[3]);

	// Se o número de threads for menor que 1 ou quando o número de buckets for maior que o tamanho do vetor
	// o programa informa um erro ao usuário e interrompe a execução.
	if (nthreads < 1 || nbuckets > tamvet) {
		printf("Houve um erro nos parâmetros passados!\n");
		return 1;
	}

	int vetor[tamvet];
	inserirValoresAleatoriosNoVetor(vetor);
	exibirVetor(vetor);
	distribuirOsValoresDoVetorNosBuckets(vetor);

	sem_init(&semaforo, 0, 1);
	criarAsThreadsEMandaElasTrabalharem();
	sem_destroy(&semaforo);

	devolverDosBucketsProVetor(vetor);
	exibirVetor(vetor);
	desalocarMemoria();
	return 0;
}
