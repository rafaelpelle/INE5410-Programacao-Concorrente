#include <unistd.h>
#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>


// Estrutura tipo Bucket
typedef struct Bucket {
   int id;				// Identificador do bucket.
   int limiteSuperior;	// Limite da faixa de valores que esse bucket receberá.
   int limiteInferior;	// Limite da faixa de valores que esse bucket receberá.
   int tamanho;			// Quantidade de elementos dentro do bucket.
   int* dados; 			// Vetor de elementos do bucket.
} Bucket ;


int tamvet; 	// Tamanho do vetor de inteiros
int nbuckets;	// Número de buckets
int nprocs;	// Número de processos
Bucket *bucket_list;	// Vetor de buckets, ordenados de 0 à nbuckets-1


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
		printf("%u ", vetor[i]);
	}
	printf("\n");
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
	int rank;
	MPI_Status st;
	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	tamvet = atoi(argv[1]);
	nbuckets = atoi(argv[2]);
	MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
	// Se o número de processos for menor que dois ou quando o número de buckets for maior que o tamanho do vetor
	// o programa informa um erro ao usuário e interrompe a execução.
	if (nprocs < 2 || nbuckets > tamvet) {
		printf("Houve um erro nos parâmetros passados!\n");
		return 1;
	}

	if (rank == 0) {  // Se processo mestre, então:
		int vetor[tamvet];
		inserirValoresAleatoriosNoVetor(vetor);
//		exibirVetor(vetor);
		distribuirOsValoresDoVetorNosBuckets(vetor);
		int contador_buckets_enviados = 0;
		int contador_buckets_recebidos = 0;
		int process_id;
		for (process_id = 1; process_id < nprocs; process_id++) {   // Manda um bucket para cada processo escravo.
			if (bucket_list[contador_buckets_enviados].tamanho > 1) {
				if (contador_buckets_enviados < nbuckets) {
					int bucketSize = bucket_list[contador_buckets_enviados].tamanho;
					int bucket_id = bucket_list[contador_buckets_enviados].id;
					MPI_Send(&bucketSize, 1, MPI_INT, process_id, bucket_id, MPI_COMM_WORLD);
					MPI_Send(bucket_list[contador_buckets_enviados].dados, bucketSize, MPI_INT, process_id, bucket_id, MPI_COMM_WORLD);
//					printf("Mestre ENVIOU bucket %u para Escravo %u\n", bucket_id, process_id);
					contador_buckets_enviados++;
				}
			} else {  // Se tamanho do bucket <= 1, ajusta os contadores sem enviar o bucket
				process_id--;
				contador_buckets_enviados++;
				contador_buckets_recebidos++;
			}
		}
		//Enquanto o meste ainda tem bucket para receber, recebe e se necessário manda outro bucket para o processo escravo.
		while (contador_buckets_recebidos < nbuckets) {
			int bucketSize;
			MPI_Recv(&bucketSize, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &st);
			int bucket_id = st.MPI_TAG;			
			MPI_Recv(bucket_list[bucket_id].dados, bucketSize, MPI_INT, st.MPI_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &st);
			int process_id = st.MPI_SOURCE;
//			printf("Mestre RECEBEU bucket %u do Escravo %u\n", bucket_id, process_id);
			contador_buckets_recebidos++;
			if (contador_buckets_enviados < nbuckets) {
				int bucketSize = bucket_list[contador_buckets_enviados].tamanho;
				bucket_id = bucket_list[contador_buckets_enviados].id;
				MPI_Send(&bucketSize, 1, MPI_INT, process_id, bucket_id, MPI_COMM_WORLD);				
				MPI_Send(bucket_list[contador_buckets_enviados].dados, bucketSize, MPI_INT, process_id, bucket_id, MPI_COMM_WORLD);
//				printf("Mestre ENVIOU bucket %u para Escravo %u\n", bucket_id, process_id);
				contador_buckets_enviados++;
			}
		}
		// Manda mensagem para os escravos pararem de trabalhar.
		int ja_terminou = -1;
		for (process_id = 1; process_id < nprocs; process_id++) {
			MPI_Send(&ja_terminou, 1, MPI_INT, process_id, 0, MPI_COMM_WORLD);
		}
		devolverDosBucketsProVetor(vetor);
//		exibirVetor(vetor);
		desalocarMemoria();
	} else {  // Se processo escravo, então:
		int ainda_nao_terminou = 1;
		while (ainda_nao_terminou) {  // Enquanto não receber um bucketSize < 0 (Mensagem para parar).
			int bucketSize;
			MPI_Recv(&bucketSize, 1, MPI_INT, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &st);
			if (bucketSize < 0) {
				ainda_nao_terminou = 0;
			} else {
				int buffer[bucketSize];
				MPI_Recv(&buffer, bucketSize, MPI_INT, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &st);
				bubble_sort(buffer, bucketSize);
				MPI_Send(&bucketSize, 1, MPI_INT, 0, st.MPI_TAG, MPI_COMM_WORLD);
				MPI_Send(&buffer, bucketSize, MPI_INT, 0, st.MPI_TAG, MPI_COMM_WORLD);
			}
		}
	}
	MPI_Finalize();
	return 0;
}

