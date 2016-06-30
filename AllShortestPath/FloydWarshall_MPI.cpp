#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <string.h>
#include <iostream>
#include <sstream>
#include <mpi.h>

#define INF_NUM 1<<28
#define NO_PAHT -1
#define N 2000

using namespace std;

int matDis[N][N], matPre[N][N], matDisAnt[N][N], matPreAnt[N][N], n, numThreads;

inline int gMin(int a, int b){ return a < b ? a : b;}

void initMatrixAdjaAndPrec(const char* nameFile){	
	ifstream myFile(nameFile);
	int i = 0;
	for(string line; getline(myFile, line); i++){
		istringstream iss(line);
		for(int j = 0; j < n; j++){
			iss >> matDisAnt[i][j];
			matDis[i][j] = matDisAnt[i][j];
			if(matDisAnt[i][j] != INF_NUM){
				// Existe camino del nodo i al nodo j
				// Su predecesor de j es i
				matPreAnt[i][j] = i;
			}
		}		
	}
	myFile.close();	
}

void printMatrix(int n, int A[N][N]){
	for(int i = 0; i < n; i++)
		for(int j = 0; j < n; j++)
			printf("%d%c", A[i][j], j + 1 == n ? '\n' : ' ');
}

char* genNameFile(int x){
	stringstream ss;
	ss << "matrixAdjacency_" << x;
	string nFile = ss.str();
  	return (char*) nFile.c_str();  	
}

int main(){	
	int myRank, numberPro, locN;	

	MPI_Init(NULL, NULL);
	MPI_Comm_size(MPI_COMM_WORLD, &numberPro);
	MPI_Comm_rank(MPI_COMM_WORLD, &myRank);

	//Master process read number of nodes	
	if(myRank == 0){
		int a = scanf("%d", &n);
		if(a == 0) printf("Error to read\n");								
	}
	MPI_Bcast(&n, 1, MPI_INT, 0, MPI_COMM_WORLD);			
	locN = n / numberPro;	
	char* nameFile = genNameFile(n);	

	// Todos los procesos leen el mismo archivo que contiene la matriz de adyacencia 	
	initMatrixAdjaAndPrec(nameFile);
	
	// Floyd Warshall Algorithm	
	if(myRank == 0){
		printf("Loading data\n");
		// printMatrix(n, matDisAnt);
	}

	MPI_Datatype rowTypeMPI;	
	MPI_Type_contiguous(n, MPI_INT, &rowTypeMPI);
	MPI_Type_commit(&rowTypeMPI);	

	MPI_Barrier(MPI_COMM_WORLD);
	// MPI_Allgather(matDis, 10, MPI_INT, matDisAnt, 10, MPI_INT, MPI_COMM_WORLD);
	double time = MPI_Wtime();	
	for(int k = 0; k < n; k++){	
		for(int i = myRank * locN, ii = 0; i < (myRank + 1) * locN; i++, ii++){
			for(int j = 0; j < n ; j++){
				matPre[ii][j] = matDisAnt[i][j] <= matDisAnt[i][k] + matDisAnt[k][j] ? matPreAnt[i][j] : matPreAnt[k][j];
				matDis[ii][j] = gMin(matDisAnt[i][j], matDisAnt[i][k] + matDisAnt[k][j]);
			}
		}		
		MPI_Allgather(matDis, locN, rowTypeMPI, matDisAnt, locN, rowTypeMPI, MPI_COMM_WORLD);
		MPI_Allgather(matPre, locN, rowTypeMPI, matPreAnt, locN, rowTypeMPI, MPI_COMM_WORLD);	
	}
	time = MPI_Wtime() - time;

	if(myRank == 0){
		printf("Total time of FloydWarshall Algorithm OpenMP with graph of %dx%d ==> %lf s.\n", n, n, time);
		// printMatrix(n, matDisAnt);		
	}

	MPI_Finalize();
	return 0;
}
 