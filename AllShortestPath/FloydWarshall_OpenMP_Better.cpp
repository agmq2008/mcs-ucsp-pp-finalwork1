#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <string>
#include <iostream>
#include <sstream>
#include <omp.h>
#include <time.h>

#define INF_NUM 1<<30
#define NO_PAHT -1

using namespace std;

int **matDis, **matPre, **matDisAnt, **matPreAnt,  n, numThreads;

inline int gMin(int a, int b){ return a < b ? a : b;}

void initMatrixAdjaAndPrec(const char* nameFile, int cant){
	n = cant;
	matDis = new int*[n];
	matPre = new int*[n];
	matDisAnt = new int*[n];
	matPreAnt = new int*[n]; 
	for(int i = 0; i < n; i++){
		matDis[i] = new int[n];
		matPre[i] = new int[n];		 
		matPreAnt[i] = new int[n];
		matDisAnt[i] = new int[n];
	}
	ifstream myFile(nameFile);
	int i = 0;
	for(string line; getline(myFile, line); i++){
		istringstream iss(line);
		for(int j = 0; j < n; j++){
			iss >> matDisAnt[i][j];
			if(matDisAnt[i][j] != INF_NUM){
				// Existe camino del nodo i al nodo j
				// Su predecesor de j es i
				matPreAnt[i][j] = i;
			}
		}		
	}
	myFile.close();	
}

void printMatrix(int n, int **A){
	for(int i = 0; i < n; i++)
		for(int j = 0; j < n; j++)
			printf("%d%c", A[i][j], j + 1 == n ? '\n' : ' ');
}

void floydWarshallAllShortestPathOpenMP(){	
	#pragma omp parallel 	
	for(int k = 0; k < n; k++){
		#pragma omp for schedule(static, n / numThreads) collapse(2)
		for(int i = 0; i < n; i++){			 		
			for(int j = 0; j < n; j++){
				matPre[i][j] = matDisAnt[i][j] <= matDisAnt[i][k] + matDisAnt[k][j] ? matPreAnt[i][j] : matPreAnt[k][j];
				matDis[i][j] = gMin(matDisAnt[i][j], matDisAnt[i][k] + matDisAnt[k][j]);
			}
		}
		#pragma omp for schedule(static, 2 * n / numThreads)
		for(int i = 0; i < n; i++)
			for(int j = 0; j < n; j++){
				matDisAnt[i][j] = matDis[i][j];
				matPreAnt[i][j] = matPre[i][j];
			}
	}
}

int main(){			
	int cant;	
	char* nameFile = (char*)malloc(sizeof(char*));	
	scanf("%s %d %d", nameFile, &cant, &numThreads);
	
	printf("Loading data\n");
	initMatrixAdjaAndPrec(nameFile, cant);
	printf("Init FloyWarshall Algorithm\n");	
	// printMatrix(cant, matDisAnt);

	omp_set_num_threads(numThreads);
	double time = omp_get_wtime();
	floydWarshallAllShortestPathOpenMP();
	time = omp_get_wtime() - time;

	printf("Total time of FloydWarshall Algorithm OpenMP better with graph of %dx%d ==> %lf s.\n", n, n, time);
	// printMatrix(cant, matDis);

	return 0;
}
 