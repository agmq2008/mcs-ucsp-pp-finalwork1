#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <string>
#include <iostream>
#include <sstream>
#include <time.h>

#define INF_NUM 1<<28
#define NO_PAHT -1

using namespace std;

int **matDis, **matPre, n;

inline int gMin(int a, int b){ return a < b ? a : b;}

void initMatrixAdjaAndPrec(const char* nameFile, int cant){
	n = cant;
	matDis = new int*[n];
	matPre = new int*[n];
	for(int i = 0; i < n; i++){
		matDis[i] = new int[n];
		matPre[i] = new int[n];
	}
	ifstream myFile(nameFile);
	int i = 0;
	for(string line; getline(myFile, line); i++){
		istringstream iss(line);
		for(int j = 0; j < n; j++){
			iss >> matDis[i][j];
			if(matDis[i][j] != INF_NUM){
				// Existe camino del nodo i al nodo j
				// Su predecesor de j es i
				matPre[i][j] = i;
			}
		}		
	}
	myFile.close();	
}

void printMatrix(int n){
	for(int i = 0; i < n; i++)
		for(int j = 0; j < n; j++)
			printf("%d%c", matDis[i][j], j + 1 == n ? '\n' : ' ');
}

void floydWarshallAllShortestPath(){
	for(int k = 0; k < n; k++){
		for(int i = 0; i < n; i++){
			for(int j = 0; j < n; j++){
				matPre[i][j] = matDis[i][j] <= matDis[i][k] + matDis[k][j] ? matPre[i][j] : matPre[k][j];
				matDis[i][j] = gMin(matDis[i][j], matDis[i][k] + matDis[k][j]);
			}
		}
	}
}

int main(){			
	int cant;	
	char* nameFile = (char*)malloc(sizeof(char*));	
	scanf("%s %d", nameFile, &cant);

	printf("Loading data\n");
	initMatrixAdjaAndPrec(nameFile, cant);
	printMatrix(cant);
	printf("Init FloyWarshall Algorithm\n");
	clock_t time = clock();	
	floydWarshallAllShortestPath();
	time = clock() - time;		
	printf("Total time of FloydWarshall Algorithm with graph of %dx%d ==> %f s.\n", n, n, ((float)(time))/CLOCKS_PER_SEC);	
	printMatrix(cant);

	return 0;
}
 