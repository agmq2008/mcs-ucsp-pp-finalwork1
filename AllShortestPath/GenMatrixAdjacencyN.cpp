#include <cstdlib>
#include <fstream>
#include <string.h>
#include <math.h>

using namespace std;

#define INF_NUM 1<<28

int **A;

int gRanNum(int min, int max){
	int res = (max - min) * ((double)rand() / (double)RAND_MAX) + min;
	if(res == 0.0) return gRanNum(min, max);
	return res;
}

void genMatrixAdjacency(int n){
	A = new int*[n];
	int number, flag;
	for(int i = 0; i < n; i++)
		A[i] = new int[n];
	for(int i = 0; i < n; i++){
		A[i][i] = INF_NUM;
		for(int j = i + 1 ; j < n; j++){
			number = gRanNum(10, 1000);
			flag = gRanNum(1, 30);						
			A[i][j] = A[j][i] = INF_NUM;						
			if(flag >= 20){
				// Existe camino de ida y vuelta entre nodo i y j
				A[i][j] = A[j][i] = number; 
			}else if(flag > 12){
				// Existe camino del nodo i al nodo j, pero no de j a i
				A[i][j] =  number;
			}else if(flag > 6){
				// Existe camino del nodo j al nodo i, pero no de i a j
				A[j][i] = number;
			}			
		}
	}	
}

void genFileMatrixAdjacency(int n, const char * nameFile){
	genMatrixAdjacency(n); 
	ofstream myFile;
	myFile.open(nameFile);	
	for(int i = 0; i < n; i++){	
		for(int j = 0; j < n; j++){			
			myFile << A[i][j];
			myFile << ( j + 1 != n ? ' ' : '\n');
		}
	}	
	myFile.close();
}

int main(){
	srand(time(NULL));
	genFileMatrixAdjacency(10  , "matrixAdjacency_10");
	genFileMatrixAdjacency(100 , "matrixAdjacency_100");
	genFileMatrixAdjacency(1000, "matrixAdjacency_1000");
	genFileMatrixAdjacency(2000, "matrixAdjacency_2000");
	genFileMatrixAdjacency(3000, "matrixAdjacency_3000");
	return 0;
}
 
