/*
 ============================================================================
 Name        : JoinPages_mpi.cpp
 Description : Join Pages using MPI C++
 ============================================================================
 */

#include <math.h>
#include <iostream>
#include <algorithm>
#include <stdio.h>
#include <cstdlib>
#include <vector>
#include <string.h>
#include <fstream>
#include <sstream>
#include <mpi.h>

using namespace std;

 #define N 3000

bool arrayFlag[100];

string searchTitle(const string text) {
	string title = "";
	if (text.substr(0,11) == "    <title>"){
		title = text.substr(11, text.length()-19);
	}
	return title;
}

string searchUrl(const string bd, const string title) {
	string url = title;
	if(url != "")	{
		replace( url.begin(), url.end(), ' ', '_');
		url = "https://" + bd + ".wikipedia.org/wiki/" + url;
	}
	return url;
}

string searchContent(const string text) {
	string content = "";
	if (text.substr(0,11) == "      <text")
		content = text.substr(39, text.length()-39);
	else if (text.substr(0,1) != " " && text.substr(0,2) != "</" && text.substr(0,1) != "<")
		content = text;
	return content;
}

vector<string> extractEngKeyword(const string content) {
	vector<string> wordList;
	string word = "";
	istringstream iss(content);

	char letter;
	int size = content.length() - 1;

	for(int i=0; i<size; i++)	{
		letter = content[i];
		if(isalnum(letter))		{
			word += letter;
		}else{
			if(word != ""){
				if(letter == ' ' || letter == '-'){
					if(isalnum(content.at(i+1))){
						word += letter;
					}
				}else{
					wordList.push_back(word);
					word = "";
				}
			}
		}
	}
	return wordList;
}

bool isMatched(string word1, string word2) {
	return (word1 == word2);
}

void write(string title, string koUrl, string word, string enUrl) {
	cout << "===== CREATE HIPERLINK =====\n";
	cout << "> Word: " << word << " == Title: " << title << endl;
	cout << "> Korean page: " << koUrl << endl;
	cout << "> English page: " << enUrl << endl;
}

int getFileLines(char *fileName) {
	ifstream file(fileName);
	if(!file.is_open()) return -1;
	int numLines = 0;
	string line = "";
	while(getline(file, line)){
		numLines++;
	}
	file.close();
	return numLines;
}

void join_problem(char *koFileName, char *enFileName, char *outFileName) {
	ifstream enFile(enFileName);
	ifstream koFile;

	if(!enFile.is_open()){
		cout << "Cannot open files: " << enFileName << " o " << koFileName << endl;
		return;
	}

	int koFileLineSize = getFileLines(koFileName);
	int enFileLineSize = getFileLines(enFileName);

	MPI_Init(NULL, NULL);

	double time = MPI_Wtime();
	int my_rank, comm_sz;
	MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);	
	MPI_Comm_size(MPI_COMM_WORLD, &comm_sz);

	string text = "";
	string content = "";
	char title[N] = "";
	char enUrl[N] = "";
	char koUrl[N] = "";
	char word[N];
	string tmpKoTitle = "";
	string enLine, koLine;
	vector<string> wordList;

	bool isLast = false;
	int list_sz;

	if(my_rank == 0)	{
		time = MPI_Wtime();

		bool isLastAll = false;
		int jj = 1;
		for(int i = 1; i < comm_sz; i++)
			arrayFlag[i] = false;

		while (1){
			isLastAll = true;
			for(int i=1; i<comm_sz; i++) {
				if(!arrayFlag[i]){
					MPI_Recv(title, N, MPI_CHAR, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
					MPI_Recv(koUrl, N, MPI_CHAR, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
					MPI_Recv(word, N, MPI_CHAR, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
					MPI_Recv(enUrl, N, MPI_CHAR, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
					MPI_Recv(&arrayFlag[i], 1, MPI::BOOL, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);				

					jj++;
					if(arrayFlag[i])
						printf("Se recibio un fin de %d\n", i);

					isLastAll = (isLastAll && arrayFlag[i]);
					if(strcmp(enUrl, "") != 0 && !arrayFlag[i])
						write(title, koUrl, word, enUrl);
				}	
			}

			if(isLastAll){
				printf("Todos acabaron \n");
				break;
			}
		}
		time = MPI_Wtime() - time;
	}else{
		int ini = ((my_rank-1) * enFileLineSize) / (comm_sz-1);
		int fin = (my_rank * enFileLineSize) / (comm_sz-1);
		int ii = 1;

		for(int i=0; i<fin; i++){
			getline(enFile, text);
			strcpy(title, searchTitle(text).c_str());

			if(i >= ini && strcmp(title, "") != 0) {
				koFile.open(koFileName);
				for(int k=0; k<koFileLineSize; k++){
					getline(koFile, koLine);
					tmpKoTitle = searchTitle(koLine);

					if(tmpKoTitle != ""){
						string tempA = searchUrl("ko", tmpKoTitle);
						strcpy(koUrl, tempA.c_str());
					}

					content = searchContent(koLine);
					wordList = extractEngKeyword(content);

					list_sz = wordList.size();					

					for(int i=0; i<list_sz; i++){
						strcpy(word, wordList[i].c_str());

						if(isMatched(word, title)) {
							strcpy(enUrl, searchUrl("en", title).c_str());
						}
						else {
							tmpKoTitle = "";
							strcpy(enUrl, tmpKoTitle.c_str());
						}

						isLast = false;
						MPI_Send(title, N, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
						MPI_Send(koUrl, N, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
						MPI_Send(word, N, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
						MPI_Send(enUrl, N, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
						MPI_Send(&isLast, 1, MPI::BOOL, 0, 0, MPI_COMM_WORLD);
						ii++;						
					}
				}
				koFile.close();
			}
		}
		printf("termine %d %d %d %d %d %d\n", my_rank, ini, fin, enFileLineSize, isLast ? 1 : 0, ii);
	}
	if(my_rank != 0 ){
		isLast = true;	
		MPI_Send(title, N, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
		MPI_Send(koUrl, N, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
		MPI_Send(word, N, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
		MPI_Send(enUrl, N, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
		MPI_Send(&isLast, 1, MPI::BOOL, 0, 0, MPI_COMM_WORLD);
	}

	printf("llegue al final process:%d\n", my_rank);
	if(my_rank == 0)
		printf("Total time of Join Pages MPI: %lf secs\n", time);		
	MPI_Finalize();		
}

int main(int argc, char* argv[]){
	char *koFile = "test_ko.xml",
		*enFile = "wiki_en_n2.xml",
		*outFile = "output.xml";

	join_problem(koFile, enFile, outFile);	
	return 0;
}
