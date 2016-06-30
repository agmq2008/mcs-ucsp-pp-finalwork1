#include <iostream>
#include <algorithm>
#include <cstdio>
#include <vector>
#include <string>
#include <fstream>
#include <sstream>
#include <omp.h>

using namespace std;

string searchTitle(const string text)
{
	string title = "";

	if (text.substr(0,11) == "    <title>")
	{
		title = text.substr(11, text.length()-19);
	}
	return title;
}

string searchUrl(const string bd, const string title)
{
	string url = title;

	if(url != "")
	{
		replace( url.begin(), url.end(), ' ', '_');
		url = "https://" + bd + ".wikipedia.org/wiki/" + url;
	}
	return url;
}

string searchContent(const string text)
{
	string content = "";

	if (text.substr(0,11) == "      <text")
		content = text.substr(39, text.length()-39);
	else if (text.substr(0,1) != " " && text.substr(0,2) != "</" && text.substr(0,1) != "<")
		content = text;

	return content;
}

vector<string> extractEngKeyword(const string content)
{
	vector<string> wordList;
	string word = "";
	istringstream iss(content);

	char letter;
	int size = content.length() - 1;

	for(int i=0; i<size; i++)
	{
		letter = content[i];

		if(isalnum(letter))
		{
			word += letter;
		}
		else
		{
			if(word != "")
			{
				if(letter == ' ' || letter == '-')
				{
					if(isalnum(content.at(i+1)))
					{
						word += letter;
					}
				}
				else
				{
					wordList.push_back(word);
					word = "";
				}
			}
		}
	}
	return wordList;
}

bool isMatched(string word1, string word2)
{
	return (word1 == word2);
}

void write(string title, string koUrl, string word, string enUrl)
{
	cout << "===== CREATE HIPERLINK =====\n";
	cout << "> Word: " << word << " == Title: " << title << endl;
	cout << "> Korean page: " << koUrl << endl;
	cout << "> English page: " << enUrl << endl;
}

void join_problem(char *koFileName, char *enFileName, char *outFileName)
{
	ifstream enFile(enFileName);
	ifstream koFile(koFileName);

	if(!enFile.is_open() || !koFile.is_open())
	{
		cout << "Cannot open files: " << enFileName << " o " << koFileName << endl;
		return;
	}

	int kN, eN;
	int max_eS, max_kS;
	int eS, kS;

	max_eS = 160;
	max_kS = 80;

	fstream tmpEnFile, tmpKoFile;
	string enLine, koLine;

	while(getline(enFile, enLine))
	{
		// Generate eBlock
		//cout << "Generating eBlock ..." << endl;
		tmpEnFile.open("tmpEn.xml", ios::out | ios::trunc);
		eS = 0;
		do {
			tmpEnFile << enLine << "\n";
			eS++;
		}
		while(eS <= max_eS && getline(enFile, enLine));
		tmpEnFile.close();

		koFile.clear();
		koFile.seekg(0, ios::beg);

		while(getline(koFile, koLine))
		{
			// Generate kBlock
			//cout << "Generating kBlock ..." << endl;
			tmpKoFile.open("tmpKo.xml", ios::out | ios::trunc);
			kS = 0;
			do {
				tmpKoFile << koLine << "\n";
				kS++;
			}
			while(kS <= max_kS && getline(koFile, koLine));
			tmpKoFile.close();

#pragma omp parallel
			{
				int tN, tid;
				tN = omp_get_num_threads();
				tid = omp_get_thread_num();

				int ini, fin, list_sz;
				ini = (tid * eS) / tN;
				fin = ((tid + 1) * eS) / tN;

				ifstream tmpEn, tmpKo;
				string text = "";
				string title = "";
				string url = "";
				string enUrl = "";
				string content = "";
				string koUrl = "";
				string word = "";
				string tmpKoTitle = "";
				string enLine, koLine;
				vector<string> wordList;

				tmpEn.open("tmpEn.xml", ios::in);

				for(int j=0; j<fin; j++)
				{
					getline(tmpEn, text);

					if(j < ini) continue;

					title = searchTitle(text);
					url = searchUrl("en", title);

					if(title != "" && url!="")
					{
						tmpKo.open("tmpKo.xml", ios::in);

						for(int k=0; k < kS; k++)
						{
							getline(tmpKo, koLine);

							tmpKoTitle = searchTitle(koLine);
							koUrl = (tmpKoTitle != "") ? searchUrl("ko", tmpKoTitle) : koUrl;

							content = searchContent(koLine);
							wordList = extractEngKeyword(content);
							list_sz = wordList.size();

							for(int i=0; i<list_sz; i++)
							{
								word = wordList[i];

								if(isMatched(word, title))
								{
									write(title, koUrl, word, url);
								}
							}
						}
						tmpKo.close();
						title = "";
						url = "";
					}
				}
				tmpEn.close();
			}
		}
	}
	enFile.close();
	koFile.close();
}

int main(int argc, char* argv[])
{
	char *koFile, *enFile, *outFile;

	// We expect 4 arguments: program, EN file name, KO file name, OUT file name
    if (argc < 4) {
        cerr << "We expect 4 arguments: program, EN file name, KO file name, OUT file name" << endl;
        enFile = "wiki_en_n2.xml";
        koFile = "test_ko.xml";
        outFile = "output.xml";
    }
    else
    {
    	enFile = argv[1];
		koFile = argv[2];
		outFile = argv[3];
    }

    double time = omp_get_wtime();
	join_problem(koFile, enFile, outFile);
	time = omp_get_wtime() - time;
	cout << "Total time of Join Pages OMP: " << ((float)(time)) << " secs" << endl;

	return 0;
}
