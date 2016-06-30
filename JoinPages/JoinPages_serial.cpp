#include <iostream>
#include <algorithm>
#include <vector>
#include <string>
#include <fstream>
#include <sstream>
#include <time.h>

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

	string text = "";
	string title = "";
	string url = "";
	string content = "";
	string koUrl = "";
	string enUrl = "";
	string word = "";
	string tmpKoTitle = "";

	vector<string> wordList;
	string enLine, koLine;

	int list_sz;

	while(getline(enFile, enLine))
	{
		text = enLine;
		title = searchTitle(text);
		url = searchUrl("en", title);

		if(title != "" && url!="")
		{
			koFile.clear();
			koFile.seekg(0, ios::beg);

			while(getline(koFile, koLine))
			{
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
			title = "";
			url = "";
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

	clock_t time = clock();
	join_problem(koFile, enFile, outFile);
	time = clock() - time;
	cout << "Total time of Join Pages Serial: " << (((float)(time))/CLOCKS_PER_SEC) << " secs" << endl;

	return 0;
}
