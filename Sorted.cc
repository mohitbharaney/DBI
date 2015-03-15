/*
 * Sorted.cc
 *
 *  Created on: Mar 7, 2015
 *      Author: mugdha
 */
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "BigQ.h"
#include "Defs.h"
#include <string.h>
#include <stdlib.h>
#include <iostream>
#include <fstream>
#include "Sorted.h"
#include<sstream>
#include "Heap.h"
#include "GenericDB.h"

int buffsz = 100; // pipe cache size
Pipe *input; //(buffsz);
Pipe *output; //(buffsz);
int runLen;
OrderMaker *sortorder = new OrderMaker();
char fname[400];
char metaDataFileName[400];
ComparisonEngine cmp;
OrderMaker query;
pthread_t work;
Sorted::Sorted() {

	//metaDataFileName = "metaData.txt";
	mode = START;
	rwBuffer = new Page();
	readRecord = 0;
	writePage = 0;
	readPage = 0;
}

/*
 *check if file path is NULL, if yes return 0 otherwise
 *create a new file and return 1
 */
int Sorted::Create(char *f_path, fType f_type, void *startup) {
	if (f_path != NULL) {

		strcpy(&fname[0], f_path);
		//cout << "in create" << fname << endl;
		struct SortInfo* data = (struct SortInfo*) (startup);
		runLen = data->runLength;
		sortorder = data->myOrder;
//		cout << "printing shit" << endl;
//		sortorder->Print();

//		cout<<"testing tpstring function"<<endl;
//		cout<<sortorder->toString();
//		cout<<endl;

		string fileName(f_path);
		fileName = fileName.substr(0, fileName.length() - 3);
		fileName = fileName + "md";

		strcpy(&metaDataFileName[0], &fileName[0]);

		/*
		 extract file name and convert it to fileName.md for the metaData file

		 */

		ofstream mdFile;
		mdFile.open(metaDataFileName);
		mdFile << "sorted" << endl;
		mdFile << "readPage " << 1 << endl;
		mdFile << "readRecord " << 0 << endl;
		mdFile << "writePage " << 1 << endl;
		mdFile << sortorder->toString() << endl;

		mdFile.close();

		/*
		 re initialize the md file on create
		 */

		dbFile.Open(0, f_path);
		//dbFile.AddPage(rwBuffer, 0);
		dbFile.Close();
		Open(f_path);
		return 1;
	} else
		return 0;

}

/** Open the filepath provided by loadpath.
 *while Loop through records in the file and load it into the temp record variable
 * Keep appending it to a page and when the page is full write this page out
 * to the disk using an instance of file class. Clear the page buffer for further
 * loads
 */
void Sorted::Load(Schema & f_schema, char *loadpath) {

	FILE *tableFile = fopen(loadpath, "r");
	int count = 0;
	Record temp;

	//cout<<"write page in load -------------------------- "<<writePage<<endl;
	if (tableFile != NULL) {
		while (temp.SuckNextRecord(&f_schema, tableFile) == 1) {

			Add(temp);
		}

		merge();
	} else
		cout << "path is null or file doesnt exist";

}

/**
 *open the DB file, success case returns 1 and failure case returned with 0
 */

int Sorted::Open(char *f_path) {
	if (f_path != NULL) {

		//cout << "check point in open" << endl;
		dbFile.Open(1, f_path);	//first parameter is non zero so that files not created again and is opened in read write mode.
		//cout << "check point after open" << endl;
		string fileName(f_path);
		fileName = fileName.substr(0, fileName.length() - 3);
		fileName = fileName + "md";
		//	metaDataFileName=&fileName[0];
		//	metaDataFileName=
		strcpy(&metaDataFileName[0], &fileName[0]);

	//	cout << "md file name :" << metaDataFileName << endl;
		ifstream mdFile(metaDataFileName);
		string line;
		char *pch;

		if (mdFile.is_open()) {
			/*
			 * read first line and tokenize it to store the value of read_page
			 */
			getline(mdFile, line);
			getline(mdFile, line);
			pch = strtok(&line[0], " ");
			pch = strtok(NULL, " ");
			readPage = atoi(pch);	//convert char* to int
			/*
			 * read first line and tokenize it to store the value of read_record
			 */
			getline(mdFile, line);
			pch = strtok(&line[0], " ");
			pch = strtok(NULL, " ");
			readRecord = atoi(pch);	//convert char* to int
			/*
			 * read first line and tokenize it to store the value of write_page
			 */
			getline(mdFile, line);
			pch = strtok(&line[0], " ");
			pch = strtok(NULL, " ");
			writePage = atoi(pch);	//convert char* to int

			//cout<<"writePage "<<writePage<<endl;
			getline(mdFile, line);
			int numAtts = atoi(&line[0]);
			//get num atts

			getline(mdFile, line);//next line contains which atts rest code is to make an array which stores this value
			stringstream s(line);
			string temp;
			int whichAtts[numAtts];
			int count = 0;
			while (s >> temp && count < numAtts) {
				whichAtts[count] = atoi(&temp[0]);
				count++;
			}
			//get the type of attri

			getline(mdFile, line);
			stringstream s1(line);
			string temp1;
			int typeAtts[numAtts];
			count = 0;
			while (s1 >> temp1 && count < numAtts) {
				typeAtts[count] = atoi(&temp1[0]);
				count++;
			}

			mdFile.close();

			sortorder->setAtrr(numAtts, whichAtts, typeAtts);
		//	cout << "final test after reseting" << endl;
			sortorder->Print();

			/*
			 *
			 * test code
			 cout<<"num atts "<<numAtts<<endl;

			 for(int i=0;i<numAtts;i++)
			 cout<<whichAtts[i]<<" ";
			 cout<<endl;

			 for(int i=0;i<numAtts;i++)
			 cout<<typeAtts[i]<<" ";
			 cout<<endl;
			 */

			//cout << "end open" << endl;
		}
		return 1;
	} else
		return 0;

}

/**
 *Move the pointer to first record in file
 */
void Sorted::MoveFirst() {
	readPage = 1;
	readRecord = 0;
}

int Sorted::Close() {

	if (mode == WRITE)
	{
		//pthread_join(work,NULL);
		merge();
	}

	ofstream mdFile;
	mdFile.open(metaDataFileName);
	mdFile << "sorted" << endl;
	mdFile << "readPage " << readPage << endl;
	mdFile << "readRecord " << readRecord << endl;
	mdFile << "writePage " << writePage << endl;
	mdFile << sortorder->toString() << endl;

	mdFile.close();

	dbFile.Close();

	return 1;
}

void* work1(void* t) {
	BigQ bq(*input, *output, *sortorder, runLen);
	cout<<"exited big q sucessfully"<<endl;

}

void Sorted::Add(Record & rec) {

	switch (mode) {
	case START: {
		input = new Pipe(buffsz);
		output = new Pipe(buffsz);
		input->Insert(&rec);
		//cout << "before bigq initilization" << endl;

		pthread_create(&work, NULL, work1, NULL);

		//BigQ bq(*input, *output, *sortorder, runLen);
		//cout << "after bigq initilization" << endl;
		mode = WRITE;

	}
		break;
	case READ: {
		input = new Pipe(buffsz);
		output = new Pipe(buffsz);
		input->Insert(&rec);

		pthread_t worker;
		pthread_create(&worker, NULL, work1, NULL);

		mode = WRITE;
		break;
	}
	case WRITE: {
///		cout<<"in add"<<endl;
		input->Insert(&rec);
		break;
	}
	default:
		break;

	}
}

int Sorted::GetNext(Record & fetchme) {

	//cout << "in get next" << endl;
	switch (mode) {

	case START: {
		if (dbFile.GetLength() <= 2)
			return 0;
		dbFile.GetPage(rwBuffer, readPage);
		Record temp;
		int count = 0;
		while (count < readRecord) {
			rwBuffer->GetFirst(&temp);
			count++;
		}
		rwBuffer->GetFirst(&fetchme);
		readRecord++;
		mode = READ;
		break;

	}
	case WRITE: {

		merge();
		dbFile.AddPage(rwBuffer, writePage);
		rwBuffer->EmptyItOut();

		dbFile.GetPage(rwBuffer, readPage);
		Record temp;
		int count = 0;
		while (count <= readRecord) {
			rwBuffer->GetFirst(&temp);
			count++;
		}
		rwBuffer->GetFirst(&fetchme);
		readRecord++;
		mode = READ;
		break;

	}
	case READ: {
		if (!rwBuffer->GetFirst(&fetchme)) {
			readPage++;
			if (readPage < dbFile.GetLength() - 1) {
				dbFile.GetPage(rwBuffer, readPage);
				rwBuffer->GetFirst(&fetchme);
				readRecord = 0;
			} else
				return 0;
		}
		readRecord++;
		break;
	}

	default: {
		break;
	}

	}

	return 1;
}

int Sorted::GetNext(Record & fetchme, CNF & cnf, Record  &literal) {
	ComparisonEngine cmp1;

	Record test;
	test.Copy(&literal);
	cout<<"in get next"<<endl;
	if (mode != READ) {
		if (mode == WRITE)
			merge();
		int check = cnf.createQuery(*sortorder, query);
		if (check) {
			//cout<<"b4 binary search"<<endl;
			readPage = binarySearch(readPage, dbFile.GetLength() - 2, test,
					query);
			readRecord = 0;
			cout<<"ReadPage===="<<readPage<<endl;
		}
		mode = START;
	}

	//Record temp;
	Schema s("catalog","customer");
	if(readPage==-1)
		return 0;
	while (GetNext(fetchme)) {

		//fetchme.Print(&s);
		if (cmp1.Compare(&fetchme, &literal	, &cnf)) {
		cout<<"after comapre"<<endl;
			return 1;
		}

		if(cmp1.Compare(&fetchme,&literal,sortorder)>0)
		{
			cout<<"test here"<<endl;
			break;
		}

	}
	return 0;
}

void Sorted::merge() {

	mode = START;
	input->ShutDown();
	int count = 0;
	if (dbFile.GetLength() == 0) {
		dbFile.AddPage(rwBuffer, 0);
		//cout << "in if--------------------------------------------------"<< endl;
		//Page* buff=new Page();
		Record temp;
		//Schema s("catalog", "lineitem");

		while (output->Remove(&temp)) {
			//temp.Print(&s);
			if (!rwBuffer->Append(&temp)) {
				dbFile.AddPage(rwBuffer, writePage);
				writePage++;
				rwBuffer->EmptyItOut();
				rwBuffer->Append(&temp);

			}
			count++;
		}
		dbFile.AddPage(rwBuffer, writePage);
		rwBuffer->EmptyItOut();

		output->ShutDown();

		cout << "records inserted are---------------------------------------------- " << count << endl;

	} else {

		/*
		 * instantiate a heap type db file open
		 * use that to scan through the main file.
		 *
		 */

		Record fromOutputPipe;
		Record fromDBFile;

		Schema s("catalog", "lineitem");
		//	fromOutputPipe.Print(&s);
		//cout << "old file name:" << fname << endl;
		//cout << "in else" << endl;
		dbFile.Close();
		GenericDB *sortedFile = new Heap();
		sortedFile->Open(fname);
		ComparisonEngine comp;

		string newFileName(fname);
		unsigned found = newFileName.find_last_of("/\\");
		newFileName = newFileName.substr(0, found + 1);
		newFileName += "newDB.bin";

		//cout << newFileName << endl;

		GenericDB *newDB = new Heap();

		newDB->Create(&newFileName[0], heap, NULL);	//(0,"temp.bin");

		//cout<< "check point................................................................................."<< endl;

		sortedFile->MoveFirst();
		//cout<<"check point................................."<<endl;

		int f1 = output->Remove(&fromOutputPipe);

		int f2 = sortedFile->GetNext(fromDBFile);

		//cout
		//		<< "check point+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
		//		<< endl;
		while (f1 && f2) {
			if (comp.Compare(&fromOutputPipe, &fromDBFile, sortorder) < 0) {
				newDB->Add(fromOutputPipe);
				f1 = output->Remove(&fromOutputPipe);
			} else {
				newDB->Add(fromDBFile);
				f2 = sortedFile->GetNext(fromDBFile);
			}
			count++;

		}

		if (f1) {
			newDB->Add(fromOutputPipe);
			count++;
			while (output->Remove(&fromOutputPipe)) {
				newDB->Add(fromOutputPipe);
				count++;
			}
		} else {
			newDB->Add(fromDBFile);
			count++;
			while (sortedFile->GetNext(fromDBFile)) {
				newDB->Add(fromDBFile);
				count++;
			}
		}
		output->ShutDown();
		sortedFile->Close();
		newDB->Close();

		//cout << "no of recs in file     " << count << endl;
		remove(fname);
		rename(&newFileName[0], fname);
		newFileName = newFileName.substr(0, newFileName.length() - 3);
		newFileName += "md";

		//cout << endl;

		//cout << newFileName << endl;
		ifstream newMd(&newFileName[0]);
		string line;
		getline(newMd, line);
		getline(newMd, line);	//readRec
		getline(newMd, line);	//readpage
		getline(newMd, line);	//writepage

		char* pch;
		pch = strtok(&line[0], " ");
		//cout << pch << "  ";
		pch = strtok(NULL, " ");
		writePage = atoi(pch);	//convert char* to int
		//cout << writePage;
		remove(&newFileName[0]);

	}

}

int Sorted::binarySearch(int low, int high, Record literal, OrderMaker& query) {
	int mid = 0;
	while (low <= high) {
		mid = (low + high) / 2;
		cout << "low :" << low << " mid:" << mid << " high:" << high << endl;

		dbFile.GetPage(rwBuffer, mid);
		Record t1;
		rwBuffer->GetFirst(&t1);
		int val = cmp.Compare(&t1, &literal, &query);
		if (val == 0)
			return mid;
		else if (val < 0) {
			cout<<"here1"<<endl;
			if (mid + 1 <= high) {
				dbFile.GetPage(rwBuffer, mid + 1);
				Record t2;
				rwBuffer->GetFirst(&t2);
				int val = cmp.Compare(&t2, &literal, &query);
				if (val == 0)
					return mid + 1;
				if (val > 0)
					return mid;
			}
			low = mid + 1;
		} else {
			cout<<"here2"<<endl;
			if (mid - 1 >=low) {
				cout<<mid-1<<endl;
				cout<<"low "<<low<<endl;
				dbFile.GetPage(rwBuffer, mid - 1);
				Record t2;
				rwBuffer->GetFirst(&t2);
				int val = cmp.Compare(&t2, &literal, &query);
				if (val == 0)
					return mid - 1;
				if (val < 0)
					return mid-1;
			}
			high = mid - 1;

		}

	}

	Record temp;
	dbFile.GetPage(rwBuffer, mid);
	while (rwBuffer->GetFirst(&temp)) {
		if (!cmp.Compare(&temp, &literal, &query))
			return mid;
	}
	return -1;
}

Sorted::~Sorted() {
	delete rwBuffer;
}
