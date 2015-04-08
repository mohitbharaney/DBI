#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include <vector>
#include <algorithm>


using namespace std;

//ComparisonEngine comparisonEngine;

class BigQ {


public:

	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	Pipe* input;
	Pipe* output;
	OrderMaker* classSortOrder;

	int runlength;
	int id;
	void* work(void* arguments);
	bool sortFunc(Record* left, Record* right);
	void mergeRuns(int runLength, int totalrun, char *f_path, Pipe *outPipe);
	void writeToFile(vector<Record*> &data, int noOfRun, int runLength,File &phase1,int& offset);

	struct myClass{
		ComparisonEngine cmpEngine;
		OrderMaker* test;
		bool operator() (Record* left, Record* right){
			if (left == NULL && right == NULL)
					return true;
				if (left == NULL)
					return false;
				if (right == NULL)
					return true;

				if (cmpEngine.Compare(left, right, test) < 0)
					return true;

				if (cmpEngine.Compare(left, right, test) >= 0)
					return false;

				return true;

		}
	}myObject;
	//static int globalCount;
	~BigQ ();
};

#endif
