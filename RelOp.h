#ifndef REL_OP_H
#define REL_OP_H

#include <iostream>
#include <stdio.h>
#include "BigQ.h"
#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#include <pthread.h>
#include <vector>
#include "Schema.h"



static void *selectPipeMethod(void *);
 
void *selectFileMethod(void *);

void *projectMethod(void *);
static void *writeOutMethod(void *);
static void *joinMethod(void *);
static void *sumMethod(void *);

static void *groupByMethod(void *);
static void *createSortedData4grpBy(void *);


static void *duplicateRemovalMethod(void *);
void *sortLeftMethod(void* arg);
void *sortRightMethod(void* arg);
void *joinMethod(void* arg);

class RelationalOp {
	public:
	// blocks the caller until the particular relational operator 
	// has run to completion
	virtual void WaitUntilDone () = 0;

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages (int n) = 0;
};

class SelectFile : public RelationalOp { 

	private:
	pthread_t selectFileThread;	 

	public:
	DBFile *dbInFile;
	Pipe *outputPipe;
	Record *literalRecord;
	CNF *selectOp;

	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n) {}

};


class SelectPipe : public RelationalOp {
	
	private:	
	pthread_t selectPipeThread;

	public:
	Pipe *inputPipe, *outputPipe;
	Record *literalRecord;
	CNF *selectOp;
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n) { }
};

class Project : public RelationalOp { 
	private:
	pthread_t projectThread;

	public:
	Pipe *inputPipe, *outputPipe;
	int numAttsInputProject;
	int numAttsOutputProject;
	int *keepMeProject;

	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
	void WaitUntilDone () ;
	void Use_n_Pages (int n) { }
};
class Join : public RelationalOp { 
	private:
	pthread_t joinThread;

	public:

	pthread_t sortLeft;
	pthread_t sortRight;
	int pipeSiez = 100; // buffer sz allowed for each pipe
	Pipe *inputPipeL,*inputPipeR, *outputPipe;
	Pipe *sortedLeft;
	Pipe *sortedRight;
	OrderMaker leftSortOrder,rightSortOrder;
	//pipe to be used in join after sorting the data out.
	Record *literalRecord;
	CNF *selectOp;
	int runLength;
	ComparisonEngine smjCmp;
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);// { }
};
class DuplicateRemoval : public RelationalOp {

       private:
       pthread_t duplicateRemovalThread;
       pthread_t bigQThread;

       public:


       Pipe *inputPipeDupliRemoval;
       Pipe *outPipeDupliRemoval;
       Schema *schemaDupliRemoval;

       int dupliRunLength;

       BigQ *bigQ_obj;
       Pipe tempBigQ;
       DuplicateRemoval()
       {
       //tempBigQ=Pipe(100);
       Pipe(100);
       }

       void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
       void WaitUntilDone ();
       void Use_n_Pages (int n);
       //~DuplicateRemoval() {}
};
static void *duplicateRemovalMethod(void *);
static void *createSortedDataBigQ(void *);

class Sum : public RelationalOp {
	private:
	pthread_t sumThread;
	
	public:
	Pipe *inputPipeSum;
	Pipe *outPipeSum;
	Function *computeMeSum;
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n) { }
};
class GroupBy : public RelationalOp {
	
	private:
	pthread_t groupByThread;
	pthread_t bigQThread;
	
	public:
	Pipe *inputPipeGrpBy;
	Pipe *outPipeGrpBy;
	OrderMaker *ordermakerGrpBy;
	Function *computeMeGrpBy;
		
	BigQ *bigQObj;
	Pipe tempBigQ_pipe;
	
	int runLengthGrpBy;
	
	GroupBy()
	{	
	//tempBigQ=Pipe(100);
	Pipe(100);
	}
	
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};


class WriteOut : public RelationalOp {

   private:
	pthread_t writeOutThread;

	public:
	Pipe *inputPipe;
	Schema *schemaWriteOut;
	FILE *outFileWriteOut;

	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n) { }
};
#endif
