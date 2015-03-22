#include "RelOp.h"
 
void SelectPipe::Run(Pipe &inPipe,Pipe &outPipe,CNF &selOp, Record &literal)
{
inputPipe=&inPipe;
outputPipe=&outPipe;
selectOp=&selOp;
literalRecord=&literal;
pthread_create(&selectPipeThread, NULL, selectPipeMethod, (void *) this); // create a new thread and pass all the parameters to it
}

static void *selectPipeMethod(void *arg){
cout<<"selected PIPE method"<<endl;
int totalcount=0;
int totalselected=0;


ComparisonEngine cmp;
Record *tempRecord = new Record();
SelectPipe *selectPipeObj = (SelectPipe *)arg;


	while((selectPipeObj->inputPipe)->Remove(tempRecord))
	{
		totalcount++;
	if(cmp.Compare(tempRecord,selectPipeObj->literalRecord,selectPipeObj->selectOp))
		{
			(selectPipeObj->outputPipe)->Insert(tempRecord);
			totalselected++;
		}	
	}

cout<<"totalcount->"<<totalcount<<endl;
cout<<"total selected->"<<totalselected<<endl;
(selectPipeObj->outputPipe)->ShutDown();

}

void SelectPipe::WaitUntilDone () {
	  pthread_join (selectPipeThread, NULL);
}


//void SelectPipe::Use_n_Pages (int runlen) {

//}

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal)
{
 
dbInFile=&inFile;
outputPipe=&outPipe;
selectOp=&selOp;
literalRecord=&literal;
pthread_create(&selectFileThread, NULL, selectFileMethod, (void *) this); // create a new thread and pass all the parameters to it
}

static void *selectFileMethod(void *arg){

cout<<"inside selected File method:"<<endl;
int totalcount=0;
int totalselected=0;

ComparisonEngine cmp; 
SelectFile *selectFileObj = (SelectFile *)arg;
Record *tempRecord = new Record();

(selectFileObj->dbInFile)->MoveFirst();

while((selectFileObj->dbInFile)->GetNext(*tempRecord))
{   totalcount++;
	if(cmp.Compare(tempRecord,selectFileObj->literalRecord,selectFileObj->selectOp))
		{
			(selectFileObj->outputPipe)->Insert(tempRecord);
			totalselected++;
		}	
}

cout<<"totalcount->"<<totalcount<<endl;
cout<<"totalselected->"<<totalselected<<endl;
(selectFileObj->outputPipe)->ShutDown();

}

void SelectFile::WaitUntilDone () {
	  pthread_join (selectFileThread, NULL);
}


//void SelectFile::Use_n_Pages (int runlen) {

//}

	void Project::Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput)
{
inputPipe=&inPipe;
outputPipe=&outPipe;
keepMeProject=keepMe;
numAttsInputProject=numAttsInput;
numAttsOutputProject=numAttsOutput;

pthread_create(&projectThread, NULL, projectMethod, (void *) this); // create a new thread and pass all the parameters to it
}


static void *projectMethod(void *arg){
	
	cout<<"in project method"<<endl;
	int totalcount=0;
	

Project *selectPrjObj = (Project *)arg;
Record *tempRecord=new Record(); 

while((selectPrjObj->inputPipe)->Remove(tempRecord))
	{
	 	tempRecord->Project(selectPrjObj->keepMeProject,selectPrjObj->numAttsOutputProject,selectPrjObj->numAttsInputProject);
		(selectPrjObj->outputPipe)->Insert(tempRecord);
		totalcount++;
	}

cout<<"totalcount of output(projectMethod)::"<<totalcount<<endl;
 

(selectPrjObj->outputPipe)->ShutDown();
}


void Project::WaitUntilDone () {
	  pthread_join (projectThread, NULL);
}


//void Project::Use_n_Pages (int runlen) {

//}


void WriteOut::Run(Pipe &inPipe, FILE *outFile, Schema &mySchema)
{
inputPipe=&inPipe;
outFileWriteOut=outFile;
schemaWriteOut=&mySchema;
pthread_create(&writeOutThread, NULL, writeOutMethod,(void*) this);

}

void *writeOutMethod(void *arg){

WriteOut *writeObj=(WriteOut *)arg;
Record *tempRecord=new Record();

while((writeObj->inputPipe)->Remove(tempRecord))
		{
		tempRecord->WriteToFile(writeObj->outFileWriteOut,writeObj->schemaWriteOut);

		}
		
cout<<"write to file is done"<<endl;
}
void WriteOut::WaitUntilDone () {
	  pthread_join (writeOutThread, NULL);
}

void Join::Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal)
{

inputPipeL=&inPipeL;
inputPipeR=&inPipeR;
outputPipe=&outPipe;
selectOp=&selOp;
literalRecord=&literal;
 
pthread_create(&joinThread, NULL, joinMethod,(void*) this);

}
 
void *joinMethod(void *arg){

Join *joinObj=(Join *)arg;
Record *tempRecord=new Record();
// need to write logic here


}
void Join::WaitUntilDone () {
	  pthread_join (joinThread, NULL);
}

void Sum:: Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe)
{
	inputPipeSum=&inPipe;
	outPipeSum=&outPipe;
	computeMeSum=&computeMe;
	pthread_create(&sumThread, NULL, sumMethod,(void*) this);
	
}

void *sumMethod(void *arg)
{
	Sum *sumObj=(Sum *)arg;
	Record tempRecord,newRecord;
	int intResult=0;
	double doubleResult=0.0;
	double sumTotal=0.0;
	
	while((sumObj->inputPipeSum)->Remove(&tempRecord))
	{
		
		(sumObj->computeMeSum)->Apply(tempRecord,intResult,doubleResult);
		sumTotal=intResult+doubleResult;
		
		intResult=0; // reset the value
		doubleResult=0.0; //reset the value
	}
	Attribute newAttri={(char*)"sum",Double}; // create a new attribute of type double and name is sum
	
	Schema schema_totalSum("total_sum",1,&newAttri); // creat a in-memory schema
	char rec[50]; //assuming that sum won't be more than 48 digits
	 sprintf(rec, "%g|", sumTotal);    //%g    a floating-point number,
	//sprintf(rec,"%s",sumTotal);
	
	
	// neeed to create this function ComposeNewRecord in Record(pending)
	newRecord.ComposeNewRecord(&schema_totalSum, rec); //create new record;

	(sumObj->outPipeSum)->Insert(&newRecord); //insert the record
	(sumObj->outPipeSum)->ShutDown();
	
}
	
void Sum::WaitUntilDone () {
	  pthread_join (sumThread, NULL);
}
	
void DuplicateRemoval:: Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema)
{
	inputPipeDupliRemoval=&inPipe;
	outPipeDupliRemoval=&outPipe;
	schemaDupliRemoval=&mySchema;
	pthread_create(&duplicateRemovalThread, NULL, duplicateRemovalMethod,(void*) this);
	pthread_create(&bigQThread,NULL, createSortedDataBigQ,(void*) this);
	
}
void *duplicateRemovalMethod(void *args)
{
	
	
	DuplicateRemoval *dupliObj=(DuplicateRemoval *)args;
	OrderMaker sortedOrder=OrderMaker(dupliObj->schemaDupliRemoval);
	
	//BigQ *bigQ_obj;
	//Pipe temp(100); // created this pipe for storing the sorted output of bigQ
		
	//bigQ_obj=new BigQ(*(dupliObj->inputPipeDupliRemoval), temp,sortedOrder,dupliObj->dupliRunLength);
	
	// temp pipe contains the sorted data. we will compare its one record with next record
	
	Record *nextRecord=new Record();
	Record *currentRecord=new Record();
	ComparisonEngine cmpe;
	
	int recordDropped=0;
	int recordtaken=0;
	
	// inserting the first record in the outputpipe
	if((dupliObj->tempBigQ).Remove(nextRecord))
	{
		currentRecord->Copy(nextRecord);
		(dupliObj->outPipeDupliRemoval)->Insert(nextRecord);
		recordtaken++;
		
	}	
	//nextRecord will contain next record and currentRecord will contain record just one previous to nextRecord. Everytime we will copy the record
	while((dupliObj->tempBigQ).Remove(nextRecord))
	{
		if(cmpe.Compare(currentRecord,nextRecord,&sortedOrder)!=0) // two consequtive records are different
		{
		 currentRecord->Copy(nextRecord);  // copying as it will be used in next iteration
		(dupliObj->outPipeDupliRemoval)->Insert(nextRecord);
		recordtaken++;
		}
		else
			recordDropped++;
	 
	}
	
	cout<<"Duplic Removal class::record taken"<<recordtaken<<endl;
	cout<<"Dupli removal class::record dropped"<<recordDropped<<endl;
	
	(dupliObj->tempBigQ).ShutDown();
	(dupliObj->outPipeDupliRemoval)->ShutDown();
	
	
}	
	
 
void *createSortedDataBigQ(void *args)
{
		DuplicateRemoval *dupliObj=(DuplicateRemoval *)args;
		OrderMaker sortedOrder=OrderMaker(dupliObj->schemaDupliRemoval);
		dupliObj->bigQ_obj=new BigQ(*(dupliObj->inputPipeDupliRemoval), dupliObj->tempBigQ,sortedOrder,dupliObj->dupliRunLength);
		
}
	
		
void DuplicateRemoval::WaitUntilDone () {
	  pthread_join (duplicateRemovalThread, NULL);
	  pthread_join (bigQThread, NULL);
}
	
	
		
void DuplicateRemoval::Use_n_Pages (int n) {
   
   dupliRunLength=n;
   }
	
		
	
	
	


