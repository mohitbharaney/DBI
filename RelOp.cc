#include "RelOp.h"

void SelectPipe::Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
	inputPipe = &inPipe;
	outputPipe = &outPipe;
	selectOp = &selOp;
	literalRecord = &literal;
	pthread_create(&selectPipeThread, NULL, selectPipeMethod, (void *) this); // create a new thread and pass all the parameters to it
}

static void *selectPipeMethod(void *arg) {
	//cout << "selected PIPE method" << endl;
	int totalcount = 0;
	int totalselected = 0;

	ComparisonEngine cmp;
	Record *tempRecord = new Record();
	SelectPipe *selectPipeObj = (SelectPipe *) arg;
	while ((selectPipeObj->inputPipe)->Remove(tempRecord)) {
		totalcount++;
		if (cmp.Compare(tempRecord, selectPipeObj->literalRecord,
				selectPipeObj->selectOp)) {
			(selectPipeObj->outputPipe)->Insert(tempRecord);
			totalselected++;
		}
	}

//	cout << "totalcount->" << totalcount << endl;
//	cout << "total selected->" << totalselected << endl;
	(selectPipeObj->outputPipe)->ShutDown();

}

void SelectPipe::WaitUntilDone() {
	pthread_join(selectPipeThread, NULL);
}

//void SelectPipe::Use_n_Pages (int runlen) {

//}

void SelectFile::Run(DBFile &inFile, Pipe &outPipe, CNF &selOp,
		Record &literal) {

	dbInFile = &inFile;
	outputPipe = &outPipe;
	selectOp = &selOp;
	literalRecord = &literal;
	pthread_create(&selectFileThread, NULL, selectFileMethod, (void *) this); // create a new thread and pass all the parameters to it
}

//static
void *selectFileMethod(void *arg) {

	//cout << "inside selected File method:" << endl;
	int totalcount = 0;
	int totalselected = 0;

	ComparisonEngine cmp;
	SelectFile *selectFileObj = (SelectFile *) arg;
	Record *tempRecord = new Record();
	selectFileObj->dbInFile->MoveFirst();
	while ((selectFileObj->dbInFile)->GetNext(*tempRecord)) {
//	cout<<"in select file "<<
		totalcount++; //<<endl;
		if (cmp.Compare(tempRecord, selectFileObj->literalRecord,
				selectFileObj->selectOp)) {
			selectFileObj->outputPipe->Insert(tempRecord);
			totalselected++;
		}
	}

//	cout << "totalcount->" << totalcount << endl;
//	cout << "totalselected->" << totalselected << endl;
	(selectFileObj->outputPipe)->ShutDown();

}

void SelectFile::WaitUntilDone() {
	pthread_join(selectFileThread, NULL);
}

//void SelectFile::Use_n_Pages (int runlen) {

//}

void Project::Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput,
		int numAttsOutput) {
	inputPipe = &inPipe;
	outputPipe = &outPipe;
	keepMeProject = keepMe;
	numAttsInputProject = numAttsInput;
	numAttsOutputProject = numAttsOutput;

	pthread_create(&projectThread, NULL, projectMethod, (void *) this); // create a new thread and pass all the parameters to it
}

//static
void *projectMethod(void *arg) {

	cout << "in project method" << endl;
	int totalcount = 0;

	Project *selectPrjObj = (Project *) arg;
	Record *tempRecord = new Record();

	while ((selectPrjObj->inputPipe)->Remove(tempRecord)) {
		tempRecord->Project(selectPrjObj->keepMeProject,
				selectPrjObj->numAttsOutputProject,
				selectPrjObj->numAttsInputProject);
		(selectPrjObj->outputPipe)->Insert(tempRecord);
		totalcount++;
	}

	cout << "totalcount of output(projectMethod)::" << totalcount << endl;

	(selectPrjObj->outputPipe)->ShutDown();
}

void Project::WaitUntilDone() {
	pthread_join(projectThread, NULL);
}

//void Project::Use_n_Pages (int runlen) {

//}

void WriteOut::Run(Pipe &inPipe, FILE *outFile, Schema &mySchema) {
	inputPipe = &inPipe;
	outFileWriteOut = outFile;
	schemaWriteOut = &mySchema;
	pthread_create(&writeOutThread, NULL, writeOutMethod, (void*) this);

}

void *writeOutMethod(void *arg) {

	WriteOut *writeObj = (WriteOut *) arg;
	Record *tempRecord = new Record();

	while ((writeObj->inputPipe)->Remove(tempRecord)) {
		tempRecord->WriteToFile(writeObj->outFileWriteOut,
				writeObj->schemaWriteOut);

	}
}
void WriteOut::WaitUntilDone() {
	pthread_join(writeOutThread, NULL);
}

void Sum::Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe) {
	inputPipeSum = &inPipe;
	outPipeSum = &outPipe;
	computeMeSum = &computeMe;
	pthread_create(&sumThread, NULL, sumMethod, (void*) this);

}

void *sumMethod(void *arg) {
	Sum *sumObj = (Sum *) arg;
	Record tempRecord, newRecord;
	int intResult = 0;
	double doubleResult = 0.0;
	double sumTotal = 0.0;

	//cout<<"in sum"<<endl;
	while ((sumObj->inputPipeSum)->Remove(&tempRecord)) {

		(sumObj->computeMeSum)->Apply(tempRecord, intResult, doubleResult);
		sumTotal += intResult + doubleResult;

		intResult = 0; // reset the value
		doubleResult = 0.0; //reset the value
	}
	//cout<<"finished calculateing the sum"<<endl;
	Attribute newAttri = { (char*) "sum", Double }; // create a new attribute of type double and name is sum

	Schema schema_totalSum("total_sum", 1, &newAttri); // creat a in-memory schema
	char rec[50]; //assuming that sum won't be more than 48 digits
	sprintf(rec, "%g|", sumTotal);    //%g    a floating-point number,
	//sprintf(rec,"%s",sumTotal);

	// neeed to create this function ComposeNewRecord in Record(pending)
	newRecord.ComposeNewRecord(&schema_totalSum, rec); //create new record;

	(sumObj->outPipeSum)->Insert(&newRecord); //insert the record
	(sumObj->outPipeSum)->ShutDown();

}

void Sum::WaitUntilDone() {
	pthread_join(sumThread, NULL);
	//cout<<"exiting sum wait"<<endl;
}

void Join::Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp,
		Record &literal) {
	inputPipeL = &inPipeL;
	inputPipeR = &inPipeR;
	outputPipe = &outPipe;
	selectOp = &selOp;
	literalRecord = &literal;

	//cout << "in join method" << endl;
	if (pthread_create(&joinThread, NULL, joinMethod, (void*) this))
		cout << "error in join thread creation";

}
void Join::WaitUntilDone() {
	pthread_join(joinThread, NULL);
	//cout << "exiting wait until done" << endl;
}
void Join::Use_n_Pages(int n) {
	runLength = n;
}

void *joinMethod(void * arg) {
	Join *data = (Join*) arg;
	int flag = data->selectOp->GetSortOrders(data->leftSortOrder,
			data->rightSortOrder);
//	data->leftSortOrder.Print();
//	data->rightSortOrder.Print();
	pthread_t sortleft, sortright;
	if (flag) {
		/*
		 * if sort order exists then create two threads to sort the records in the input pipe
		 */
		data->sortedLeft = new Pipe(100);
		data->sortedRight = new Pipe(100);
		if (pthread_create(&sortleft, NULL, sortLeftMethod, arg))
			cout << "failed to create thread to sortleft";
		if (pthread_create(&sortright, NULL, sortRightMethod, arg))
			cout << "failed to create thread to sortRight";

		/*
		 * remove records from the input pipe and perform sort merge join
		 */
		Record leftRec, rightRec, joinRec;
		int lcnt = 0;
		int jcnt = 0;
		int rcnt = 0;
		int flagL = data->sortedLeft->Remove(&leftRec);
		int flagR = data->sortedRight->Remove(&rightRec);
		Schema sl("catalog", "supplier");
		Schema sr("catalog", "partsupp");
		int numOfAttsLeft = 0;
		int numOfAttsRight = 0;
		int attsToKeep[40];
		int numOfAttsToKeep = 0;
		int startOfRight = 0;

		if (flagL && flagR) {
			numOfAttsLeft=leftRec.numOfAttributeInRecord();
			numOfAttsRight=rightRec.numOfAttributeInRecord();
			numOfAttsToKeep=numOfAttsLeft+numOfAttsRight;
			//attsToKeep=new int[numOfAttsToKeep];
			for(int j=0;j<numOfAttsLeft;j++)
				attsToKeep[j]=j;
			for(int k=numOfAttsLeft;k<numOfAttsToKeep;k++)
				attsToKeep[k]=k-numOfAttsLeft;
//			cout << "left rec no of att:" << numOfAttsLeft << endl;
//			cout << "right rec no of att:" << rightRec.numOfAttributes()<< endl;
//
//			for(int i=0;i<numOfAttsToKeep;i++)
//			{
//				cout<<(int)attsToKeep[i]<<" ";
//			}
//			cout<<endl;
		}
		while (flagL && flagR) {
			int cmp = data->smjCmp.Compare(&leftRec, &(data->leftSortOrder),
					&rightRec, &(data->rightSortOrder));
			if (cmp == 0) {
				vector<Record*> leftBlock;
				vector<Record*> rightBlock;
				Record* leftCopy = new Record();
				Record* rightCopy = new Record();
				Record* leftNext = new Record();
				Record* rightNext = new Record();
				leftCopy->Copy(&leftRec);
				rightCopy->Copy(&rightRec);
				leftBlock.push_back(leftCopy);
				rightBlock.push_back(rightCopy);

				/*
				 * push all recs in the left pipe that are same to the current left rec into the vector.
				 */
				flagL = data->sortedLeft->Remove(leftNext);
				lcnt++;
				while (flagL
						&& !(data->smjCmp.Compare(&leftRec, leftNext,
								&data->leftSortOrder))) {
					leftCopy = new Record();
					leftCopy->Copy(leftNext);
					leftBlock.push_back(leftCopy);
					flagL = data->sortedLeft->Remove(leftNext);
					lcnt++;
				}
				Schema s1("catalog", "supplier");
				if (flagL) {
					leftRec.Copy(leftNext);
					//leftRec.Print(&s1);
				}
				/*
				 * push all recs in right pipe that are same to the current one into a vector.
				 */
				flagR = data->sortedRight->Remove(rightNext);
				rcnt++;
				while (flagR
						&& !(data->smjCmp.Compare(&rightRec, rightNext,
								&data->rightSortOrder))) {
					rightCopy = new Record();
					rightCopy->Copy(rightNext);
					rightBlock.push_back(rightCopy);
					flagR = data->sortedRight->Remove(rightNext);
					rcnt++;
				}
				if (flagR)
					rightRec.Copy(rightNext);

				/*
				 * merge recs in left and right and put them in output pipe
				 */
				for (int i = 0; i < leftBlock.size(); i++) {
					for (int j = 0; j < rightBlock.size(); j++) {
						joinRec.MergeRecords(&leftRec,&rightRec,numOfAttsLeft,numOfAttsRight,attsToKeep,numOfAttsToKeep,numOfAttsLeft);
						data->outputPipe->Insert(&joinRec);
						jcnt++;
					}
				}
				//cout << jcnt << endl;

				/*
				 * cleanUp
				 */
				for (int i = 0; i < leftBlock.size(); i++)
					delete leftBlock[i];
				for (int i = 0; i < rightBlock.size(); i++)
					delete rightBlock[i];

				leftBlock.clear();
				rightBlock.clear();

			} else if (cmp < 0) {
				flagL = data->sortedLeft->Remove(&leftRec);
				lcnt++;
			} else {
				flagR = data->sortedRight->Remove(&rightRec);
				rcnt++;
			}
		}
//
//		/*
//		 * empty the left pipe
//		 */
		while (flagL) {
//			leftRec.Print(&sl);
			flagL = data->sortedLeft->Remove(&leftRec);

			lcnt++;
		}
		data->sortedLeft->ShutDown();
//			c1++;
		/*
		 * empty the right pipe
		 */
		while (flagR) {
//				rightRec.Print(&sr);
			flagR = data->sortedRight->Remove(&rightRec);
			rcnt++;
		}
		data->sortedRight->ShutDown();
		data->outputPipe->ShutDown();
		/*
		 * wait for both the pipes to empty
		 *
		 */
		pthread_join(sortleft, NULL);
		pthread_join(sortright, NULL);

		//		pthread_join(data->sortLeft,NULL);
//		cout << "\n\nrecs joined=" << jcnt << endl << endl; //		pthread_join(data->sortRight,NULL);
//		cout << "\nleft pipe recs:" << lcnt << endl;
//		cout << "\right pipe recs:" << rcnt << endl;
//		cout << "exiting the join" << endl;
	}
}
void *sortLeftMethod(void* arg) {
	Join *data = (Join *) arg;

	BigQ(*(data->inputPipeL), *(data->sortedLeft), (data->leftSortOrder),
			data->runLength);
}

void *sortRightMethod(void* arg) {
	Join *data = (Join *) arg;

	BigQ(*(data->inputPipeR), *(data->sortedRight), data->rightSortOrder,
			data->runLength);
}

void DuplicateRemoval::Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {
	inputPipeDupliRemoval = &inPipe;
	outPipeDupliRemoval = &outPipe;
	schemaDupliRemoval = &mySchema;
	pthread_create(&duplicateRemovalThread, NULL, duplicateRemovalMethod,
			(void*) this);
	pthread_create(&bigQThread, NULL, createSortedDataBigQ, (void*) this);

}
void *duplicateRemovalMethod(void *args) {

	DuplicateRemoval *dupliObj = (DuplicateRemoval *) args;
	OrderMaker sortedOrder = OrderMaker(dupliObj->schemaDupliRemoval);

	//BigQ *bigQ_obj;
	//Pipe temp(100); // created this pipe for storing the sorted output of bigQ

	//bigQ_obj=new BigQ(*(dupliObj->inputPipeDupliRemoval), temp,sortedOrder,dupliObj->dupliRunLength);

	// temp pipe contains the sorted data. we will compare its one record with next record

	Record *nextRecord = new Record();
	Record *currentRecord = new Record();
	ComparisonEngine cmpe;

	int recordDropped = 0;
	int recordtaken = 0;

	// inserting the first record in the outputpipe
	if ((dupliObj->tempBigQ).Remove(nextRecord)) {
		currentRecord->Copy(nextRecord);
		(dupliObj->outPipeDupliRemoval)->Insert(nextRecord);
		recordtaken++;

	}
	//nextRecord will contain next record and currentRecord will contain record just one previous to nextRecord. Everytime we will copy the record
	while ((dupliObj->tempBigQ).Remove(nextRecord)) {
		if (cmpe.Compare(currentRecord, nextRecord, &sortedOrder) != 0) // two consequtive records are different
				{
			currentRecord->Copy(nextRecord); // copying as it will be used in next iteration
			(dupliObj->outPipeDupliRemoval)->Insert(nextRecord);
			recordtaken++;
		} else
			recordDropped++;

	}

	cout << "Duplic Removal class::record taken" << recordtaken << endl;
	cout << "Dupli removal class::record dropped" << recordDropped << endl;

	(dupliObj->tempBigQ).ShutDown();
	(dupliObj->outPipeDupliRemoval)->ShutDown();

}

void *createSortedDataBigQ(void *args) {

	DuplicateRemoval *dupliObj = (DuplicateRemoval *) args;
	OrderMaker sortedOrder = OrderMaker(dupliObj->schemaDupliRemoval);
	dupliObj->bigQ_obj = new BigQ(*(dupliObj->inputPipeDupliRemoval),
			dupliObj->tempBigQ, sortedOrder, dupliObj->dupliRunLength);

}

void DuplicateRemoval::WaitUntilDone() {
	pthread_join(duplicateRemovalThread, NULL);
	pthread_join(bigQThread, NULL);
}

void DuplicateRemoval::Use_n_Pages(int n) {
	dupliRunLength = n;
}



void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe)
{
	inputPipeGrpBy=&inPipe;
	outPipeGrpBy=&outPipe;
	ordermakerGrpBy=&groupAtts;
	computeMeGrpBy=&computeMe;
	pthread_create(&groupByThread, NULL, groupByMethod,(void*) this);
	pthread_create(&bigQThread,NULL, createSortedData4grpBy,(void*) this);
}
void *groupByMethod(void *args)
{
	GroupBy *grpByObj=(GroupBy *)args;	
		//grpByObj->bigQObj contains sorted data based on ordermake given
	double doubleResult;
	int intResult=0;
	double sum=0;;
	ComparisonEngine cme;
	Record currentRecord, nextRecord, tempRecord, newRecord;
	
	int numAttsRight, *attsToKeepRight,numAttsToKeep;
	
	attsToKeepRight=(grpByObj->ordermakerGrpBy)->attsList(); // attribute list to keep in record
	numAttsRight=(grpByObj->ordermakerGrpBy)->numOfAtts();   // number of attribute to be kept
	numAttsToKeep=numAttsRight+1; 
	// want to place attribute 1 to index 1 so increasing one size extra and in index 0 we will put zero
	
	int *AttsToKeep=new int[numAttsToKeep];
	AttsToKeep[0]=0;
	
	for(int j=1; j<numAttsToKeep; j++)
		AttsToKeep[j] = attsToKeepRight[j-1];
		
	// create a new attribute 
	Attribute as={(char*)"sum", Double}; // assume that type is double
	Schema 	schema_totalSum("totalSum",1,&as);
	
	int numOfAttrInRecord;
	
	if((grpByObj->tempBigQ_pipe).Remove(&currentRecord))
	{
		(grpByObj->computeMeGrpBy)->Apply(currentRecord, intResult, doubleResult);
		sum+=intResult+doubleResult;
		numOfAttrInRecord=currentRecord.numOfAttributeInRecord();
		intResult=0;
		doubleResult=0;		
		//sum=0;
		
	}	
	cout<<"no of attribute in the record"<<numOfAttrInRecord<<endl;
	
	char records[70];
	while((grpByObj->tempBigQ_pipe).Remove(&nextRecord))
	{
		if((cme.Compare(&currentRecord, &nextRecord, grpByObj->ordermakerGrpBy)) != 0){ 
			snprintf(records, 69, "%g|", sum); // write the sum in char recorda
			tempRecord.ComposeNewRecord(&schema_totalSum, records); //create new record;
			newRecord.MergeRecords(&tempRecord, &currentRecord, 1, numOfAttrInRecord, AttsToKeep, numAttsToKeep, 1); //merge the record 
			(grpByObj->outPipeGrpBy)->Insert(&currentRecord);
			currentRecord.Copy(&nextRecord); //copy the records for next iteration
			sum=0;
			
		}
		(grpByObj->computeMeGrpBy)->Apply(currentRecord, intResult, doubleResult);
		sum+=intResult+doubleResult;
		
		intResult=0;
		doubleResult=0; 

	}
		
		snprintf(records, 69, "%g|", sum); //write the sum into rec string;
		tempRecord.ComposeNewRecord(&schema_totalSum, records); //create new record;
		newRecord.MergeRecords(&tempRecord, &currentRecord, 1, numOfAttrInRecord, AttsToKeep, numAttsToKeep, 1); //merge the record 
		(grpByObj->outPipeGrpBy)->Insert(&currentRecord);
		(grpByObj->outPipeGrpBy)->ShutDown();
}
 
	

void *createSortedData4grpBy(void *args)
{
		
		GroupBy *grpByObj=(GroupBy *)args;		 
		grpByObj->bigQObj=new BigQ(*(grpByObj->inputPipeGrpBy), grpByObj->tempBigQ_pipe,*(grpByObj->ordermakerGrpBy),grpByObj->runLengthGrpBy);
		
}	

		
void GroupBy::WaitUntilDone () {
	  pthread_join (groupByThread, NULL);
	  pthread_join (bigQThread, NULL);
}
	
	
		
void GroupBy::Use_n_Pages (int n) {   
   runLengthGrpBy=n;
   }
	
	


