#include "BigQ.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include <algorithm>
#include "DBFile.h"
#include <time.h>
#include<sstream>
//ComparisonEngine compEngine;
//OrderMaker globalSortOrder;

//void writeToFile(vector<Record*> &data, int noOfRun, int runLength,
//		File &phase1,int& offset);
//void mergeRuns(int runLength, int totalrun, char *f_path, Pipe *outPipe);

//bool BigQ::sortFunc(Record* left, Record* right) {
//	//cout<<"comparison value is "<<compEngine.Compare(left, right, &globalSortOrder)<<endl;
//	if (left == NULL && right == NULL)
//		return true;
//	if (left == NULL)
//		return false;
//	if (right == NULL)
//		return true;
//
//	if (cmpEngine.Compare(left, right, classSortOrder) < 0)
//		return true;
//
//	if (cmpEngine.Compare(left, right, classSortOrder) >= 0)
//		return false;
//
//	return true;
//}

/*
 * args strucutre to pass parameters to the thread;
 */

/*
 * work function for the worker thread
 */

void* workerThread(void* arguments)
{
	BigQ* data=(BigQ*)arguments;
	data->work(arguments);

}

void* BigQ::work(void* arguments) {

	//cout<<"in work"<<endl;
	//struct args* para;					//args struc to receive arguments
	BigQ* para = (BigQ*) arguments;

	Pipe* in = para->input;	//pointer to the parameters , needed for tpmms algo
	Pipe* out = para->output;	//pointer to the parameters , needed for tpmms algo
	OrderMaker* sortorder = para->classSortOrder;//pointer to the parameters , needed for tpmms algo
	int runlen = para->runlength;//pointer to the parameters , needed for tpmms algo

	int count = 0;
	Record* temp = new Record();
	Record* copyRec;
	Page* input = new Page();
	Page* output = new Page();
	vector<Record*> toSort;
	File phase1;

//long double sysTime=time(0);

	stringstream ss;
	ss<<"phase"<<para->id<<".bin";
	string runFile=ss.str();
	//cout<<endl<<runFile<<endl;
	phase1.Open(0, (char*)runFile.c_str());
	phase1.AddPage(input, 0);
	int noOfRuns = 0;
	int recs=0;
	int offset=1;
//	bool flag;
	while (in->Remove(temp)) {//Retrieve the record in temp from the input pipe

		if (!input->Append(temp)) {	//add to input buffer, if fails go into if
			count++;								//increases the pageCount
			Record* t1 = new Record();
			while (input->GetFirst(t1))				//dump the page to vector
			{
				copyRec = new Record();
				copyRec->Copy(t1);
				toSort.push_back(copyRec);
				recs++;
			}

			if (count == runlen) {
				writeToFile(toSort, noOfRuns, runlen, phase1,offset);
				count = 0;
				noOfRuns++;
			}
			input->Append(temp);
		}

	}

	Record *t1 = new Record();
	Record *t2;
	Schema mySchema("catalog", "nation");					//temp record again
	while (input->GetFirst(t1)) {
		t2 = new Record();
		t2->Copy(t1);
		//t1->Print(&mySchema);
		toSort.push_back(t2);
		recs++;
	}

	if (!toSort.empty()) {
		//	cout<<"going in for the kill"<<endl;
		writeToFile(toSort, noOfRuns, runlen, phase1,offset);
		count = 0;
		noOfRuns++;
	}

		//cout << "total no of records are " << recs << endl<<endl;
	phase1.Close();
	mergeRuns(runlen, noOfRuns, (char*)runFile.c_str(), out);

///*
// *
// * test case to check if sort works
// *
// */
//
	/*	Page *test = new Page();
	 phase1.GetPage(test, 1);
	 Record t;
	 //Schema mySchema ("catalog", "nation");
	 while (test->GetFirst(&t))
	 out->Insert(&t); */

	//cout<<"runlength is"<<runlen<<endl<<endl;
//	Record temp;
//	in->Remove(&temp);
//	Schema mySchema ("catalog", "nation");
//	temp.Print(&mySchema);
}

/*
 * personal function to sort vector of records, needed for in memory sort.
 */

BigQ::BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {

	pthread_t worker;
	int check;
	input=&in;
	output=&out;
	classSortOrder=&sortorder;
	runlength=runlen;
//	struct args arguments;
//	arguments.in = &in;
//	arguments.out = &out;
//	arguments.sortorder = &sortorder;
//	arguments.runlen = runlen;
	static int globalCount=0;
	id=globalCount++;
	classSortOrder= &sortorder;
	myObject.test=&sortorder;
	check = pthread_create(&worker, NULL, workerThread, (void*) this);
	if (check != 0) {
		cout << "error in thread creatig";
		//exit(1);
	}
	// read data from in pipe sort them into runlen pages

	// construct priority queue over sorted runs and dump sorted data
	// into the out pipe

	// finally shut down the out pipe
	pthread_join(worker, NULL);
//	out.ShutDown();
}

BigQ::~BigQ() {

}

void BigQ::writeToFile(vector<Record*> &data, int noOfRun, int runLength,
		File &phase1,int& offset) {
//	Schema mySchema("catalog", "nation");

	//sort(data.begin(), data.begin() + data.size(),sortFunc);//sort the vector based on the custom function
sort(data.begin(),data.begin()+data.size(),myObject);
	Record *temp = new Record();									//temp rec
	Page* output = new Page();									//output buffer
	bool flag;//flag to check after the while loop,if the entire vecotrs written to the temp file
	int outCount = 1;										//output page count
	int count = 0;
	while (count < data.size()) {//run the loop till the entire vectors been traversed
		flag = true;							//set the flag true in the loop
		temp->Copy(data[count]);		//retrive the record in temp
		if (!output->Append(temp)) {			//if the output buffer is full
	//		int offset = noOfRun * runLength + outCount;//calculate the offset at which the record is to be put
//			offset++;
			//cout<<"offset="<<offset<<endl;
			phase1.AddPage(output, offset);				//add page to the file
			 offset++;
			output->EmptyItOut();							//empty the buffer
			output->Append(temp);//add the last record that triggered the if condition to the output buffer
			//outCount++;							//ouput page count goes up by 1
			
			flag = true;	//set flag to false as the entire pages is written
		}
		count++;					//increment the count in vector traversal
	}
	if (flag) {	//if the while loop exits with flag=true it implies that the vector was not of exact page size, henc the last page still needs to be written to the file

	//	int offset = noOfRun * runLength + outCount;//following code is to handle the above case
	//	offset++;
		//cout<<"in if offset="<<offset<<endl;
		phase1.AddPage(output, offset);
		output->EmptyItOut();
 offset++;
	}
	for(int i=0;i<data.size();i++)
		delete data[i];

	data.clear();				//empty out the vector.
	delete output;
	//delete temp;
}

void BigQ::mergeRuns(int runLength, int totalrun, char *f_path, Pipe *outPipe) {

	File tempFile;
	tempFile.Open(1, f_path);
	vector<Page *> pageVector; // it will store the first page
	vector<Record *> recordVector; // it will store the 1st record of 1st page
	//cout << "total run" << totalrun << endl;
	int fileLegth = tempFile.GetLength() - 1;
	//cout << "file length" << fileLegth << endl;


/*

test code

*/


/*

	DBFile t;
	t.Open("phase1.bin");
	t.MoveFirst();
	Record x;
	int tcount=0;
	while(t.GetNext(x)==1)
		tcount++;
	cout<<"this is to find the fucker <<"<<tcount<<endl<<endl;


*/







	if (totalrun == 1) {

		//cout<<"file length"<<fileLegth<<endl;

		for (int i = 1; i < fileLegth; i++) {
			Page *page = new Page();
		//	cout << "value of i ==" << i << endl;
			tempFile.GetPage(page, i);
			Record *tempRecord = new Record();

			while (page->GetFirst(tempRecord)) {
				outPipe->Insert(tempRecord);
			}
			delete tempRecord;
			delete page;
		}
	} else {
		Page *newPage;
		int indiPageCount[totalrun];
		for (int k = 0; k < totalrun; k++) {
			indiPageCount[k] = 1; // individual page number for each run
		}
		for (int j = 1; j < fileLegth;) /// check fileLength while testing
				{
			newPage = new Page();
			tempFile.GetPage(newPage, j);
			pageVector.push_back(newPage);
			j = j + runLength;

		}
		Record *temprecord = new Record();
		Record *copyrecord;

		for (int j = 0; j < pageVector.size(); j++) {
			copyrecord = new Record();
			pageVector[j]->GetFirst(temprecord);
			copyrecord->Copy(temprecord);
			recordVector.push_back(copyrecord);

		}
		int nullCounter = 0;
		Record *tmpRecord = new Record();
		Record *copyRecord;
		while (true) {
			int minIndex = min_element(recordVector.begin(), recordVector.end(),
					myObject) - recordVector.begin();
			if(recordVector[minIndex]==NULL)
			{
				//cout<<"caught u mother fucker"<<endl;
				break;
			}
			outPipe->Insert(recordVector[minIndex]);
			delete recordVector[minIndex];
			copyRecord = new Record();
			if (!pageVector[minIndex]->GetFirst(tmpRecord)) {
				int countCheck = 0;
				if (minIndex == totalrun - 1) {

					countCheck = fileLegth-((minIndex * runLength) + 1);
				} else {
					countCheck = runLength;
				}
				indiPageCount[minIndex] += 1;
				//cout<<"countcheck"<< countCheck<<"min index"<<minIndex<<endl;
				//cout<<"run length"<<runLength<<endl;

				//cout<<"indiPageCount["<<minIndex<<"]="<<indiPageCount[minIndex]<<endl;
				if (indiPageCount[minIndex] <= countCheck) {
					int offset = (minIndex * runLength) + indiPageCount[minIndex];
				//cout<<"offset value"<<offset<<"indiPageCount["<<minIndex<<"]"<<indiPageCount[minIndex]<<endl;
					tempFile.GetPage(pageVector[minIndex], offset);
					pageVector[minIndex]->GetFirst(tmpRecord);
					copyRecord->Copy(tmpRecord);
					recordVector[minIndex] = copyRecord;
				}
				else
				{
				//	cout<<"in else"<<endl;
					recordVector[minIndex]=NULL;
					nullCounter++;
					//cout<<"null counter "<<nullCounter<<endl<<endl;
				}

			} else {
				copyRecord->Copy(tmpRecord);
				recordVector[minIndex] = copyRecord;
			}

			if (nullCounter == totalrun)
				break;
		}

	}

for(int i=0;i<pageVector.size();i++)
		delete pageVector[i];

outPipe->ShutDown();
tempFile.Close();
remove(f_path);
}
