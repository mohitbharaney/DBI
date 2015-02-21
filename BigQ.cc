#include "BigQ.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include <algorithm>
ComparisonEngine compEngine;
OrderMaker globalSortOrder;

void writeToFile(vector<Record*> &data, int noOfRun, int runLength, File &phase1);

bool sortFunc(Record* left, Record* right) {
	cout<<"comparison value is "<<compEngine.Compare(left, right, &globalSortOrder)<<endl;
	if (compEngine.Compare(left, right, &globalSortOrder) < 0)
		return true;

	if (compEngine.Compare(left, right, &globalSortOrder) >= 0)
			return false;


	return true;
}

/*
 * args strucutre to pass parameters to the thread;
 */
struct args {
	Pipe *in;
	Pipe *out;
	OrderMaker *sortorder;
	int runlen;
};
/*
 * work function for the worker thread
 */
void* work(void* arguments) {
	struct args* para;					//args struc to receive arguments
	para = (struct args*) arguments;

	Pipe* in = para->in;	//pointer to the parameters , needed for tpmms algo
	Pipe* out = para->out;	//pointer to the parameters , needed for tpmms algo
	OrderMaker* sortorder = para->sortorder;//pointer to the parameters , needed for tpmms algo
	int runlen = para->runlen;//pointer to the parameters , needed for tpmms algo

	int count = 0;
	Record* temp=new Record();
	Record* copyRec;
	Page* input = new Page();
	Page* output = new Page();
	vector<Record*> toSort;
	File phase1;
	phase1.Open(0, "phase1.bin");
	phase1.AddPage(input,0);
	int noOfRuns = 0;
//	bool flag;
	while (in->Remove(temp)) {									//Retrieve the record in temp from the input pipe

		if (!input->Append(temp)) {							//add to input buffer, if fails go into if
			count++;											//increases the pageCount
			Record* t1=new Record();
			//Record t1;
			while (input->GetFirst(t1))						//dump the page to vector
			{
				copyRec=new Record();
				copyRec->Copy(t1);
				toSort.push_back(copyRec);

			}

			if (count == runlen) {
				writeToFile(toSort,noOfRuns,runlen,phase1);
				count = 0;
				noOfRuns++;
			}
			input->Append(temp);
		}
//		cout<<"critical test here"<<endl;
//		Record t1;
//		input->GetFirst(&t1);
//		toSort.push_back(t1);
//
	}

//	cout<<"/n vecote size is "<<toSort.size()<<endl;
	Record *t1=new Record();
	Record *t2;
	Schema mySchema ("catalog", "nation");//temp record again
	while (input->GetFirst(t1))
	{
		t2=new Record();
		t2->Copy(t1);
		//t1->Print(&mySchema);
			toSort.push_back(t2);

	}
	cout<<"/n vecote size is "<<toSort.size()<<endl;
	if(!toSort.empty())
	{
		cout<<"going in for the kill"<<endl;
		writeToFile(toSort,noOfRuns,runlen,phase1);
		count = 0;
		noOfRuns++;
	}

///*
// *
// * test case to check if sort works
// *
// */
//
	Page *test=new Page();
	phase1.GetPage(test,1);
	Record t;
	//Schema mySchema ("catalog", "nation");
	while(test->GetFirst(&t))
		out->Insert(&t);

	cout<<"runlength is"<<runlen<<endl<<endl;

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
	struct args arguments;
	arguments.in = &in;
	arguments.out = &out;
	arguments.sortorder = &sortorder;
	arguments.runlen = runlen;
	globalSortOrder = sortorder;
	check = pthread_create(&worker, NULL, work, (void*) &arguments);
	if (check != 0) {
		cout << "error in thread creatig";
		//exit(1);
	}
	// read data from in pipe sort them into runlen pages

	// construct priority queue over sorted runs and dump sorted data
	// into the out pipe

	// finally shut down the out pipe
	pthread_join(worker, NULL);
	out.ShutDown();
}

BigQ::~BigQ() {
}

void writeToFile(vector<Record*> &data, int noOfRun, int runLength, File &phase1) {
	Schema mySchema ("catalog", "nation");
	cout<<"i think i know whats fucking things up"<<endl;

	data[0]->Print(&mySchema);
	cout<<endl;
	data[1]->Print(&mySchema);
	cout<<endl;
	data[24]->Print(&mySchema);
	cout<<endl;



	sort(data.begin(), data.begin()+data.size(), sortFunc);					//sort the vector based on the custom function
	cout<<"sort successful fuck yeah"<<endl;
	Record *temp=new Record();																//temp rec
	Page* output = new Page();													//output buffer
	bool flag;																	//flag to check after the while loop,if the entire vecotrs written to the temp file
	int outCount = 1;															//output page count
	int count=0;
	while (count<data.size()) {													//run the loop till the entire vectors been traversed
		flag = true;															//set the flag true in the loop
		temp ->Copy(data[count]);		//retrive the record in temp
		temp->Print(&mySchema);
		cout<<endl;
		if (!output->Append(temp)) {											//if the output buffer is full
			int offset = noOfRun * runLength + outCount;						//calculate the offset at which the record is to be put
			phase1.AddPage(output, offset);										//add page to the file
			output->EmptyItOut();												//empty the buffer
			output->Append(temp);												//add the last record that triggered the if condition to the output buffer
			outCount++;															//ouput page count goes up by 1

			flag = false;														//set flag to false as the entire pages is written
		}
		count++;																//increment the count in vector traversal
	}
	if (flag) {																	//if the while loop exits with flag=true it implies that the vector was not of exact page size, henc the last page still needs to be written to the file
		int offset = noOfRun * runLength + outCount;							//following code is to handle the above case
		phase1.AddPage(output, offset);
		output->EmptyItOut();
	}
	data.clear();				//empty out the vector.
	delete output;
	//delete temp;
}
