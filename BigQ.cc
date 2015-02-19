#include "BigQ.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

ComparisonEngine compEngine;
OrderMaker globalSortOrder;

/*
 * args strucutre to pass parameters to the thread;
 */
struct args{
	Pipe *in;
	Pipe *out;
	OrderMaker *sortorder;
	int runlen;
};
/*
 * work function for the worker thread
 */
void* work(void* arguments){
	struct args* para;					//args struc to receive arguments
	para=(struct args*)arguments;

	Pipe* in=para->in;					//pointer to the parameters , needed for tpmms algo
	Pipe* out=para->out;					//pointer to the parameters , needed for tpmms algo
	OrderMaker* sortorder=para->sortorder;					//pointer to the parameters , needed for tpmms algo
	int runlen=para->runlen;					//pointer to the parameters , needed for tpmms algo

while()



//	cout<<"runlength is"<<runlen<<endl<<endl;
//	Schema mySchema ("catalog", "nation");
//	Record temp;
//	in->Remove(&temp);
//
//	temp.Print(&mySchema);

}

/*
 * personal function to sort vector of records, needed for in memory sort.
 */
bool sortFunc(Record A,Record B)
{
if(compEngine.Compare(&A,&B,&globalSortOrder)==1)
	return false;
else
	return true;
}



BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {

	pthread_t worker;
	int check;
	struct args arguments;
	arguments.in=&in;
	arguments.out=&out;
	arguments.sortorder=&sortorder;
	arguments.runlen=runlen;
	globalSortOrder=sortorder;
	check=pthread_create(&worker,NULL,work,(void*)&arguments);
    if(check!=0)
    {
    	cout<<"error in thread creatig";
    	//exit(1);
    }
	// read data from in pipe sort them into runlen pages

    // construct priority queue over sorted runs and dump sorted data
 	// into the out pipe

    // finally shut down the out pipe
	pthread_join(worker,NULL);
	out.ShutDown ();
}

BigQ::~BigQ () {
}
