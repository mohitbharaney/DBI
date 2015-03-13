#ifndef SORTED_H
#define SORTED_H


#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "GenericDB.h"
#include "Pipe.h"

#define READ 0
#define WRITE 1
#define START -1

struct SortInfo {
OrderMaker *myOrder;
int runLength;
};



class Sorted: public GenericDB {
	int readPage, writePage, readRecord;
	int mode;
	Page *rwBuffer;
	File dbFile;



public:
	Sorted();
	int Create(char *fpath, fType file_type, void *startup);
	int Open(char *fpath);
	int Close();
	void Load(Schema &myschema, char *loadpath);
	void MoveFirst();
	void Add(Record &addme);
	int GetNext(Record &fetchme);
	int GetNext(Record &fetchme, CNF &cnf, Record &literal);
	void merge();
	int binarySearch(int low, int high, Record literal, OrderMaker& query);
	~Sorted();
};

#endif /* SORTED_H_ */
