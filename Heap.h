#ifndef HEAP_H
#define HEAP_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "GenericDB.h"

//typedef enum {heap, sorted, tree} fType;

// stub DBFile header..replace it with your own DBFile.h 

class Heap : public  GenericDB{

	int readPage,writePage,readRecord;
	int flag;
	Page *rwBuffer;
	File dbFile;
	char metaDataFileName[40];

public:
	Heap ();

	int Create (char *fpath, fType file_type, void *startup);
	int Open (char *fpath);
	int Close ();

	void Load (Schema &myschema, char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
	~Heap();

};
#endif
