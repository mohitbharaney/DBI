#ifndef SORTED_H
#define SORTED_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "GenericDB.h"

class Sorted: public GenericDB {
	int readPage, writePage, readRecord;
	int flag;
	Page *rwBuffer;
	File dbFile;
	char metaDataFileName[40];

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
	~Sorted();
};

#endif /* SORTED_H_ */
