#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include <iostream>
#include <fstream>

typedef enum {heap, sorted, tree} fType;

// stub DBFile header..replace it with your own DBFile.h 

class DBFile {
File dbFile;				//file descriptor for file on disk
Page *writeBuffer;				//declare read write buffer
Page *readBuffer;
int writePage,readPage,readRecord;	//metadata of file to keep track of the file.
fType file_type;						//to be used later
char* metaDataFileName="metaData.txt";
		
public:
	DBFile (); 
	~DBFile();
	int Create (char *fpath, fType file_type, void *startup);
	int Open (char *fpath);
	int Close ();

	void Load (Schema &myschema, char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};
#endif
