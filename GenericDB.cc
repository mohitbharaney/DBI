#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "GenericDB.h"
#include "Defs.h"

// stub file .. replace it with your own DBFile.cc

GenericDB::GenericDB () {

}

int GenericDB::Create (char *fpath, fType file_type, void *startup){

}


void GenericDB::Load (Schema &f_schema, char *loadpath) {

}

int GenericDB::Open (char *f_path) {
}

void GenericDB::MoveFirst () {
}

int GenericDB::Close () {
}

void GenericDB::Add (Record &rec) {
}

int GenericDB::GetNext (Record &fetchme) {
	return 0;
}

int GenericDB::GetNext (Record &fetchme, CNF &cnf, Record &literal) {

}

GenericDB::~GenericDB(){
}
