#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <string.h>
#include <stdlib.h>





// stub file .. replace it with your own DBFile.cc

DBFile::DBFile () {
writeBuffer=new Page();
readBuffer=new Page();
}

/*
 * create the db file and the metadata file
 */ 
int DBFile::Create (char *f_path, fType f_type, void *startup) {
	file_type=f_type;
	dbFile.Open(0,f_path);							
	dbFile.Close();
}

void DBFile::Load (Schema &f_schema, char *loadpath) {
	 FILE *tableFile = fopen (loadpath, "r");
	 
        Record temp;
        while (temp.SuckNextRecord (&f_schema, tableFile) == 1) {
			//cout<<"in while"<<endl;
			//temp.Print(&f_schema);
			
			//cout<<"appends result "<<a<<endl;
			if(!writeBuffer->Append(&temp))
			{
			
				dbFile.AddPage(writeBuffer,writePage);
				writePage++;
				writeBuffer->EmptyItOut();
				writeBuffer->Append(&temp);
			}
			
		}
		fclose(tableFile);
       // cout<<"total no of pages:"<<dbFile.GetLength();
}
/*
 *open the already created file and and set the metadata pointers of the file. 
 */
int DBFile::Open (char *f_path) {
	dbFile.Open(1,f_path);				//first parameter is non zero so that files not created again and is opened in read write mode.

	ifstream mdFile(metaDataFileName);
	string line;
	char* pch;
	
	 if (mdFile.is_open())
  {
	/*
	 * read first line and tokenize it to store the value of read_page
	 */ 
    getline (mdFile,line);
	pch = strtok (&line[0]," ");
	pch = strtok (NULL," ");
    readPage=atoi(pch);		//convert char* to int
    /*
	 * read first line and tokenize it to store the value of read_record
	 */ 
    getline (mdFile,line);
	pch = strtok (&line[0]," ");
	pch = strtok (NULL," ");
    readRecord=atoi(pch);		//convert char* to int
    /*
	 * read first line and tokenize it to store the value of write_page
	 */ 
    getline (mdFile,line);
	pch = strtok (&line[0]," ");
	pch = strtok (NULL," ");
    writePage=atoi(pch);		//convert char* to int
    
    cout<<readPage<<endl<<readRecord<<endl<<writePage;
    mdFile.close();
  }

}

void DBFile::MoveFirst () {
	readPage=0;
	readRecord=0;
}

int DBFile::Close () {
	dbFile.Close();
	ofstream mdFile;
	mdFile.open(metaDataFileName);
	mdFile<<"readPage "<<readPage<<endl;
	mdFile<<"readRecord "<<readRecord<<endl;
	mdFile<<"writePage "<<writePage<<endl;
	mdFile.close(); 
	
}

void DBFile::Add (Record &rec) {
	Record temp=rec;
	if(!writeBuffer->Append(&rec))
			{
			
				dbFile.AddPage(writeBuffer,writePage);
				writePage++;
				writeBuffer->EmptyItOut();
				writeBuffer->Append(&rec);
			}
			if(readPage==writePage)
				readBuffer->Append(&temp);
}

int DBFile::GetNext (Record &fetchme) {

	dbFile.GetPage(readBuffer,0);
	readBuffer->GetFirst(&fetchme);
	
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
}

DBFile::~DBFile () {
delete writeBuffer;
delete readBuffer; 	
}
