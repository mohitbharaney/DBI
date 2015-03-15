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
#include <iostream>
#include <fstream>
#include "Heap.h"
#include "GenericDB.h"
#include "Sorted.h"

// stub file .. replace it with your own DBFile.cc

GenericDB *genericFile;

DBFile::DBFile ()
{

}

/*
*check if file path is NULL, if yes return 0 otherwise
*create a new file and return 1
*/
int
DBFile::Create (char *f_path, fType f_type, void *startup)
{
		if(f_type==heap)
		{
			genericFile=new Heap();

		}
		else if(f_type==sorted)
		{
			genericFile=new Sorted();
		}

		if(genericFile!=NULL)
			return genericFile->Create(f_path,f_type,startup);

		else
		{
			cout<<"could not create the file";
					exit(1);
		}

    return 1;
}

/** Open the filepath provided by loadpath.
*while Loop through records in the file and load it into the temp record variable
* Keep appending it to a page and when the page is full write this page out 
* to the disk using an instance of file class. Clear the page buffer for further
* loads
*/
void
DBFile::Load (Schema & f_schema, char *loadpath)
{
	genericFile->Load(f_schema,loadpath);
}

/**
*open the DB file, success case returns 1 and failure case returned with 0
*/


int
DBFile::Open (char *f_path)
{

  if (f_path != NULL)
    {
    //  dbFile.Open (1, f_path);	//first parameter is non zero so that files not created again and is opened in read write mode.

	string fileName(f_path);
	fileName=fileName.substr(0,fileName.length()-3);
	fileName=fileName+"md";
//	metaDataFileName=&fileName[0];
//	metaDataFileName=
strcpy(&metaDataFileName[0],&fileName[0]);

      ifstream mdFile (metaDataFileName);
      string line;
      char *pch;

      if (mdFile.is_open ())
	{
	  /*
	   * read first line and tokenize it to store the value of read_page
	   */

      getline(mdFile,line);
      if(line=="heap")
    	  genericFile=new Heap();
      else if(line=="sorted")
      {
    	  genericFile=new Sorted();
      }
      	  mdFile.close ();
	 return genericFile->Open(f_path);
	}
      else
      {
    	  cout<<"check if file exists else u are screwed"<<endl;
    	  return 0;
      }

    }
  else
  {
	  cout<<"check file path"<<endl;
    return 0;
  }
}


/**
*Move the pointer to first record in file
*/
void
DBFile::MoveFirst ()
{
	genericFile->MoveFirst();
}

int
DBFile::Close ()
{

return genericFile->Close();
}

void
DBFile::Add (Record & rec)
{
genericFile->Add(rec);
}

int
DBFile::GetNext (Record & fetchme)
{
return genericFile->GetNext(fetchme);
}


int
DBFile::GetNext (Record & fetchme, CNF & cnf, Record & literal)
{
return genericFile->GetNext(fetchme,cnf,literal);
}


DBFile::~DBFile ()
{

}

