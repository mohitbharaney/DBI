#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

#include "Defs.h"
#include <string.h>
#include <stdlib.h>
#include <iostream>
#include <fstream>
#include "Heap.h"


// stub file .. replace it with your own DBFile.cc
Heap::Heap ()
{
  //metaDataFileName = "metaData.txt";
  flag = -1;
  rwBuffer = new Page ();
  readRecord=0;
  writePage=0;
  readPage=0;
}

/*
*check if file path is NULL, if yes return 0 otherwise
*create a new file and return 1
*/
int
Heap::Create (char *f_path, fType f_type, void *startup)
{
  if (f_path != NULL)
    {


	string fileName(f_path);
	fileName=fileName.substr(0,fileName.length()-3);
	fileName=fileName+"md";

strcpy(&metaDataFileName[0],&fileName[0]);


/*
extract file name and convert it to fileName.md for the metaData file

*/


	 ofstream mdFile;
  mdFile.open (metaDataFileName);
  mdFile << "heap"<<endl;
  mdFile << "readPage " << 1 << endl;
  mdFile << "readRecord " << 0 << endl;
  mdFile << "writePage " << 1 << endl;
  mdFile.close ();

/*
re initialize the md file on create
*/



      dbFile.Open (0, f_path);
      dbFile.AddPage (rwBuffer, 0);
      dbFile.Close ();
      Open (f_path);
      return 1;
    }
  else
    return 0;
}

/** Open the filepath provided by loadpath.
*while Loop through records in the file and load it into the temp record variable
* Keep appending it to a page and when the page is full write this page out 
* to the disk using an instance of file class. Clear the page buffer for further
* loads
*/
void
Heap::Load (Schema & f_schema, char *loadpath)
{

//  cout << loadpath << endl;
  FILE *tableFile = fopen (loadpath, "r");
  int count = 0;
  Record temp;

//cout<<"write page in load -------------------------- "<<writePage<<endl;
  if (tableFile != NULL)
    {
      while (temp.SuckNextRecord (&f_schema, tableFile) == 1)
	{


	  //cout<<"inside while"<<endl;
/*	  if (!rwBuffer->Append (&temp))
	    {
	      dbFile.AddPage (rwBuffer, writePage);
	      writePage++;
	      rwBuffer->EmptyItOut ();
	      rwBuffer->Append (&temp);
	      cout << writePage << endl;
	    }
	  count++;
*/


		Add(temp);
	}
      dbFile.AddPage (rwBuffer, writePage);
      flag = 1;
//cout<<"write page in load -------------------------- "<<writePage<<endl;
      fclose (tableFile);
    }
  else
    cout << "check yout loadPath" << endl;
}

/**
*open the DB file, success case returns 1 and failure case returned with 0
*/


int
Heap::Open (char *f_path)
{
	//cout<<"in open heap"<<endl;

  if (f_path != NULL)
    {
      dbFile.Open (1, f_path);	//first parameter is non zero so that files not created again and is opened in read write mode.

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
      getline (mdFile,line);
	  getline (mdFile, line);
	  pch = strtok (&line[0], " ");
	  pch = strtok (NULL, " ");
	  readPage = atoi (pch);	//convert char* to int
	  /*
	   * read first line and tokenize it to store the value of read_record
	   */
	  getline (mdFile, line);
	  pch = strtok (&line[0], " ");
	  pch = strtok (NULL, " ");
	  readRecord = atoi (pch);	//convert char* to int
	  /*
	   * read first line and tokenize it to store the value of write_page
	   */
	  getline (mdFile, line);
	  pch = strtok (&line[0], " ");
	  pch = strtok (NULL, " ");
	  writePage = atoi (pch);	//convert char* to int

//cout<<"writePage "<<writePage<<endl;

	 // cout<<"exiting open"<<endl;
	  mdFile.close ();
	 
	}
 return 1;
    }
  else
    return 0;
}


/**
*Move the pointer to first record in file
*/
void
Heap::MoveFirst ()
{
  readPage = 1;
  readRecord = 0;
 // cout<<"exiting move first"<<endl;
}

int
Heap::Close ()
{


  //Appending the write buffer to end of file to add remaining elements in the buffer
  if (flag == 1)
    {
      dbFile.AddPage (rwBuffer, writePage);
    }

  dbFile.Close ();
  ofstream mdFile;
//cout<<"in close function"<<endl;
//cout<<metaDataFileName<<endl;
  mdFile.open (metaDataFileName);
  //cout<<"readPage: "<<readPage<<endl;
  mdFile << "heap" <<endl;
  mdFile << "readPage " << readPage << endl;
  //cout<<"readRecord: "<<readRecord<<endl;
  mdFile << "readRecord " << readRecord << endl;
  //cout<<"writePage: "<<writePage<<endl;
  mdFile << "writePage " << writePage << endl;
  mdFile.close ();
  return 1;
}

void
Heap::Add (Record & rec)
{
  //cout<<"in 1"<<endl;
  //rwBuffer->EmptyItOut();
  if (flag == 0 || (flag == -1 && dbFile.GetLength () > 2))
    {
      dbFile.GetPage (rwBuffer, writePage);
      flag = 1;

    }
  else
    flag = 1;
  if (!rwBuffer->Append (&rec))
    {
      dbFile.AddPage (rwBuffer, writePage);
      writePage++;
      rwBuffer->EmptyItOut ();
      rwBuffer->Append (&rec);

    }

}

int
Heap::GetNext (Record & fetchme)
{

//	cout<<"new point check"<<endl;
  if (flag == -1 && dbFile.GetLength () == 0)
    return 0;

  if (flag != 0)
    {
      if (flag == 1)
	{
	  dbFile.AddPage (rwBuffer, writePage);
	  rwBuffer->EmptyItOut ();
	}
      dbFile.GetPage (rwBuffer, readPage);
      flag = 0;
      for (int i = 0; i < readRecord; i++)
	{
	  Record temp;
	  rwBuffer->GetFirst (&temp);
	}

    }

  if (!rwBuffer->GetFirst (&fetchme))
    {

      int noOfPages = dbFile.GetLength () - 1;
      if (readPage + 1 < noOfPages)
	{
		readRecord = 0;
	  readPage = readPage + 1;
	  dbFile.GetPage (rwBuffer, readPage);
	  rwBuffer->GetFirst (&fetchme);

	}
      else
	return 0;
    }
  readRecord++;
//cout<<readRecord<<endl;
  return 1;
}


int
Heap::GetNext (Record & fetchme, CNF & cnf, Record & literal)
{
  int cnt = 0;
  Schema x ("catalog", "nation");
  ComparisonEngine compEngine;
  while (GetNext (fetchme))
    {
      //fetchme.Print(&x);
      if (compEngine.Compare (&fetchme, &literal, &cnf))
	{
	  return 1;
	}
    }

  return 0;
}


Heap::~Heap ()
{
  delete rwBuffer;
}

