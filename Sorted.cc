/*
 * Sorted.cc
 *
 *  Created on: Mar 7, 2015
 *      Author: mugdha
 */
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
#include "Sorted.h"

Sorted::Sorted() {

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
Sorted::Create (char *f_path, fType f_type, void *startup)
{
	return 0;
}

/** Open the filepath provided by loadpath.
*while Loop through records in the file and load it into the temp record variable
* Keep appending it to a page and when the page is full write this page out
* to the disk using an instance of file class. Clear the page buffer for further
* loads
*/
void
Sorted::Load (Schema & f_schema, char *loadpath)
{

 // write here the implementation
}

/**
*open the DB file, success case returns 1 and failure case returned with 0
*/


int
Sorted::Open (char *f_path)
{
       return 0;
}


/**
*Move the pointer to first record in file
*/
void
Sorted::MoveFirst ()
{
  readPage = 1;
  readRecord = 0;
}

int
Sorted::Close ()
{

 // write here the implementation
  return 1;
}

void
Sorted::Add (Record & rec)
{
  // need to write here the implementation
}

int
Sorted::GetNext (Record & fetchme)
{
  return 1;
}


int
Sorted::GetNext (Record & fetchme, CNF & cnf, Record & literal)
{
  // need to implement here

  return 0;
}


Sorted::~Sorted() {
	 delete rwBuffer;
}
