
#include <iostream>
#include "Record.h"
#include <stdlib.h>
#include "DBFile.h"
using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}

extern struct AndList *final;

/*int main () {

	// try to parse the CNF
	cout << "Enter in your CNF: ";
  	if (yyparse() != 0) {
		cout << "Can't parse your CNF.\n";
		exit (1);
	}

	// suck up the schema from the file
	Schema lineitem ("catalog", "lineitem");

	// grow the CNF expression from the parse tree 
	CNF myComparison;
	Record literal;
	myComparison.GrowFromParseTree (final, &lineitem, literal);
	
	// print out the comparison to the screen
	myComparison.Print ();

	// now open up the text file and start procesing it
        FILE *tableFile = fopen ("/cise/tmp/dbi_sp11/DATA/10M/lineitem.tbl", "r");

        Record temp;
        Schema mySchema ("catalog", "lineitem");

	//char *bits = literal.GetBits ();
	//cout << " numbytes in rec " << ((int *) bits)[0] << endl;
	//literal.Print (&supplier);

        // read in all of the records from the text file and see if they match
	// the CNF expression that was typed in
	int counter = 0;
	ComparisonEngine comp;
        while (temp.SuckNextRecord (&mySchema, tableFile) == 1) {
		counter++;
		if (counter % 10000 == 0) {
			cerr << counter << "\n";
		}

		if (comp.Compare (&temp, &literal, &myComparison))
                	temp.Print (&mySchema);

        }

}*/

int main()
{
	/*
	 * test create function of DBfile
	 */
		DBFile dbfile;
		dbfile.Create("db.bin",heap,NULL);
		
		/*
		 * test open function of dbfile
		 */
		 
		 dbfile.Open("db.bin");
		 printf("finished opening file\n------------------------------------------------------------------------");
		 Schema mySchema ("catalog", "lineitem");
		dbfile.Load(mySchema,"./10M/lineitem.tbl");
		Record temp;
		FILE *tableFile = fopen ("./10M/lineitem.tbl", "r");
		if(temp.SuckNextRecord (&mySchema, tableFile) == 1)
		{
			dbfile.Add(temp);
		}
		else
		cout<<"file not read"<<endl;


		Page test;
		int x=dbfile.GetNext(temp);	
		cout<<"get record return value    "<<x<<endl;	
		if(x)
		temp.Print(&mySchema);
		dbfile.Close();
		

return 0;
}
