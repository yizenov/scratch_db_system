#include <cstring>
#include <bits/stdc++.h>

#include "Catalog.h"
#include "QueryCompiler.h"
#include "QueryOptimizer.h"
#include "RelOp.h"
extern "C" {
    #include "QueryParser.h"
}

using namespace std;

// these data structures hold the result of the parsing
extern struct FuncOperator *finalFunction; // the aggregate function
extern struct TableList *tables;           // the list of tables in the query
extern struct AndList *predicate;          // the predicate in WHERE
extern struct NameList *groupingAtts;      // grouping attributes
extern struct NameList *attsToSelect;      // the attributes in SELECT
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query

extern "C" int yyparse();
extern "C" int yylex_destroy();

// PHASE #2 Original --------------------------------------------
int main(int argc, char *argv[]) {

    // this is the catalog
    string dbFile = DB_FILE;
    Catalog catalog(dbFile);

    // this is the query optimizer
    // it is not invoked directly but rather passed to the query compiler
    QueryOptimizer optimizer(catalog);

    // this is the query compiler
    // it includes the catalog and the query optimizer
    QueryCompiler compiler(catalog, optimizer);

    cout << "Enter Next Query" << endl;
    while (true) {

        // the query parser is accessed directly through yyparse
        // this populates the extern data structures
        int parse = -1;
        if (yyparse() == 0) {
            cout << "OK!" << endl;
            parse = 0;
        } else {
            cout << "Error: Query is not correct!" << endl;
            parse = -1;
        }

        yylex_destroy();

        if (parse != 0) return -1;

        // at this point we have the parse tree in the ParseTree data structures
        // we are ready to invoke the query compiler with the given query
        // the result is the execution tree built from the parse tree and optimized
        QueryExecutionTree queryTree;
        compiler.Compile(tables, attsToSelect, finalFunction, predicate, groupingAtts,
                         distinctAtts, queryTree);

        //TODO: printing the tree by levels
        cout << queryTree << endl << endl;

        queryTree.ExecuteQuery();

        string option;
        cout << "Type 'exit' to finish, otherwise enter any other input: ";
        cin >> option;
        if (option == "exit")
            return 0;
        else
            cout << "Enter Next Query" << endl;
    }
}
///////////////////////////////////////////////////////////////////////////////////////////////////////////

// PHASE #3 Original --------------------------------------------
int main1(int argc, char *argv[]) {

  // this is the catalog
  string dbFile = DB_FILE;
  Catalog catalog(dbFile);

  string txt_files_location = "tpch_0_01_sf/";
  string heap_files_location = "HeapFileData/";

  vector<string> table_names;
  catalog.GetTables(table_names);

  for (string t_name : table_names) {

    string txt_file_name = txt_files_location + t_name + ".tbl";
    string heap_file_name = heap_files_location + t_name + ".hfile";

    DBFile heap_file;
    unsigned long heap_file_name_len = heap_file_name.length() + 1;
    char heap_file_path[heap_file_name_len];
    strcpy(heap_file_path, heap_file_name.c_str());

    if (heap_file.Create(heap_file_path, Heap) == 0) {

      Schema table_schema;
      if (!catalog.GetSchema(t_name, table_schema)) {
        cout << "table schema wasn't found: " << t_name << endl;
        exit(-1);
      }

      unsigned long txt_file_name_len = txt_file_name.length() + 1;
      char txt_file_path[txt_file_name_len];
      strcpy(txt_file_path, txt_file_name.c_str());
      heap_file.Load(table_schema, txt_file_path);

      // update catalog
      catalog.SetDataFile(t_name, heap_file_name);
      int no_pages = heap_file.Close(); // TODO: what do we do with this info?

    } else {
      cout << "file loading failed: " << t_name << endl;
      exit(-1);
    }
  }

  //TODO: updating number of pages in rel.ops from File.GetLength()
  //TODO: creating heap file whenever we create a new table. Should we update phase1 code then?
  //TODO: heap file extension?
  if (!catalog.Save()) {
      cout << "failed to update catalog" << endl;
  }
}