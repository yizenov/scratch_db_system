#include <cstring>
#include <bits/stdc++.h>

#include "Catalog.h"
#include "QueryCompiler.h"
#include "QueryOptimizer.h"
#include "RelOp.h"
//extern "C" {
//    #include "QueryParser.h"
//}

using namespace std;


// PHASE #3 Original --------------------------------------------
int main(int argc, char *argv[]) {

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
      heap_file.Close();

    } else {
      cout << "file loading failed: " << t_name << endl;
      exit(-1);
    }
  }

  //TODO: updating number of pages in rel.ops from File.GetLength()
  //TODO: creating heap file whenever we crate a new table. Should we update phase1 code then?
  //TODO: heap file extension?
  if (!catalog.Save()) {
      cout << "failed to update catalog" << endl;
  }
}