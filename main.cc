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

  vector<string> file_names = {"region.tbl", "nation.tbl"};

  File txt_file;

  string f_path = "../tpch_0_01/nation.tbl";
  unsigned long file_name_len = f_path.length() + 1;
  char path[file_name_len];
  strcpy(path, f_path.c_str());

  int status = txt_file.Open(0, path);
  //TODO: heap file extension?

  //TODO: updating number of pages in rel.ops from File.GetLength()

  txt_file.Close();
}