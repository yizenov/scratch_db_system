#include <ctime>

#include "Catalog.h"

using namespace std;


int main (int argc, char* argv[]) {

  clock_t begin = clock();

  if (argc != 4) {
      cout << "Usage: main [sqlite_file] [no_tables] [no_atts]" << endl;
    return -1;
  }

  string dbFile = argv[1];
  int tNo = atoi(argv[2]);
  int aNo = atoi(argv[3]);

  Catalog catalog(dbFile);
  cout << catalog << endl; cout.flush();

  ////////////////////////////////
  for (int i = 0; i < tNo; i++) {
    char tN[20]; sprintf(tN, "T_%d", i);
    string tName = tN;

    int taNo = i * aNo;
    vector<string> atts;
    vector<string> types;
    for (int j = 0; j < taNo; j++) {
      char aN[20]; sprintf(aN, "A_%d_%d", i, j);
      string aName = aN;
      atts.push_back(aN);

      string aType;
      int at = j % 3;
      if (0 == at) aType = "Integer"; //TODO: Integer or INTEGER
      else if (1 == at) aType = "Float";
      else if (2 == at) aType = "String";
      types.push_back(aType);
    }

    bool ret = catalog.CreateTable(tName, atts, types);
    if (true == ret) {
      cout << "CREATE TABLE " << tName << " OK" << endl;

      for (int j = 0; j < taNo; j++) {
        unsigned int dist = i * 10 + j;
        string aN = atts[j];
        catalog.SetNoDistinct(tName, atts[j], dist);
      }

      unsigned int tuples = i * 1000;
      catalog.SetNoTuples(tName, tuples);

      string path = tName + ".dat";
      catalog.SetDataFile(tName, path);
    }
    else {
      cout << "CREATE TABLE " << tName << " FAIL" << endl;
    }
  }

  clock_t end = clock();
  double elapsed_secs1 = double(end - begin) / CLOCKS_PER_SEC;
  begin = clock();

  ////////////////////////////////
  catalog.Save();
  cout << catalog << endl; cout.flush();

  ////////////////////////////////
  vector<string> tables;
  catalog.GetTables(tables);
  for (vector<string>::iterator it = tables.begin();
       it != tables.end(); it++) {
    cout << *it << endl;
  }
  cout << endl;

  end = clock();
  double elapsed_secs2 = double(end - begin) / CLOCKS_PER_SEC;
  begin = clock();


  ////////////////////////////////
  for (int i = 0; i < 1000; i++) {
    int r = rand() % tNo + 1; //TODO: need to be in range of number of tables
    char tN[20]; sprintf(tN, "T_%d", r);
    string tName = tN;

    unsigned int tuples;
    catalog.GetNoTuples(tName, tuples);
    cout << tName << " tuples = " << tuples << endl;

    string path;
    catalog.GetDataFile(tName, path);
    cout << tName << " path = " << path << endl;

    vector<string> atts;
    catalog.GetAttributes(tName, atts);
    for (vector<string>::iterator it = atts.begin();
         it != atts.end(); it++) {
      cout << *it << " ";
    }
    cout << endl;

    Schema schema;
    catalog.GetSchema(tName, schema);
    cout << schema << endl;

    ////////////////////////////////
    for (int j = 0; j < 10; j++) {
      int s = rand() % (r * aNo) + 1; //TODO: rand should be between 0 and n-1
      char aN[20]; sprintf(aN, "A_%d_%d", r, s);
      string aName = aN;

      unsigned int distinct;
      catalog.GetNoDistinct(tName, aName, distinct);
      cout << tName << "." << aName << " distinct = " << distinct << endl;
    }
  }

  end = clock();
  double elapsed_secs3 = double(end - begin) / CLOCKS_PER_SEC;
  begin = clock();


  ////////////////////////////////
  for (int i = 0; i < 5; i++) {
    char tN[20]; sprintf(tN, "T_%d", i);
    string tName = tN;

    bool ret = catalog.DropTable(tName);
    if (true == ret) {
      cout << "DROP TABLE " << tName << " OK" << endl;
    }
    else {
      cout << "DROP TABLE " << tName << " FAIL" << endl;
    }
  }

  catalog.Save();
  cout << catalog << endl; cout.flush();

  end = clock();
  double elapsed_secs4 = double(end - begin) / CLOCKS_PER_SEC;

  cout << "Runtime (INSERT TABLES+ATTRIBUTES): " << elapsed_secs1 << endl;
  cout << "Runtime (PRINT TABLES): " << elapsed_secs2 << endl;
  cout << "Runtime (PRINT SCHEMAS): " << elapsed_secs3 << endl;
  cout << "Runtime (DROP TABLES): " << elapsed_secs4 << endl;

  return 0;
}

//--------------------------------------------------------------------------------------
//Catalog connect_db() {
//  string dbFile = DB_FILE;
//  Catalog cat_(dbFile);
//  return cat_;
//}
//
//int main() {
//
//  auto cat = connect_db();
//  if (!cat.GetConnectionStatus()) {
//    cout << "NO CONNECTION WITH DB" << endl;
//    return 0;
//  }
//
//  unsigned user_choice;
//  while (true) {
//
//    cout << "********** Main Menu **********" << endl;
//    cout << "(1): Create a table" << endl;
//    cout << "(2): Drop a table" << endl;
//    cout << "(3): Display the catalog content" << endl;
//    cout << "(4): Save the catalog content" << endl;
//    cout << "(5): Exit" << endl;
//    cout << "Your choice: ";
//    cin >> user_choice;
//
//    if (user_choice == 1) {
//
//      string table_name;
//      cout << "Enter table name: ";
//      cin >> table_name;
//
//      vector<string> attributes, types;
//      string attribute, type;
//      int attr_nbr;
//
//      cout << "Enter number of attributes: ";
//      cin >> attr_nbr;
//
//      cout << "Enter attribute name and type:" << endl;
//      for (int i = 0; i < attr_nbr; i++) {
//        cout << i + 1 << ": ";
//        cin >> attribute >> type;
//        attributes.push_back(attribute);
//        types.push_back(type);
//      }
//
//      cat.CreateTable(table_name, attributes, types);
//
//    } else if (user_choice == 2) {
//      string table_name;
//      cout << "Enter table name: ";
//      cin >> table_name;
//      if (cat.DropTable(table_name))
//        cout << "table is deleted." << endl;
//      else
//        cout << "deletion failed." << endl;
//    } else if (user_choice == 3) {
//      cout << cat;
//    } else if (user_choice == 4) {
//      if (cat.Save())
//        cout << "CATALOG IS UP TO DATE" << endl;
//      else
//        cout << "ERRORS WHILE UPDATING CATALOG" << endl;
//    } else if (user_choice == 5) {
//      //      if (!cat.Save())
//      //        cout << "ERRORS WHILE UPDATING CATALOG" << endl;
//      break;
//    } else {
//      cout << "INVALID CHOICE" << endl;
//    }
//  }
//
//  return 0;
//}
