#include <iostream>
#include <string>
#include <vector>

#include "Catalog.h"
#include "Schema.h"

using namespace std;

Catalog connect_db() {
  string dbFile = "catalog.sqlite";
  Catalog cat_(dbFile);
  return cat_;
}

bool update_catalog() { return true; }

int main() {

  auto cat = connect_db();
  if (!cat.GetConnectionStatus()) {
  	cout << "NO CONNECTION WITH DB" << endl;
  	return 0;
  }
  cat.UploadSchemas();

  unsigned user_choice;
  while (true) {

    cout << "********** Main Menu **********" << endl;
    cout << "(1): Create a table" << endl;
    cout << "(2): Drop a table" << endl;
    cout << "(3): Display the catalog content" << endl;
    cout << "(4): Save the catalog content" << endl;
    cout << "(5): Exit" << endl;
    cout << "Your choice: ";
    cin >> user_choice;

    if (user_choice == 1) {

		string table_name;
		cin >> table_name;

		vector<string> attributes, types;
		string attribute, type;
		cin >> attribute >> type;
		attributes.push_back(attribute); types.push_back(type);

		cat.CreateTable(table_name, attributes, types);

	} else if (user_choice == 2) {
      break;
    } else if (user_choice == 3) {
      break;
    } else if (user_choice == 4) {
      if (update_catalog())
        cout << "CATALOG IS UP TO DATE" << endl;
      else
        cout << "ERRORS WHILE UPDATING CATALOG" << endl;
    } else if (user_choice == 5) {
      if (!update_catalog())
        cout << "ERRORS WHILE UPDATING CATALOG" << endl;
      break;
    } else {
      cout << "INVALID CHOICE" << endl;
    }
  }

  return 0;
}
