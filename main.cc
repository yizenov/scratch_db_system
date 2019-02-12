#include <iostream>
#include <string>
#include <vector>

#include "Catalog.h"
#include "Config.h"

using namespace std;

Catalog connect_db() {
  string dbFile = DB_FILE;
  Catalog cat_(dbFile);
  return cat_;
}

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
      int attr_nbr;

      cout << "Enter number of attributes: ";
      cin >> attr_nbr;

      for (int i = 0; i < attr_nbr; i++) {
          cin >> attribute >> type;
          attributes.push_back(attribute);
          types.push_back(type);
      }

      cat.CreateTable(table_name, attributes, types);

    } else if (user_choice == 2) {
      string table_name;
      cin >> table_name;
      cat.DropTable(table_name);
    } else if (user_choice == 3) {
      cout << cat;
    } else if (user_choice == 4) {
      if (cat.Save())
        cout << "CATALOG IS UP TO DATE" << endl;
      else
        cout << "ERRORS WHILE UPDATING CATALOG" << endl;
    } else if (user_choice == 5) {
//      if (!cat.Save())
//        cout << "ERRORS WHILE UPDATING CATALOG" << endl;
      break;
    } else {
      cout << "INVALID CHOICE" << endl;
    }
  }

  return 0;
}
