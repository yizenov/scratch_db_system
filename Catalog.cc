#include "Catalog.h"

#include "InefficientMap.cc"  //TODO:undefined reference (forced because of using 'template' in there)
#include "Keyify.cc" //TODO:undefined reference (forced because of using 'template' in there)
#include "ComplexSwapify.cc" //TODO:undefined reference (forced because of using 'template' in there)
#include "DBFile.h"

#include <bits/stdc++.h>

using namespace std;

bool isItError(int rc) {
  // only OK, ROW, DONE are non-error result codes
  if (rc != SQLITE_OK && rc != SQLITE_ROW && rc != SQLITE_DONE) {
    return true;
  } else {
    return false;
  }
}

Catalog::Catalog(string &_fileName) {

  ifstream input_file(_fileName);
  if (input_file.fail()) {
    connection_status = false;
    cout << "database file wasn't found." << endl;
  } else {
    if (sqlite3_open(_fileName.c_str(), &catalog_db) == SQLITE_OK) {
      // sqlite3_exec(catalog_db,"PRAGMA
      // schema.page_size=16384",NULL,NULL,NULL); // TODO: try to use PRAGMA
      connection_status = true;
      UploadSchemas();
      cout << "database is connected." << endl << endl;
    } else {
      connection_status = false;
      cout << "database is not connected." << endl;
    }
  }
}

Catalog::~Catalog() {
  this->Save();
  sqlite3_close(catalog_db);
  connection_status = false;
  schema_data_->Clear();
  delete schema_data_;
}

bool Catalog::Save() {

  if (!connection_status) { // TODO: sqlite3_db_status(catalog_db) != SQLITE_OK
    cout << "database is not connected." << endl;
    return false;
  }
  sqlite3_exec(catalog_db, "BEGIN TRANSACTION", nullptr, nullptr, &error_msg);

  // dropping in-memory deleted tables
  query = "SELECT " + table_col1 + " FROM " DB_TABLE_LIST ";";
  sqlite3_prepare_v2(catalog_db, query.c_str(), -1, &stmt, nullptr);
  set<string> existing_tables;

  string query_one = "SELECT " + table_attr_col2 +
                     " FROM " DB_TABLE_ATTR_LIST " WHERE " + table_attr_col1 +
                     " = ?1;";
  sqlite3_stmt *stmt_one;
  sqlite3_prepare_v2(catalog_db, query_one.c_str(), -1, &stmt_one, nullptr);

  string inner_query_one =
      "DELETE FROM " DB_TABLE_ATTR_LIST " WHERE " + table_attr_col1 + " = ?1;";
  sqlite3_stmt *inner_stmt_one;
  sqlite3_prepare_v2(catalog_db, inner_query_one.c_str(), -1, &inner_stmt_one,
                     nullptr);

  string inner_query_two =
      "DELETE FROM " DB_TABLE_LIST " WHERE " + table_col1 + " = ?1;";
  sqlite3_stmt *inner_stmt_two;
  sqlite3_prepare_v2(catalog_db, inner_query_two.c_str(), -1, &inner_stmt_two,
                     nullptr);

  string inner_query_three =
      "DELETE FROM " DB_ATTRIBUTE_LIST " WHERE " + attr_col1 + " = ?1;";
  sqlite3_stmt *inner_stmt_three;
  sqlite3_prepare_v2(catalog_db, inner_query_three.c_str(), -1,
                     &inner_stmt_three, nullptr);

  while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {

    string table_name =
        string(reinterpret_cast<const char *>(sqlite3_column_text(stmt, 0)));
    KeyString table_data(table_name);
    //Schema *table_schema = &schema_data_->CurrentData().GetData();
    // if row is not present in memory or has been updated
    if (schema_data_->IsThere(table_data) == 0) {
      // || table_schema.GetSchemaStatus()) {

      // collecting attribute names
      vector<string> attributes;
      sqlite3_bind_text(stmt_one, 1, table_name.c_str(), -1, NULL);
      while (sqlite3_step(stmt_one) == SQLITE_ROW) {
        string attr_name = string(
            reinterpret_cast<const char *>(sqlite3_column_text(stmt_one, 0)));
        attributes.push_back(attr_name);
      }
      sqlite3_clear_bindings(stmt_one);
      sqlite3_reset(stmt_one);

      // deleting data from table_attribute
      sqlite3_bind_text(inner_stmt_one, 1, table_name.c_str(), -1,
                        SQLITE_STATIC);
      rc = sqlite3_step(inner_stmt_one);
      if (isItError(rc)) {
        cout << sqlite3_errmsg(catalog_db) << endl;
        cout << "pairs where " << table_attr_col1 << " = " << table_name
             << ") weren't deleted in the database." << endl;
        sqlite3_finalize(inner_stmt_one);
        return false;
      }
      sqlite3_clear_bindings(inner_stmt_one);
      sqlite3_reset(inner_stmt_one);

      // deleting data from attribute
      for (auto att : attributes) {
        sqlite3_bind_text(inner_stmt_three, 1, att.c_str(), -1, SQLITE_STATIC);
        rc = sqlite3_step(inner_stmt_three);
        if (isItError(rc)) {
          cout << sqlite3_errmsg(catalog_db) << endl;
          cout << "attribute: " << att << " wasn't deleted in the database."
               << endl;
          sqlite3_finalize(inner_stmt_three);
          return false;
        }
        sqlite3_clear_bindings(inner_stmt_three);
        sqlite3_reset(inner_stmt_three);
      }

      // deleting data from table
      sqlite3_bind_text(inner_stmt_two, 1, table_name.c_str(), -1,
                        SQLITE_STATIC);
      rc = sqlite3_step(inner_stmt_two);
      if (isItError(rc)) {
        cout << sqlite3_errmsg(catalog_db) << endl;
        cout << "table: " << table_name << " wasn't deleted in the database."
             << endl;
        sqlite3_finalize(inner_stmt_two);
        return false;
      }
      sqlite3_clear_bindings(inner_stmt_two);
      sqlite3_reset(inner_stmt_two);
    } else {
      // existing tables in database
      existing_tables.insert(table_name);
    }
  }
  sqlite3_finalize(stmt);

  sqlite3_exec(catalog_db, "END TRANSACTION", nullptr, nullptr, &error_msg);

  sqlite3_exec(catalog_db, "BEGIN TRANSACTION", nullptr, nullptr, &error_msg);

  string attr_query = "INSERT INTO " DB_ATTRIBUTE_LIST " (" +
                      attr_col1 + "," + attr_col2 + "," + attr_col3 +
                      ") VALUES (?1,?2,?3);";
  sqlite3_stmt *stmt_attr;
  sqlite3_prepare_v2(catalog_db, attr_query.c_str(), -1, &stmt_attr, nullptr);

  string table_attr_query = "INSERT INTO " DB_TABLE_ATTR_LIST " (" +
                            table_attr_col1 + "," + table_attr_col2 + "," + table_attr_col3 +
                            ") VALUES (?1,?2, ?3);";
  sqlite3_stmt *stmt_table_attr;
  sqlite3_prepare_v2(catalog_db, table_attr_query.c_str(), -1, &stmt_table_attr,
                     nullptr);

  string table_query = "INSERT INTO " DB_TABLE_LIST " (" +
                       table_col1 + "," + table_col2 + "," + table_col3 +
                       ") VALUES (?1,?2,?3);";
  sqlite3_stmt *stmt_table;
  sqlite3_prepare_v2(catalog_db, table_query.c_str(), -1, &stmt_table, nullptr);

  // creating tables in the database
  schema_data_->MoveToStart();
  while (!schema_data_->AtEnd()) {
    string table_name = schema_data_->CurrentKey();
    Schema *table_schema = &schema_data_->CurrentData();

    auto it = existing_tables.find(table_name);
    if (it != existing_tables.end() && !table_schema->GetSchemaStatus()) {
      schema_data_->Advance(); // skips existing and unchanged tables in db.
      continue;
    }

    unsigned long file_name_len = table_schema->GetTablePath().length() + 1;
    char file_path[file_name_len];
    strcpy(file_path, table_schema->GetTablePath().c_str());

    sqlite3_bind_text(stmt_table, 1, table_name.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt_table, 2, table_schema->GetTuplesNumber());
    sqlite3_bind_text(stmt_table, 3, file_path, -1, SQLITE_STATIC);
    rc = sqlite3_step(stmt_table);
    if (isItError(rc)) {
      cout << sqlite3_errmsg(catalog_db) << endl;
      cout << "table: " << table_name << " wasn't inserted in the database."
           << endl;
      sqlite3_finalize(stmt_table);
      return false;
    }
    sqlite3_clear_bindings(stmt_table);
    sqlite3_reset(stmt_table);

    for (Attribute &att : table_schema->GetAtts()) {

      string attr_type = "Unknown";
      if (att.type == Integer)
        attr_type = "Integer";
      else if (att.type == Float)
        attr_type = "Float";
      else if (att.type == String)
        attr_type = "String";

      sqlite3_bind_text(stmt_attr, 1, att.name.c_str(), -1, SQLITE_STATIC);
      sqlite3_bind_text(stmt_attr, 2, attr_type.c_str(), -1, SQLITE_STATIC);
      sqlite3_bind_int(stmt_attr, 3, att.noDistinct);
      rc = sqlite3_step(stmt_attr);
      if (isItError(rc)) {
        cout << sqlite3_errmsg(catalog_db) << endl;
        cout << "attribute: " << att.name << " wasn't inserted in the database."
             << endl;
        sqlite3_finalize(stmt_attr);
        return false;
      }
      sqlite3_clear_bindings(stmt_attr);
      sqlite3_reset(stmt_attr);

      int attr_index = table_schema->Index(att.name);
      if (attr_index == -1) {
          cout << "attribute index is incorrect" << endl;
          exit(-1);
      }

      sqlite3_bind_text(stmt_table_attr, 1, table_name.c_str(), -1,
                        SQLITE_STATIC);
      sqlite3_bind_text(stmt_table_attr, 2, att.name.c_str(), -1,
                        SQLITE_STATIC);
      sqlite3_bind_int(stmt_table_attr, 3, attr_index);

      rc = sqlite3_step(stmt_table_attr);
      if (isItError(rc)) {
        cout << sqlite3_errmsg(catalog_db) << endl;
        cout << "pair: (" << table_name << "," << att.name
             << ") wasn't inserted in the database." << endl;
        sqlite3_finalize(stmt_table_attr);
        return false;
      }
      sqlite3_clear_bindings(stmt_table_attr);
      sqlite3_reset(stmt_table_attr);
    }

    // TODO: roll-back if failure happens

    schema_data_->Advance();
  }

  sqlite3_exec(catalog_db, "END TRANSACTION", nullptr, nullptr, &error_msg);

  // TODO: roll-back if failure happens
  // TODO: do updates and changes (attr,path,etc. delete/update/add)

  return true;
}

bool Catalog::GetNoTuples(string &_table, unsigned long int &_noTuples) {
  KeyString table_name(_table);
  if (schema_data_->IsThere(table_name) == 0)
    return false;
  Schema *schema_ = &schema_data_->Find(table_name);
  _noTuples = schema_->GetTuplesNumber();
  return true;
}

void Catalog::SetNoTuples(string &_table, unsigned long int &_noTuples) {
  KeyString table_name(_table);
  if (schema_data_->IsThere(table_name) == 1) {
    Schema *schema_ = &schema_data_->Find(table_name);
    schema_->SetTuplesNumber(_noTuples);
    schema_data_->Find(table_name).Swap(*schema_);
  }
}

bool Catalog::GetDataFile(string &_table, string &_path) {
  KeyString table_name(_table);
  if (schema_data_->IsThere(table_name) == 0)
    return false;
  Schema *schema_ = &schema_data_->Find(table_name);
  _path = schema_->GetTablePath();
  return true;
}

void Catalog::SetDataFile(string &_table, string &_path) {
  KeyString table_name(_table);
  if (schema_data_->IsThere(table_name) == 1) {
    Schema *schema_ = &schema_data_->Find(table_name);
    schema_->SetTablePath(_path);
    schema_data_->Find(table_name).Swap(*schema_);
  }
}

bool Catalog::GetNoDistinct(string &_table, string &_attribute,
                            unsigned int &_noDistinct) {
  KeyString table_name(_table);
  if (schema_data_->IsThere(table_name) == 0)
    return false;
  Schema *schema_ = &schema_data_->Find(table_name);
  int tuple_no = schema_->GetDistincts(_attribute);
  if (tuple_no == -1)
    return false;
  _noDistinct = (unsigned int)tuple_no; // TODO: initial type mismatch
  return true;
}
void Catalog::SetNoDistinct(string &_table, string &_attribute,
                            unsigned int &_noDistinct) {
  KeyString table_name(_table);
  if (schema_data_->IsThere(table_name) == 1) {
    Schema *schema_ = &schema_data_->Find(table_name);
    schema_->SetDistincts(_attribute, _noDistinct);
    schema_data_->Find(table_name).Swap(*schema_);
  }
}

bool Catalog::GetAttributeType(string &_table, string &_attribute,
                               string &_attrType) {
  KeyString table_name(_table);
  if (schema_data_->IsThere(table_name) == 0)
    return false;
  ComplexSwapify<Schema> schema_ = schema_data_->Find(table_name);
  int attr_idx = schema_.GetData().Index(_attribute);
  if (attr_idx == -1)
    return false;

  Type att_type = schema_.GetData().FindType(_attribute);
  if (att_type == Integer)
    _attrType = "Integer"; //"INTEGER";
  else if (att_type == Float)
    _attrType = "Float"; //"FLOAT";
  else if (att_type == String)
    _attrType = "String"; //"STRING";
  else
    _attrType = "Unknown";

  return true;
}

void Catalog::GetTables(vector<string> &_tables) {
  schema_data_->MoveToStart();
  while (!schema_data_->AtEnd()) {
    _tables.push_back(schema_data_->CurrentKey());
    schema_data_->Advance();
  }
}

bool Catalog::GetAttributes(string &_table, vector<string> &_attributes) {
  KeyString table_name(_table);
  if (schema_data_->IsThere(table_name) == 0)
    return false;
  Schema *schema_ = &schema_data_->Find(table_name);
  auto *attrs = &schema_->GetAtts();
  for (auto it : *attrs)
    _attributes.push_back(it.name);
  return true;
}

bool Catalog::GetSchema(string &_table, Schema &_schema) {
  KeyString table_name(_table);
  if (schema_data_->IsThere(table_name) == 0)
    return false;
  _schema = schema_data_->Find(table_name);
  return true;
}

bool Catalog::CreateTable(string &_table, vector<string> &_attributes,
                          vector<string> &_attributeTypes) {

  KeyString table_data(_table);
  if (schema_data_->IsThere(table_data) != 0)
    return false;
  vector<unsigned int> distinct_values(_attributes.size(), 0);
  Schema table_schema(_attributes, _attributeTypes, distinct_values);
  schema_data_->Insert(table_data, table_schema);

  table_data = _table;
  return schema_data_->IsThere(table_data) != 0;
}

bool Catalog::DropTable(string &_table) {
  KeyString table_info(_table);
  if (schema_data_->IsThere(table_info) == 0)
    return false;
  KeyString table_name("");
  vector<string> attributes, attr_types;
  vector<unsigned int> distinct_values;
  Schema table_schema(attributes, attr_types, distinct_values);
  return schema_data_->Remove(table_info, table_name, table_schema) != 0;
}

ostream &operator<<(ostream &_os, Catalog &_c) {

  vector<string> tables;

  _c.GetTables(tables);
  for (auto &table : tables) {
    _os << "TABLE_NAME: " << table << "; ";

    Schema table_schema;
    if (!_c.GetSchema(table, table_schema)) {
      _os << "; NO SCHEMA." << endl;
      continue;
    }
    _os << table_schema << endl;
  }

  return _os;
}

void Catalog::UploadSchemas() {

  if (!connection_status) { // TODO: sqlite3_db_status(catalog_db) != SQLITE_OK
    cout << "database is not connected." << endl;
  } else {

    schema_data_ = new InefficientMap<KeyString, Schema>(); // TODO: avoid NEW

    query = "SELECT * FROM " DB_TABLE_LIST ";";
    sqlite3_prepare_v2(catalog_db, query.c_str(), -1, &stmt, nullptr);

    string inner_query_one = "SELECT " + table_attr_col2 + ", " + table_attr_col3 +
                             " FROM " DB_TABLE_ATTR_LIST " WHERE " +
                             table_attr_col1 + " = ?;";
    sqlite3_stmt *inner_stmt_one;
    sqlite3_prepare_v2(catalog_db, inner_query_one.c_str(), -1, &inner_stmt_one,
                       nullptr);

    string inner_query_two = "SELECT " + attr_col2 + ", " + attr_col3 +
                             " FROM " DB_ATTRIBUTE_LIST " WHERE " + attr_col1 +
                             " = ?;";
    sqlite3_stmt *inner_stmt_two;
    sqlite3_prepare_v2(catalog_db, inner_query_two.c_str(), -1, &inner_stmt_two,
                       nullptr);

    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
      string table_name =
          string(reinterpret_cast<const char *>(sqlite3_column_text(stmt, 0)));
      KeyString table_data(table_name);

      vector<string> attributes, attr_types;
      vector<unsigned int> distinct_values;

      vector<int> attr_orders; // temporal fix

      sqlite3_bind_text(inner_stmt_one, 1, table_name.c_str(), -1, SQLITE_STATIC);
      while (sqlite3_step(inner_stmt_one) == SQLITE_ROW) {
        string attr_name = string(reinterpret_cast<const char *>(
            sqlite3_column_text(inner_stmt_one, 0)));
        int attr_order = (int)sqlite3_column_int(inner_stmt_one, 1);

        sqlite3_bind_text(inner_stmt_two, 1, attr_name.c_str(), -1, NULL);
        while (sqlite3_step(inner_stmt_two) == SQLITE_ROW) {
          string attr_type =
              string(reinterpret_cast<const char *>(sqlite3_column_text(
                  inner_stmt_two, 0))); // TYPES: INTEGER, FLOAT, STRING
          auto dist_no = (unsigned int)sqlite3_column_int(
              inner_stmt_two, 1); // TODO: extra work
          attributes.push_back(attr_name);
          attr_types.push_back(attr_type);
          attr_orders.push_back(attr_order);
          distinct_values.push_back(dist_no);
          break;
        }
        sqlite3_clear_bindings(inner_stmt_two);
        sqlite3_reset(inner_stmt_two);
      }

      //ordering the attributes
      int attr_no = attributes.size();
      vector<string> new_attributes(attr_no), new_attr_types(attr_no);
      vector<unsigned int> new_distinct_values(attr_no);
      for (auto i = 0; i < attr_orders.size(); i++) {
        int idx = attr_orders[i];
        new_attributes[idx] = attributes[i];
        new_attr_types[idx] = attr_types[i];
        new_distinct_values[idx] = distinct_values[i];
      }

      Schema table_schema(new_attributes, new_attr_types, new_distinct_values);
      sqlite3_clear_bindings(inner_stmt_one);
      sqlite3_reset(inner_stmt_one);

      auto tuple_no = (unsigned long int)sqlite3_column_int(stmt, 1);
      string table_path =
          string(reinterpret_cast<const char *>(sqlite3_column_text(stmt, 2)));

      table_schema.SetTuplesNumber(tuple_no); // TODO: use SetNoTuples
      table_schema.SetTablePath(table_path);  // TODO: use SetDataFile

      schema_data_->Insert(table_data, table_schema);
      // SetNoTuples(table_name, tuple_no);
      // SetDataFile(table_name, table_path);
    }

    sqlite3_finalize(stmt);
  }
}

string Catalog::GetTypeName(Type _attr_type) {
  if (_attr_type == Integer)
    return "Integer";
  else if (_attr_type == Float)
    return "Float";
  else if (_attr_type == String)
    return "String";
  else
    return "UNKNOWN TYPE";
}

void Catalog::CreateTableSQL(char* _table, AttsAndTypes* _attrAndTypes) {
  string table_name = _table;
  vector<string> attributes;
  vector<string> attribute_types;

  AttsAndTypes *current = _attrAndTypes;
  while (current != NULL) {
      string type_name = current->type;
      if (type_name != "Integer" && type_name != "Float" && type_name != "String") {
          cout << "Unsupported type in creating a table" << endl;
          exit(-1);
      }
      string col_name = current->name;
      attributes.push_back(col_name);
      attribute_types.push_back(type_name);
      current = current->next;
  }
  reverse(attribute_types.begin(), attribute_types.end());
  reverse(attributes.begin(), attributes.end());

  //TODO: projects in reverse order
  bool status = CreateTable(table_name, attributes, attribute_types);
  if (status) {
      KeyString table_data(table_name);
      if (schema_data_->IsThere(table_data) == 0) {
          cout << "schema wasn't found for the new table" << endl;
          exit(-1);
      }
      Schema& table_schema = schema_data_->Find(table_data);
      string path = "HeapFileData/" + table_name + ".hfile";
      table_schema.SetTablePath(path);
      cout << "TABLE IS SAVED IN DB" << endl;

      this->Save(); // saving into the db
  } else {
      cout << "TABLE IS FAILED TO BE SAVED" << endl;
      exit(-1);
  }
}

void Catalog::LoadDataSQL(char* _table, char* _textFile) {
  // delimiter '|'

  string txt_files_location = "tpch_0_01_sf/";
  string txt_file_name = txt_files_location + _textFile + ".tbl";

  string table_name = _table;
  KeyString table_data(table_name);
  if (schema_data_->IsThere(table_data) == 0) {
      cout << "schema wasn't found for the new table" << endl;
      exit(-1);
  }
  Schema& table_schema = schema_data_->Find(table_data);
  string heap_file_name = table_schema.GetTablePath();

  DBFile heap_file;
  unsigned long heap_file_name_len = heap_file_name.length() + 1;
  char heap_file_path[heap_file_name_len];
  strcpy(heap_file_path, heap_file_name.c_str());

  if (heap_file.Create(heap_file_path, Heap) == 0) {

    unsigned long txt_file_name_len = txt_file_name.length() + 1;
    char txt_file_path[txt_file_name_len];
    strcpy(txt_file_path, txt_file_name.c_str());

    heap_file.Load(table_schema, txt_file_path);

    // update catalog
    // TODO: update number of tuples, distinct value per column in catalog
    int no_pages = heap_file.Close();
    cout << "TEXT DATA IS LOADED INTO THE TABLE" << endl;

  } else {
    cout << "file loading failed: " << table_name << endl;
    exit(-1);
  }

}

void Catalog::CreateIndexSQL(char* _index, char* _table, char* _attr) {
  // need to store in catalog if there is index on  given table column
  // decision to use index during optimization
  cout << "123" << endl;
}