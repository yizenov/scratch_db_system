
#include "Catalog.h"

Catalog::Catalog(string &_fileName) {

  ifstream input_file(_fileName);
  if (input_file.fail()) {
    connection_status = false;
    cout << "database file wasn't found." << endl;
  } else {
    if (sqlite3_open(_fileName.c_str(), &catalog_db) == SQLITE_OK) {
      connection_status = true;
      cout << "database is connected." << endl;
    } else {
      connection_status = false;
      cout << "database is not connected." << endl;
    }
  }
}

Catalog::~Catalog() {
  sqlite3_close(catalog_db);
  connection_status = false;
  schema_data_->Clear();
}

bool Catalog::Save() {

  if (!connection_status) { // TODO: sqlite3_db_status(catalog_db) != SQLITE_OK
    cout << "database is not connected." << endl;
    return false;
  }

  // dropping in-memory deleted tables
  query = "SELECT " + table_col1 + " FROM " DB_TABLE_LIST ";";
  sqlite3_prepare_v2(catalog_db, query.c_str(), -1, &stmt, nullptr);
  set<string> existing_tables;

  while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
    string table_name =
        string(reinterpret_cast<const char *>(sqlite3_column_text(stmt, 0)));
    KeyString table_data(table_name);
    if (schema_data_->IsThere(table_data) == 0) {

      sqlite3_stmt *inner_stmt;
      string inner_query = "DELETE FROM " DB_TABLE_ATTR_LIST " WHERE " +
                           table_attr_col1 + " = '" + table_name + "';";
      sqlite3_prepare_v2(catalog_db, inner_query.c_str(), -1, &inner_stmt,
                         nullptr);
      if (sqlite3_step(inner_stmt) != SQLITE_DONE) {
        cout << sqlite3_errmsg(catalog_db) << endl;
        cout << "pairs where " << table_attr_col1 << " = " << table_name
             << ") weren't deleted in the database." << endl;
        sqlite3_finalize(inner_stmt);
        return false;
      }
      sqlite3_finalize(inner_stmt);

      inner_query = "DELETE FROM " DB_TABLE_LIST " WHERE " + table_col1 +
                    " = '" + table_name + "';";

      sqlite3_prepare_v2(catalog_db, inner_query.c_str(), -1, &inner_stmt,
                         nullptr);
      if (sqlite3_step(inner_stmt) != SQLITE_DONE) {
        cout << sqlite3_errmsg(catalog_db) << endl;
        cout << "table: " << table_name << " wasn't deleted in the database."
             << endl;
        sqlite3_finalize(inner_stmt);
        return false;
      }
      sqlite3_finalize(inner_stmt);

    } else {
      // existing tables
      existing_tables.insert(table_name);
    }
  }
  sqlite3_finalize(stmt);

  // creating tables in the database
  schema_data_->MoveToStart();
  while (!schema_data_->AtEnd()) {
    string table_name = schema_data_->CurrentKey();
    auto it = existing_tables.find(table_name);
    if (it != existing_tables.end()) { // skips existing tables
      schema_data_->Advance();
      continue;
    }

    Schema table_schema = schema_data_->CurrentData().GetData();
    vector<Attribute> attribute_infos = table_schema.GetAtts();

    for (auto att : attribute_infos) {

      string attr_type = "UNKNOWN";
      if (att.type == Integer)
        attr_type = "INTEGER";
      else if (att.type == Float)
        attr_type = "FLOAT";
      else if (att.type == String)
        attr_type = "STRING";

      query = "INSERT OR REPLACE INTO " DB_ATTRIBUTE_LIST "(" + attr_col1 +
              "," + attr_col2 + "," + attr_col3 + ") VALUES('" + att.name +
              "','" + attr_type + "'," + to_string(att.noDistinct) + ");";
      sqlite3_prepare_v2(catalog_db, query.c_str(), -1, &stmt, nullptr);
      if (sqlite3_step(stmt) != SQLITE_DONE) {
        cout << sqlite3_errmsg(catalog_db) << endl;
        cout << "attribute: " << att.name << " wasn't inserted in the database."
             << endl;
        sqlite3_finalize(stmt);
        schema_data_->Advance();
        return false;
      }
      sqlite3_finalize(stmt);

      query = "INSERT OR REPLACE INTO " DB_TABLE_ATTR_LIST "(" +
              table_attr_col1 + "," + table_attr_col2 + ") VALUES('" +
              table_name + "','" + att.name + "');";
      sqlite3_prepare_v2(catalog_db, query.c_str(), -1, &stmt, nullptr);
      if (sqlite3_step(stmt) != SQLITE_DONE) {
        cout << sqlite3_errmsg(catalog_db) << endl;
        cout << "pair: (" << table_name << "," << att.name
             << ") wasn't inserted in the database." << endl;
        sqlite3_finalize(stmt);
        schema_data_->Advance();
        return false;
      }
      sqlite3_finalize(stmt);
    }

    // TODO: roll-back if failure happens

    query = "INSERT OR REPLACE INTO " DB_TABLE_LIST "(" + table_col1 + "," +
            table_col2 + "," + table_col3 + ") VALUES('" + table_name + "'," +
            to_string(table_schema.GetTuplesNumber()) + ",'" +
            table_schema.GetTablePath() + "');";
    sqlite3_prepare_v2(catalog_db, query.c_str(), -1, &stmt, nullptr);
    if (sqlite3_step(stmt) != SQLITE_DONE) {
      cout << sqlite3_errmsg(catalog_db) << endl;
      cout << "table: " << table_name << " wasn't inserted in the database."
           << endl;
      sqlite3_finalize(stmt);
      schema_data_->Advance();
      return false;
    }
    sqlite3_finalize(stmt);

    schema_data_->Advance();
  }

  // TODO: roll-back if failure happens
  // TODO: do updates and changes (attr,path,etc. delete/update/add)

  return true;
}

bool Catalog::GetNoTuples(string &_table, unsigned int &_noTuples) {
  KeyString table_name(_table);
  if (schema_data_->IsThere(table_name) == 0)
    return false;
  auto schema_ = schema_data_->Find(table_name);
  _noTuples = schema_.GetData().GetTuplesNumber();
  return true;
}

void Catalog::SetNoTuples(string &_table, unsigned int &_noTuples) {
  KeyString table_name(_table);
  if (schema_data_->IsThere(table_name) == 1) {
    auto schema_ = schema_data_->Find(table_name);
    schema_.GetData().SetTuplesNumber(_noTuples); // TODO: doesn't work
  }
}

bool Catalog::GetDataFile(string &_table, string &_path) {
  KeyString table_name(_table);
  if (schema_data_->IsThere(table_name) == 0)
    return false;
  auto schema_ = schema_data_->Find(table_name);
  _path = schema_.GetData().GetTablePath();
  return true;
}

void Catalog::SetDataFile(string &_table, string &_path) {
  KeyString table_name(_table);
  if (schema_data_->IsThere(table_name) == 1) {
    auto schema_ = schema_data_->Find(table_name);
    schema_.GetData().SetTablePath(_path);
  }
}

bool Catalog::GetNoDistinct(string &_table, string &_attribute,
                            unsigned int &_noDistinct) {
  KeyString table_name(_table);
  if (schema_data_->IsThere(table_name) == 0)
    return false;
  auto schema_ = schema_data_->Find(table_name);
  int tuple_no = schema_.GetData().GetDistincts(_attribute);
  if (tuple_no == -1)
    return false;
  _noDistinct = (unsigned int)tuple_no; // TODO: initial type mismatch
  return true;
}
void Catalog::SetNoDistinct(string &_table, string &_attribute,
                            unsigned int &_noDistinct) {
  KeyString table_name(_table);
  if (schema_data_->IsThere(table_name) == 1) {
    auto schema_ = schema_data_->Find(table_name);
    schema_.GetData().GetDistincts(_attribute);
  }
}

bool Catalog::GetAttributeType(string &_table, string &_attribute,
                               string &_attrType) {
  KeyString table_name(_table);
  if (schema_data_->IsThere(table_name) == 0)
    return false;
  auto schema_ = schema_data_->Find(table_name);
  int attr_idx = schema_.GetData().Index(_attribute);
  if (attr_idx == -1)
    return false;

  Type att_type = schema_.GetData().FindType(_attribute);
  if (att_type == Integer)
    _attrType = "INTEGER";
  else if (att_type == Float)
    _attrType = "FLOAT";
  else if (att_type == String)
    _attrType = "STRING";
  else
    _attrType = "NONE";

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
  auto schema_ = schema_data_->Find(table_name);
  auto attrs = schema_.GetData().GetAtts();
  for (auto it : attrs)
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
  vector<unsigned int> distinct_values(_attributes.size(), 0);
  Schema table_schema(_attributes, _attributeTypes, distinct_values);
  ComplexSwapify<Schema> table_info(table_schema);
  unsigned int default_value = 0;
  string default_path = "NO PATH";
  table_info.GetData().SetTuplesNumber(default_value);
  table_info.GetData().SetTablePath(default_path);
  schema_data_->Insert(table_data, table_info);

  return schema_data_->IsThere(table_data) != 0;
}

bool Catalog::DropTable(string &_table) {
  KeyString table_info(_table);
  KeyString table_name("");
  vector<string> attributes, attr_types;
  vector<unsigned int> distinct_values;
  Schema table_schema(attributes, attr_types, distinct_values);
  ComplexSwapify<Schema> table_data(table_schema);
  return schema_data_->Remove(table_info, table_name, table_data) != 0;
}

ostream &operator<<(ostream &_os, Catalog &_c) {

  vector<string> tables, table_attributes;
  Schema table_schema;
  unsigned int tuple_no, dist_no;
  string attr_type, table_path;

  _c.GetTables(tables);
  for (auto &table : tables) {
    cout << "table_name: " << table;

    if (!_c.GetSchema(table, table_schema)) {
      cout << "; NO SCHEMA." << endl;
      continue;
    }

    if (_c.GetNoTuples(table, tuple_no))
      cout << "; number of tuples: " << tuple_no;
    else
      cout << "; number of tuples: ZERO";

    if (_c.GetDataFile(table, table_path))
      cout << "; path: " << table_path << endl;
    else
      cout << "; path: NO PATH" << endl;

    cout << "attributes:";
    if (!_c.GetAttributes(table, table_attributes) ||
        table_attributes.empty()) {
      cout << " NO ATTRIBUTES" << endl;
      continue;
    }
    cout << endl;

    for (auto attr : table_attributes) {
      cout << "\tname: " << attr;

      if (_c.GetAttributeType(table, attr, attr_type))
        cout << "; type: " << attr_type;
      else
        cout << "; type: NO TYPE";

      if (_c.GetNoDistinct(table, attr, dist_no))
        cout << "; distinct values: " << dist_no << endl;
      else
        cout << "; distinct values: ZERO" << endl;

      table_attributes.clear();
    }
  }

  return _os;
}

void Catalog::UploadSchemas() {

  if (!connection_status) { // TODO: sqlite3_db_status(catalog_db) != SQLITE_OK
    cout << "database is not connected." << endl;
  } else {

    schema_data_ = new SchemaMap(); // TODO: avoid NEW

    query = "SELECT * FROM " DB_TABLE_LIST ";";
    sqlite3_prepare_v2(catalog_db, query.c_str(), -1, &stmt, nullptr);

    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
      string table_name =
          string(reinterpret_cast<const char *>(sqlite3_column_text(stmt, 0)));
      KeyString table_data(table_name);

      vector<string> attributes, attr_types;
      vector<unsigned int> distinct_values;

      string inner_query = "SELECT " + table_attr_col2 +
                           " FROM " DB_TABLE_ATTR_LIST " WHERE " +
                           table_attr_col1 + " = '" + table_name + "';";
      sqlite3_stmt *inner_stmt;
      sqlite3_prepare_v2(catalog_db, inner_query.c_str(), -1, &inner_stmt,
                         nullptr);

      while (sqlite3_step(inner_stmt) == SQLITE_ROW) {
        string attr_name = string(
            reinterpret_cast<const char *>(sqlite3_column_text(inner_stmt, 0)));

        string temp_query = "SELECT " + attr_col2 + ", " + attr_col3 +
                            " FROM " DB_ATTRIBUTE_LIST " WHERE " + attr_col1 +
                            " = '" + attr_name + "';";
        sqlite3_stmt *temp_stmt;
        sqlite3_prepare_v2(catalog_db, temp_query.c_str(), -1, &temp_stmt,
                           nullptr);

        while (sqlite3_step(temp_stmt) == SQLITE_ROW) {
          string attr_type =
              string(reinterpret_cast<const char *>(sqlite3_column_text(
                  temp_stmt, 0))); // TYPES: INTEGER, FLOAT, STRING
          auto dist_no = (unsigned int)sqlite3_column_int(
              temp_stmt, 1); // TODO: extra work
          attributes.push_back(attr_name);
          attr_types.push_back(attr_type);
          distinct_values.push_back(dist_no);
          break;
        }

        sqlite3_finalize(temp_stmt);
      }

      Schema table_schema(attributes, attr_types, distinct_values);
      sqlite3_finalize(inner_stmt);

      auto tuple_no = (unsigned int)sqlite3_column_int(stmt, 1);
      string table_path =
          string(reinterpret_cast<const char *>(sqlite3_column_text(stmt, 2)));

      ComplexSwapify<Schema> table_info(table_schema);
      table_info.GetData().SetTuplesNumber(tuple_no); // TODO: use SetNoTuples
      table_info.GetData().SetTablePath(table_path);  // TODO: use SetDataFile

      schema_data_->Insert(table_data, table_info);
      // SetNoTuples(table_name, tuple_no);
      // SetDataFile(table_name, table_path);
    }

    sqlite3_finalize(stmt);
  }
}