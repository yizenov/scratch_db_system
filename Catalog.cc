
#include "Catalog.h"

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
      // schema.page_size=16384",NULL,NULL,NULL);
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
    sqlite3_exec(catalog_db, "BEGIN TRANSACTION", NULL, NULL, &error_msg);

    // dropping in-memory deleted tables
    query = "SELECT " + table_col1 + " FROM " DB_TABLE_LIST ";";
    sqlite3_prepare_v2(catalog_db, query.c_str(), -1, &stmt, nullptr);
    set<string> existing_tables;

    sqlite3_stmt *inner_stmt_one;
    string inner_query_one =
        "DELETE FROM " DB_TABLE_ATTR_LIST " WHERE " + table_attr_col1 + " = ?1;";
    //sqlite3_prepare_v2(catalog_db, inner_query_one.c_str(), -1, &inner_stmt_one, nullptr);

    sqlite3_stmt *inner_stmt_two;
    string inner_query_two =
        "DELETE FROM " DB_TABLE_LIST " WHERE " + table_col1 + " = ?1;";
    sqlite3_prepare_v2(catalog_db, inner_query_two.c_str(), -1,
    &inner_stmt_two, nullptr);

    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
      string table_name =
          string(reinterpret_cast<const char *>(sqlite3_column_text(stmt,
          0)));
      KeyString table_data(table_name);
      Schema table_schema = schema_data_->CurrentData().GetData();
      if (schema_data_->IsThere(table_data) == 0) { // ||
        // table_schema.GetSchemaStatus()) { // if row is not present in memory or
        // has been updated

          sqlite3_prepare_v2(catalog_db, inner_query_one.c_str(), -1, &inner_stmt_one, nullptr);
        sqlite3_bind_text(inner_stmt_one, 1, table_name.c_str(), -1, nullptr);
        rc = sqlite3_step(inner_stmt_one);
        if (isItError(rc)) {
          cout << sqlite3_errmsg(catalog_db) << endl;
          cout << "pairs where " << table_attr_col1 << " = " << table_name
               << ") weren't deleted in the database." << endl;
          sqlite3_finalize(inner_stmt_one);
          return false;
        }
        sqlite3_finalize(inner_stmt_one);

        sqlite3_bind_text(inner_stmt_two, 1, table_name.c_str(), -1, nullptr);
        rc = sqlite3_step(inner_stmt_two);
        if (isItError(rc) != SQLITE_DONE) {
          cout << sqlite3_errmsg(catalog_db) << endl;
          cout << "table: " << table_name << " wasn't deleted in the database." << endl;
          sqlite3_finalize(inner_stmt_two);
          return false;
        }
        sqlite3_finalize(inner_stmt_two);

      } else {
        // existing tables in database
        existing_tables.insert(table_name);
      }
    }
    sqlite3_finalize(stmt);

    sqlite3_exec(catalog_db, "END TRANSACTION", NULL, NULL, &error_msg);

    sqlite3_exec(catalog_db, "BEGIN TRANSACTION", NULL, NULL, &error_msg);

  string attr_query = "INSERT INTO " DB_ATTRIBUTE_LIST " (" + attr_col1 + "," +
                      attr_col2 + "," + attr_col3 + ") VALUES (?1,?2,?3);";
  sqlite3_stmt *stmt_attr;
  sqlite3_prepare_v2(catalog_db, attr_query.c_str(), -1, &stmt_attr, nullptr);

  string table_attr_query = "INSERT INTO " DB_TABLE_ATTR_LIST " (" +
                            table_attr_col1 + "," + table_attr_col2 +
                            ") VALUES (?1,?2);";
  sqlite3_stmt *stmt_table_attr;
   sqlite3_prepare_v2(catalog_db, table_attr_query.c_str(), -1, &stmt_table_attr, nullptr);

  string table_query = "INSERT INTO " DB_TABLE_LIST " (" + table_col1 + "," +
                       table_col2 + "," + table_col3 + ") VALUES (?1,?2,?3);";
  sqlite3_stmt *stmt_table;
   sqlite3_prepare_v2(catalog_db, table_query.c_str(), -1, &stmt_table, nullptr);

  // creating tables in the database
  schema_data_->MoveToStart();
  while (!schema_data_->AtEnd()) {
    string table_name = schema_data_->CurrentKey();
    Schema *table_schema = &schema_data_->CurrentData().GetData();
    // Schema table_schema = schema_data_->CurrentData().GetData();

    auto it = existing_tables.find(table_name);
    if (it != existing_tables.end()) {
      schema_data_->Advance(); // skips existing and unchanged tables in db.
      continue;
    }

          //sqlite3_prepare_v2(catalog_db, table_query.c_str(), -1, &stmt_table, nullptr);
      sqlite3_bind_text(stmt_table, 1, table_name.c_str(), -1, SQLITE_STATIC);
      sqlite3_bind_int(stmt_table, 2, table_schema->GetTuplesNumber());
      sqlite3_bind_text(stmt_table, 3, table_schema->GetTablePath().c_str(), -1, SQLITE_STATIC);
      rc = sqlite3_step(stmt_table);
      if (isItError(rc)) {
          cout << sqlite3_errmsg(catalog_db) << endl;
          cout << "table: " << table_name << " wasn't inserted in the database."
               << endl;
          sqlite3_finalize(stmt_table);
          return false;
      }
      //sqlite3_finalize(stmt_table);
      sqlite3_clear_bindings(stmt_table);
      sqlite3_reset(stmt_table);

    // auto vec = table_schema.GetAtts();
    for (Attribute &att : table_schema->GetAtts()) {

      string attr_type = "Unknown";
      if (att.type == Integer)
        attr_type = "Integer"; //"INTEGER";
      else if (att.type == Float)
        attr_type = "Float"; //"FLOAT";
      else if (att.type == String)
        attr_type = "String"; //"STRING";

      //sqlite3_prepare_v2(catalog_db, attr_query.c_str(), -1, &stmt_attr, nullptr);
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
      //sqlite3_finalize(stmt_attr);
        sqlite3_clear_bindings(stmt_attr);
        sqlite3_reset(stmt_attr);

//      sqlite3_prepare_v2(catalog_db, table_attr_query.c_str(), -1,
//                         &stmt_table_attr, nullptr);
      sqlite3_bind_text(stmt_table_attr, 1, table_name.c_str(), -1, nullptr);
      sqlite3_bind_text(stmt_table_attr, 2, att.name.c_str(), -1, nullptr);
      rc = sqlite3_step(stmt_table_attr);
      if (isItError(rc)) {
        cout << sqlite3_errmsg(catalog_db) << endl;
        cout << "pair: (" << table_name << "," << att.name
             << ") wasn't inserted in the database." << endl;
        sqlite3_finalize(stmt_table_attr);
        return false;
      }
      //sqlite3_finalize(stmt_table_attr);
        sqlite3_clear_bindings(stmt_table_attr);
        sqlite3_reset(stmt_table_attr);
    }

    // TODO: roll-back if failure happens

    schema_data_->Advance();
  }

    sqlite3_exec(catalog_db, "END TRANSACTION", NULL, NULL, &error_msg);

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
    schema_.GetData().SetTuplesNumber(_noTuples);
    schema_data_->Find(table_name).Swap(schema_);
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
    schema_data_->Find(table_name).Swap(schema_);
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
    schema_.GetData().SetDistincts(_attribute, _noDistinct);
    schema_data_->Find(table_name).Swap(schema_);
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
  if (schema_data_->IsThere(table_data) != 0)
    return false;
  vector<unsigned int> distinct_values(_attributes.size(), 0);
  Schema table_schema(_attributes, _attributeTypes, distinct_values);
  ComplexSwapify<Schema> table_info(table_schema);
  //  unsigned int default_value = 0;
  //  string default_path = "NO PATH";
  //  table_info.GetData().SetTuplesNumber(default_value);
  //  table_info.GetData().SetTablePath(default_path);
  schema_data_->Insert(table_data, table_info);

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
  ComplexSwapify<Schema> table_data(table_schema);
  return schema_data_->Remove(table_info, table_name, table_data) != 0;
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

    schema_data_ = new SchemaMap(); // TODO: avoid NEW

    query = "SELECT * FROM " DB_TABLE_LIST ";";
    sqlite3_prepare_v2(catalog_db, query.c_str(), -1, &stmt, nullptr);

    string inner_query_one = "SELECT " + table_attr_col2 +
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

      sqlite3_bind_text(inner_stmt_one, 1, table_name.c_str(), -1, NULL);
      while (sqlite3_step(inner_stmt_one) == SQLITE_ROW) {
        string attr_name = string(reinterpret_cast<const char *>(
            sqlite3_column_text(inner_stmt_one, 0)));

        sqlite3_bind_text(inner_stmt_two, 1, attr_name.c_str(), -1, NULL);
        while (sqlite3_step(inner_stmt_two) == SQLITE_ROW) {
          string attr_type =
              string(reinterpret_cast<const char *>(sqlite3_column_text(
                  inner_stmt_two, 0))); // TYPES: INTEGER, FLOAT, STRING
          auto dist_no = (unsigned int)sqlite3_column_int(
              inner_stmt_two, 1); // TODO: extra work
          attributes.push_back(attr_name);
          attr_types.push_back(attr_type);
          distinct_values.push_back(dist_no);
          break;
        }

        // sqlite3_finalize(inner_stmt_two);
      }

      Schema table_schema(attributes, attr_types, distinct_values);
      // sqlite3_finalize(inner_stmt_one);

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