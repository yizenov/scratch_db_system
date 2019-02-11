#include <iostream>
#include "sqlite3.h"
#include <fstream>

#include "Schema.h"
#include "Catalog.h"

using namespace std;


Catalog::Catalog(string& _fileName) {

	ifstream input_file(_fileName);
	if (input_file.fail()) {
		connection_status = false;
	} else {
        connection_status = sqlite3_open(_fileName.c_str(), &catalog_db) == SQLITE_OK;
	}
}

Catalog::~Catalog() {
	// TODO
}

bool Catalog::Save() {
    sqlite3_close(catalog_db); // TODO:
	return true;
}

bool Catalog::GetNoTuples(string& _table, unsigned int& _noTuples) {
	KeyString table_name(_table);
	auto schema_ = schema_data_->Find(table_name);
	if (schema_data_->AtEnd())  // TODO: verify
		return false;
	_noTuples = schema_.getData().GetTuplesNumber();
	return true;
}

void Catalog::SetNoTuples(string& _table, unsigned int& _noTuples) {
	KeyString table_name(_table);
	auto schema_ = schema_data_->Find(table_name);
	if (!schema_data_->AtEnd())  // TODO: verify
		schema_.getData().SetTuplesNumber(_noTuples);
}

bool Catalog::GetDataFile(string& _table, string& _path) {
	KeyString table_name(_table);
	auto schema_ = schema_data_->Find(table_name);
	if (schema_data_->AtEnd())  // TODO: verify
		return false;
	_path = schema_.getData().GetTablePath();
	return true;
}

void Catalog::SetDataFile(string& _table, string& _path) {
	KeyString table_name(_table);
	auto schema_ = schema_data_->Find(table_name);
	if (!schema_data_->AtEnd())  // TODO: verify
		schema_.getData().SetTablePath(_path);
}

bool Catalog::GetNoDistinct(string& _table, string& _attribute,
	unsigned int& _noDistinct) {
	KeyString table_name(_table);
	auto schema_ = schema_data_->Find(table_name);
	if (schema_data_->AtEnd())  // TODO: verify
		return false;
	int tuple_no = schema_.getData().GetDistincts(_attribute);
	if (tuple_no == -1)
		return false;
	_noDistinct = (unsigned int) tuple_no;  //TODO: initial type mismatch
	return true;
}
void Catalog::SetNoDistinct(string& _table, string& _attribute,
	unsigned int& _noDistinct) {
	KeyString table_name(_table);
	auto schema_ = schema_data_->Find(table_name);
	if (!schema_data_->AtEnd())  // TODO: verify
		schema_.getData().GetDistincts(_attribute);
}

void Catalog::GetTables(vector<string>& _tables) {
	schema_data_->MoveToStart();
	while(!schema_data_->AtEnd()) {
		_tables.push_back(schema_data_->CurrentKey());
		schema_data_->Advance();
	}
	//schema_data_->MoveToStart();
}

bool Catalog::GetAttributes(string& _table, vector<string>& _attributes) {
	KeyString table_name(_table);
	auto schema_ = schema_data_->Find(table_name);
	if (schema_data_->AtEnd())  // TODO: verify
		return false;
	auto attrs = schema_.getData().GetAtts();
	for (auto it : attrs)
		_attributes.push_back(it.name);
	return true;
}

bool Catalog::GetSchema(string& _table, Schema& _schema) {
	KeyString table_name(_table);
	auto schema_ = schema_data_->Find(table_name);
	if (schema_data_->AtEnd())  // TODO: verify
		return false;
	_schema = schema_;
	return true;
}

bool Catalog::CreateTable(string& _table, vector<string>& _attributes,
	vector<string>& _attributeTypes) {

	query = "CREATE TABLE " + _table + " (";
	for (auto i = 0; i < _attributes.size(); i++) {
		query += _attributes[i] + " " + _attributeTypes[i] + ", ";
	}
	query += ");"; //TODO: PK-FK, NOT NULL, etc.
	sqlite3_prepare_v2(catalog_db, query.c_str(), -1, &stmt, nullptr);

	if(sqlite3_step(stmt) != SQLITE_DONE) {
		return false;
	}
	sqlite3_finalize(stmt); //TODO: ???
	return true;
}

bool Catalog::DropTable(string& _table) {
	query = "DROP TABLE IF EXISTS " + _table + ";";
	sqlite3_prepare_v2(catalog_db, query.c_str(), -1, &stmt, nullptr);
	if(sqlite3_step(stmt) != SQLITE_DONE) {
		return false;
	}
	sqlite3_finalize(stmt);
	//TODO: need to do in memory and save to db in Save function.
	return true;
}

ostream& operator<<(ostream& _os, Catalog& _c) {
	//TODO
	return _os;
}

void Catalog::UploadSchemas() {
	schema_data_ = new SchemaMap(); // TODO: avoid NEW

    query = "SELECT * FROM " DB_TABLE_LIST ";";
    sqlite3_prepare_v2(catalog_db, query.c_str(), -1, &stmt, nullptr);

    while((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        KeyString table_name(string(reinterpret_cast<const char *>(sqlite3_column_text(stmt, 0))));
        vector<string> attributes, attr_types;
        vector<unsigned int> distinct_vals;
        Schema table_schema(attributes, attr_types, distinct_vals);

		unsigned int tuple_no = (unsigned int) sqlite3_column_int(stmt, 1);
		string table_path = string(reinterpret_cast<const char *>(sqlite3_column_text(stmt, 2)));

		ComplexSwapify<Schema> table_info(table_schema);
		table_info.getData().SetTuplesNumber(tuple_no);
		table_info.getData().SetTablePath(table_path);

        schema_data_->Insert(table_name, table_info);
    }

    //TODO: closing query
}