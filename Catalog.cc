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
    sqlite3_close(catalog_db);
	return true;
}

bool Catalog::GetNoTuples(string& _table, unsigned int& _noTuples) {
	return true;
}

void Catalog::SetNoTuples(string& _table, unsigned int& _noTuples) {
}

bool Catalog::GetDataFile(string& _table, string& _path) {
	return true;
}

void Catalog::SetDataFile(string& _table, string& _path) {
}

bool Catalog::GetNoDistinct(string& _table, string& _attribute,
	unsigned int& _noDistinct) {
	return true;
}
void Catalog::SetNoDistinct(string& _table, string& _attribute,
	unsigned int& _noDistinct) {
}

void Catalog::GetTables(vector<string>& _tables) {
	schema_data_->MoveToStart();
	while(!schema_data_->AtEnd()) {
		_tables.push_back(schema_data_->CurrentKey());
		schema_data_->Advance();
	}
}

bool Catalog::GetAttributes(string& _table, vector<string>& _attributes) {
	return true;
}

bool Catalog::GetSchema(string& _table, Schema& _schema) {

	return true;
}

bool Catalog::CreateTable(string& _table, vector<string>& _attributes,
	vector<string>& _attributeTypes) {
	return true;
}

bool Catalog::DropTable(string& _table) {
	return true;
}

ostream& operator<<(ostream& _os, Catalog& _c) {
	return _os;
}

void Catalog::UploadSchemas() {
	schema_data_ = new SchemaMap(); // TODO: avoid NEW

    query = "SELECT * FROM " DB_TABLE_LIST ";";

    sqlite3_prepare_v2(catalog_db, query.c_str(), -1, &stmt, nullptr);
    string table_path;
    int tuple_no;
    while((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        KeyString table_name(string(reinterpret_cast<const char *>(sqlite3_column_text(stmt, 0))));
        Schema table_schema;
        KeySchema table_info(table_schema);
        schema_data_->Insert(table_name, table_info);

        tuple_no = sqlite3_column_int(stmt, 1);
        table_path = string(reinterpret_cast<const char *>(sqlite3_column_text(stmt, 2)));
    }

    //TODO: closing query
}