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
		sqlite3 *catalog_db;
        connection_status = sqlite3_open(_fileName.c_str(), &catalog_db) == SQLITE_OK;
		sqlite3_close(catalog_db);
	}
}

Catalog::~Catalog() {
}

bool Catalog::Save() {

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
	schema_data_ = new SchemaMap();

    //  vector<unsigned int> distincts;
    //	Schema s(attributes, types, distincts);
    //	Schema s1(s), s2; s2 = s1;
    //	cout << s << endl;
    //	cout << s1 << endl;
    //	cout << s2 << endl;
}