

#include "Schema.h"

#include "Swap.h"

#include <iostream>

using namespace std;

Attribute::Attribute() : name(""), type(Name), noDistinct(0) {}

Attribute::Attribute(const Attribute& _other) :
	name(_other.name), type(_other.type), noDistinct(_other.noDistinct) {}

Attribute& Attribute::operator=(const Attribute& _other) {
	// handle self-assignment first
	if (this == &_other) return *this;

	name = _other.name;
	type = _other.type;
	noDistinct = _other.noDistinct;

	return *this;
}

void Attribute::Swap(Attribute& _other) {
	STL_SWAP(name, _other.name);
	SWAP(type, _other.type);
	SWAP(noDistinct, _other.noDistinct);
}


Schema::Schema(vector<string>& _attributes,	vector<string>& _attributeTypes,
	vector<unsigned int>& _distincts) {
	for (int i = 0; i < _attributes.size(); i++) {
		Attribute a;
		a.name = _attributes[i];
		a.noDistinct = _distincts[i];
		if (_attributeTypes[i] == "Integer") a.type = Integer; //INTEGER
		else if (_attributeTypes[i] == "Float") a.type = Float; //FLOAT
		else if (_attributeTypes[i] == "String") a.type = String; //STRING
		else {
            cout << "unsupported attribute type" << endl;
            exit(-1);
        }

		atts.push_back(a);
	}
}

Schema::Schema(const Schema& _other) {
	for (int i = 0; i < _other.atts.size(); i++) {
		Attribute a; a = _other.atts[i];
		atts.push_back(a);
	}
	tuple_no = _other.tuple_no;
	table_path = _other.table_path;
}

Schema& Schema::operator=(const Schema& _other) {
	// handle self-assignment first
	if (this == &_other) return *this;

	for (int i = 0; i < _other.atts.size(); i++) {
		Attribute a; a = _other.atts[i];
		atts.push_back(a);
	}
	tuple_no = _other.tuple_no;
	table_path = _other.table_path;
	isChanged = true;

	return *this;
}

void Schema::Swap(Schema& _other) {
	STL_SWAP(atts, _other.atts);
	SWAP(tuple_no, _other.tuple_no);
	STL_SWAP(table_path, _other.table_path);
	isChanged = true;
}

int Schema::Append(Schema& _other) {
	for (int i = 0; i < _other.atts.size(); i++) {
		int pos = Index(_other.atts[i].name);
		if (pos != -1) return -1;
	}

	for (int i = 0; i < _other.atts.size(); i++) {
		Attribute a; a = _other.atts[i];
		atts.push_back(a);
	}
	SWAP(tuple_no, _other.tuple_no);
	SWAP(table_path, _other.table_path);
	isChanged = true;

	return 0;
}

int Schema::Index(string& _attName) {
	for (int i = 0; i < atts.size(); i++) {
		if (_attName == atts[i].name) return i;
	}

	// if we made it here, the attribute was not found
	return -1;
}

Type Schema::FindType(string& _attName) {
	int pos = Index(_attName);
	if (pos == -1) return Integer;

	return atts[pos].type;
}

int Schema::GetDistincts(string& _attName) {
	int pos = Index(_attName);
	if (pos == -1) return -1;

	return atts[pos].noDistinct;
}

void Schema::SetDistincts(string& _attName, unsigned int& _distNo) {
	int pos = Index(_attName);
	if (pos != -1) {
		SWAP(atts[pos].noDistinct, _distNo);
		isChanged = true;
	}
}

int Schema::RenameAtt(string& _oldName, string& _newName) {
	int pos = Index(_newName);
	if (pos != -1) return -1;

	pos = Index(_oldName);
	if (pos == -1) return -1;


	atts[pos].name = _newName;
	isChanged = true;

	return 0;
}

int Schema::Project(vector<int>& _attsToKeep) {
	int numAttsToKeep = _attsToKeep.size();
	int numAtts = atts.size();
	
	// too many attributes to keep
	if (numAttsToKeep > numAtts) return -1;

	vector<Attribute> copy; atts.swap(copy);

	for (int i=0; i<numAttsToKeep; i++) {
		int index = _attsToKeep[i];
		if ((index >= 0) && (index < numAtts)) {
			Attribute a; a = copy[index];
			atts.push_back(a);
		}
		else {
			atts.swap(copy);
			copy.clear();

			return -1;
		}
	}

	copy.clear();

	return 0;
}

void Schema::SetTuplesNumber(unsigned long int &_tupleNumber) {
	SWAP(tuple_no, _tupleNumber);
	isChanged = true;
}

unsigned long int Schema::GetTuplesNumber() { return tuple_no; }

void Schema::SetTablePath(string& _tablePath) {
	SWAP(table_path, _tablePath);
	isChanged = true;
}

string Schema::GetTablePath() { return table_path; }

bool Schema::GetSchemaStatus() { return isChanged; }

ostream& operator<<(ostream& _os, Schema& _c) {
	_os << "number of tuples: " << _c.tuple_no;
	_os << "; path: " << _c.table_path;
	_os << " ( " << endl;
	for(int i=0; i<_c.atts.size(); i++) {
		_os << _c.atts[i].name << ':';

		switch(_c.atts[i].type) {
			case Integer:
				_os << "Integer"; //INTEGER
				break;
			case Float:
				_os << "Float"; //FLOAT
				break;
			case String:
				_os << "String"; //STRING
				break;
			default:
				_os << "Unknown"; //UNKNOWN
				break;
		}

		_os << " [" << _c.atts[i].noDistinct << "]";
		if (i < _c.atts.size()-1) _os << ", ";
	}
	_os << endl << "\t\t) " << endl;

	return _os;
}
