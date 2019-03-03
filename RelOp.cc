

#include "RelOp.h"

using namespace std;

ostream& operator<<(ostream& _os, RelationalOp& _op) {
	return _op.print(_os);
}


Scan::Scan(Schema& _schema, DBFile& _file) {

}

Scan::~Scan() {

}

ostream& Scan::print(ostream& _os) {
    return _os << "SCAN";
}

void Scan::Swap(Scan &_other) {
    SWAP(noPages, _other.noPages);
    OBJ_SWAP(schema, _other.schema);
    OBJ_SWAP(file, _other.file);
}


Select::Select(Schema& _schema, CNF& _predicate, Record& _constants,
	RelationalOp* _producer) {

}

Select::~Select() {

}

ostream& Select::print(ostream& _os) {
	return _os << "SELECT";
}

void Select::Swap(Select &_other) {
    SWAP(noPages, _other.noPages);
    OBJ_SWAP(schema, _other.schema);
    OBJ_SWAP(predicate, _other.predicate);
    OBJ_SWAP(constants, _other.constants);
    SWAP(producer, _other.producer);
}


Project::Project(Schema& _schemaIn, Schema& _schemaOut, int _numAttsInput,
	int _numAttsOutput, int* _keepMe, RelationalOp* _producer) {

}

Project::~Project() {

}

ostream& Project::print(ostream& _os) {
	return _os << "PROJECT";
}

void Project::Swap(Project &_other) {
	SWAP(noPages, _other.noPages);
	OBJ_SWAP(schemaIn, _other.schemaIn);
	OBJ_SWAP(schemaOut, _other.schemaOut);
	SWAP(numAttsInput, _other.numAttsInput);
	SWAP(numAttsOutput, _other.numAttsOutput);
	SWAP(keepMe, _other.keepMe);
	SWAP(producer, _other.producer);
}

Join::Join(Schema& _schemaLeft, Schema& _schemaRight, Schema& _schemaOut,
	CNF& _predicate, RelationalOp* _left, RelationalOp* _right) {

}

Join::~Join() {

}

ostream& Join::print(ostream& _os) {
	return _os << "JOIN";
}

void Join::Swap(Join &_other) {
    SWAP(noPages, _other.noPages);
    OBJ_SWAP(schemaLeft, _other.schemaLeft);
    OBJ_SWAP(schemaRight, _other.schemaRight);
    OBJ_SWAP(predicate, _other.predicate);
    SWAP(left, _other.left);
    SWAP(right, _other.right);
}

DuplicateRemoval::DuplicateRemoval(Schema& _schema, RelationalOp* _producer) {

}

DuplicateRemoval::~DuplicateRemoval() {

}

ostream& DuplicateRemoval::print(ostream& _os) {
	return _os << "DISTINCT";
}

void DuplicateRemoval::Swap(DuplicateRemoval &_other) {
    SWAP(noPages, _other.noPages);
    OBJ_SWAP(schema, _other.schema);
    SWAP(producer, _other.producer);
}

Sum::Sum(Schema& _schemaIn, Schema& _schemaOut, Function& _compute,
	RelationalOp* _producer) {

}

Sum::~Sum() {

}

ostream& Sum::print(ostream& _os) {
	return _os << "SUM";
}

void Sum::Swap(Sum &_other) {
    SWAP(noPages, _other.noPages);
    OBJ_SWAP(schemaIn, _other.schemaIn);
    OBJ_SWAP(schemaOut, _other.schemaOut);
    OBJ_SWAP(compute, _other.compute);
    SWAP(producer, _other.producer);
}

GroupBy::GroupBy(Schema& _schemaIn, Schema& _schemaOut, OrderMaker& _groupingAtts,
	Function& _compute,	RelationalOp* _producer) {

}

GroupBy::~GroupBy() {

}

ostream& GroupBy::print(ostream& _os) {
	return _os << "GROUP BY";
}

void GroupBy::Swap(GroupBy &_other) {
    SWAP(noPages, _other.noPages);
    OBJ_SWAP(schemaIn, _other.schemaIn);
    OBJ_SWAP(schemaOut, _other.schemaOut);
    OBJ_SWAP(groupingAtts, _other.groupingAtts);
    OBJ_SWAP(compute, _other.compute);
    SWAP(producer, _other.producer);
}


WriteOut::WriteOut(Schema& _schema, string& _outFile, RelationalOp* _producer) {

}

WriteOut::~WriteOut() {

}

ostream& WriteOut::print(ostream& _os) {
	return _os << "OUTPUT";
}

void WriteOut::Swap(WriteOut &_other) {
    SWAP(noPages, _other.noPages);
    OBJ_SWAP(schema, _other.schema);
    STL_SWAP(outFile, _other.outFile);
    SWAP(producer, _other.producer);
}


ostream& operator<<(ostream& _os, QueryExecutionTree& _op) {
	return _os << "QUERY EXECUTION TREE";
}
