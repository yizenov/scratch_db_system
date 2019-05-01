#ifndef _REL_OP_H
#define _REL_OP_H

//TODO: replace all with forward declaration
//#include "Schema.h"
#include "DBFile.h"
#include "Function.h"
#include "Comparison.h"

#include "Keyify.h"
#include "ComplexKeyify.h"
#include "Swapify.h"
#include "InefficientMap.h"

#include <iostream>
#include <fstream>

using std::string;
using std::ostream;

class Record;
//class DBFile; //TODO:use smart pointers
class Schema;

class RelationalOp {
protected:
	// the number of pages that can be used by the operator in execution
	int noPages;
public:
	// empty constructor & destructor
	RelationalOp() : noPages(-1) {}
	virtual ~RelationalOp() {}

	// set the number of pages the operator can use
	void SetNoPages(int _noPages) {noPages = _noPages;}

	// every operator has to implement this method
	virtual bool GetNext(Record& _record) = 0;

	void Swap(RelationalOp& _other);
	virtual Schema& GetSchemaOut() = 0;

	/* Virtual function for polymorphic printing using operator<<.
	 * Each operator has to implement its specific version of print.
	 */
    virtual ostream& print(ostream& _os) = 0;

    /* Overload operator<< for printing.
     */
    friend ostream& operator<<(ostream& _os, RelationalOp& _op);
};

class Scan : public RelationalOp {
private:
	// schema of records in operator
	Schema schema;

	// physical file where data to be scanned are stored
	DBFile file;

public:
	Scan() {}
	Scan(Schema& _schema, DBFile& _file);
	virtual ~Scan();

	int GetNoPages() { return noPages; }

	virtual bool GetNext(Record& _record);

	Schema& GetSchemaOut() { return schema; }

	void Swap(Scan& _other);

	virtual ostream& print(ostream& _os);
};

class Select : public RelationalOp {
private:
	// schema of records in operator
	Schema schema;

	// selection predicate in conjunctive normal form
	CNF predicate;
	// constant values for attributes in predicate
	Record constants;

	// operator generating data
	RelationalOp* producer;

public:
    Select() {}
	Select(Schema& _schema, CNF& _predicate, Record& _constants,
		RelationalOp* _producer);
	virtual ~Select();

	virtual bool GetNext(Record& _record);

	Schema& GetSchemaOut() { return schema; }

	void Swap(Select& _other);

	virtual ostream& print(ostream& _os);
};

class Project : public RelationalOp {
private:
	// schema of records input to operator
	Schema schemaIn;
	// schema of records output by operator
	Schema schemaOut;

	// number of attributes in input records
	int numAttsInput;
	// number of attributes in output records
	int numAttsOutput;
	// index of records from input to keep in output
	// size given by numAttsOutput
	int* keepMe;

	// operator generating data
	RelationalOp* producer;

public:
    Project() {}
	Project(Schema& _schemaIn, Schema& _schemaOut, int _numAttsInput,
		int _numAttsOutput, int* _keepMe, RelationalOp* _producer);
	virtual ~Project();

	virtual bool GetNext(Record& _record);

	int* GetKeepAtts() { return keepMe; }
	int GetNumAttsOutput() { return numAttsOutput; }
	Schema& GetSchemaOut() { return schemaOut; }
    Schema& GetSchemaIn() { return schemaIn; }
	void SetProducer(RelationalOp* _producer) { producer = _producer; }

	void Swap(Project& _other);

	virtual ostream& print(ostream& _os);
};

class Join : public RelationalOp {
private:
	// schema of records in left operand
	Schema schemaLeft;
	// schema of records in right operand
	Schema schemaRight;
	// schema of records output by operator
	Schema schemaOut;

	// selection predicate in conjunctive normal form
	CNF predicate;

	// defining a type of join algorithm
	JoinType joinType;

	// operators generating data
	RelationalOp* left;
	RelationalOp* right;

    virtual bool NLJoin(Record& _origin_record);
    virtual bool HJoin(Record& _origin_record);
    virtual bool SHJoin(Record& _origin_record);

    // nested-loop data
    bool isInnerTableExists; // indicates if smaller table is loaded into main memory
    TwoWayList<Record> innerTable;
    int smallerSide; // indicates which side is inner, 0 is left and 1 is right side
    Record currentOuterRecord;
    RelationalOp* outerSide;
	int *attributes;
	int rightSideIndexStart;  // start index of right side schema

	// hash-join data
    // value: 0 means record has not been used, -1 - record has already been used
    InefficientMap<ComplexKeyify<Record>, SwapInt> innerHashedRecords;
    TwoWayList<Record> usedRecords;
    OrderMaker compareRecords; // for comparing two records
    bool isNextTupleNeeded;
    Schema compare_schema;
    int *join_attributes;

public:
    Join() {}
	Join(Schema& _schemaLeft, Schema& _schemaRight, Schema& _schemaOut,
		CNF& _predicate, RelationalOp* _left, RelationalOp* _right);
	virtual ~Join();

	virtual bool GetNext(Record& _record);

	void Swap(Join& _other);

	Schema& GetSchemaOut() { return schemaOut; }

	virtual ostream& print(ostream& _os);
};

class DuplicateRemoval : public RelationalOp {
private:
	// schema of records in operator
	Schema schema;

	// operator generating data
	RelationalOp* producer;

	// for comparing two records
    OrderMaker compareRecords;

	// for maintaining distinct key values
    InefficientMap<ComplexKeyify<Record>, SwapInt> distinctRecords;

public:
    DuplicateRemoval() {}
	DuplicateRemoval(Schema& _schema, RelationalOp* _producer, OrderMaker& _compareRecords);
	virtual ~DuplicateRemoval();

	virtual bool GetNext(Record& _record);

	void Swap(DuplicateRemoval& _other);

    Schema& GetSchemaOut() { return schema; }

	virtual ostream& print(ostream& _os);
};

class Sum : public RelationalOp {
private:
	// schema of records input to operator
	Schema schemaIn;
	// schema of records output by operator
	Schema schemaOut;

	// function to compute
	Function compute;

	// flag to detect if sum is computed
	bool isComputed;

	// operator generating data
	RelationalOp* producer;

public:
    Sum() {}
	Sum(Schema& _schemaIn, Schema& _schemaOut, Function& _compute,
		RelationalOp* _producer);
	virtual ~Sum();

	virtual bool GetNext(Record& _record);

	void Swap(Sum& _other);

    Schema& GetSchemaOut() { return schemaOut; }
    void SetProducer(RelationalOp* _producer) { producer = _producer; }

	virtual ostream& print(ostream& _os);
};

class GroupBy : public RelationalOp {
private:
	// schema of records input to operator
	Schema schemaIn;
	// schema of records output by operator
	Schema schemaOut;
    Schema schemaOriginOut;

	// grouping attributes
	OrderMaker groupingAtts;
	// function to compute
	Function compute;

	// operator generating data
	RelationalOp* producer;

	//TODO: handles only double counters
    // for maintaining distinct key values
    InefficientMap<ComplexKeyify<Record>, SwapDouble> recordStats;

    // flag to detect if sum is computed
    bool isComputed;

public:
    GroupBy() {}
	GroupBy(Schema& _schemaIn, Schema& _schemaOut, OrderMaker& _groupingAtts,
		Function& _compute,	RelationalOp* _producer);
	virtual ~GroupBy();

	virtual bool GetNext(Record& _record);

	void Swap(GroupBy& _other);

    Schema& GetSchemaOut() { return schemaOut; }

	virtual ostream& print(ostream& _os);
};

class WriteOut : public RelationalOp {
private:
	// schema of records in operator
	Schema schema;

	// output file where to write the result records
	string outFile;
    std::ofstream outStream; // for outputting the query results

	// operator generating data
	RelationalOp* producer;

public:
    WriteOut() {}
	WriteOut(Schema& _schema, string& _outFile, RelationalOp* _producer);
	virtual ~WriteOut();

	virtual bool GetNext(Record& _record);

	void Swap(WriteOut& _other);

    Schema& GetSchemaOut() { return schema; }

	virtual ostream& print(ostream& _os);
};


class QueryExecutionTree {
private:
	RelationalOp* root;

public:
	QueryExecutionTree() {}
	virtual ~QueryExecutionTree() {}

	void ExecuteQuery();
	void SetRoot(RelationalOp& _root);

    friend ostream& operator<<(ostream& _os, QueryExecutionTree& _op);
};

#endif //_REL_OP_H
