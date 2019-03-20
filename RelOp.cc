
#include "RelOp.h"

using namespace std;

ostream& operator<<(ostream& _os, RelationalOp& _op) {
	return _op.print(_os);
}

Scan::Scan(Schema& _schema, DBFile& _file) : schema(_schema), file(_file) {}

Scan::~Scan() {
    file.Close();
}

bool Scan::GetNext(Record& _record) {
    int status = file.GetNext(_record);
    return status == 0;
}

ostream& Scan::print(ostream& _os) {
    return _os << "SCAN [ number of tuples: "<< schema.GetTuplesNumber() << " (end of scan operator) ]" << endl;
}

void Scan::Swap(Scan &_other) {
    SWAP(noPages, _other.noPages);
    OBJ_SWAP(schema, _other.schema);
    OBJ_SWAP(file, _other.file);
}

/*
 * if an operand is Literal, then corresponding
 *      whichAtt is the index of the literal in the list of selection predicates.
 */
Select::Select(Schema& _schema, CNF& _predicate, Record& _constants,
	RelationalOp* _producer) : schema(_schema), predicate(_predicate),
	constants(_constants), producer(_producer) {

    if (predicate.numAnds > 0) {

        // estimating the cardinality of a relation
        int denominator_value = 0;

        // extracting first selection predicate in order to initialize denominator value (i.e. avoid zero division)
        if (_predicate.andList[0].operand2 == Literal && _predicate.andList[0].operand1 == Literal) {
            cout << "there may be at most one literal in selection predicate" << endl;
            exit(-1);
        } else if (_predicate.andList[0].operand1 == Left && _predicate.andList[0].operand2 == Left) {
            cout << "Comparing two columns. This is not supported yet." << endl; //TODO:
            exit(-1);
        } else if (_predicate.andList[0].operand2 == Literal) { // if left side of the selection predicate is literal
            CompOperator anOperator = _predicate.andList[0].op;
            string attr_name = schema.GetAtts()[_predicate.andList[0].whichAtt1].name;
            if (anOperator == LessThan || anOperator == GreaterThan) {
                denominator_value = 3;
            } else if (anOperator == Equals) {
                int no_distincts = schema.GetDistincts(attr_name);
                if (no_distincts <= 0) {
                    cout << "Incorrect distinct value" << endl;
                    exit(-1);
                }
                denominator_value = no_distincts;
            } else {
                cout << "Unsupported operator" << endl;
                exit(-1);
            }
        } else if (_predicate.andList[0].operand1 == Literal) { // if right side of the selection predicate is literal
            CompOperator anOperator = _predicate.andList[0].op;
            string attr_name = schema.GetAtts()[_predicate.andList[0].whichAtt2].name;
            if (anOperator == LessThan || anOperator == GreaterThan) {
                denominator_value = 3;
            } else if (anOperator == Equals) {
                int no_distincts = schema.GetDistincts(attr_name);
                if (no_distincts <= 0) {
                    cout << "Incorrect distinct value" << endl;
                    exit(-1);
                }
                denominator_value = no_distincts;
            } else {
                cout << "Unsupported operator" << endl;
                exit(-1);
            }
        } else {
            cout << "Unsupported selection predicate" << endl;
        }

        // processing the rest of the selection predicates
        unsigned counter = 1;
        while (counter < _predicate.numAnds) {
            if (_predicate.andList[counter].operand2 == Literal && _predicate.andList[counter].operand1 == Literal) {
                cout << "there may be at most one literal in selection predicate" << endl;
                exit(-1);
            } else if (_predicate.andList[counter].operand1 == Left && _predicate.andList[counter].operand2 == Left) {
                cout << "Comparing two columns. This is not supported yet." << endl; //TODO:
                exit(-1);
            } else if (_predicate.andList[counter].operand2 == Literal) {
                CompOperator anOperator = _predicate.andList[counter].op;
                string attr_name = schema.GetAtts()[_predicate.andList[counter].whichAtt1].name;
                if (anOperator == LessThan || anOperator == GreaterThan) {
                    denominator_value *= 3;
                } else if (anOperator == Equals) {
                    int no_distincts = schema.GetDistincts(attr_name);
                    if (no_distincts <= 0) {
                        cout << "Incorrect distinct value" << endl;
                        exit(-1);
                    }
                    denominator_value *= no_distincts;
                }
            } else if (_predicate.andList[counter].operand1 == Literal) { // if right side of the selection predicate is literal
                CompOperator anOperator = _predicate.andList[counter].op;
                string attr_name = schema.GetAtts()[_predicate.andList[counter].whichAtt2].name;
                if (anOperator == LessThan || anOperator == GreaterThan) {
                    denominator_value *= 3;
                } else if (anOperator == Equals) {
                    int no_distincts = schema.GetDistincts(attr_name);
                    if (no_distincts <= 0) {
                        cout << "Incorrect distinct value" << endl;
                        exit(-1);
                    }
                    denominator_value *= no_distincts;
                } else {
                    cout << "Unsupported operator" << endl;
                    exit(-1);
                }
            } else {
                cout << "Unsupported selection predicate" << endl;
            }
            counter++;
        }

        unsigned long int cardinality = schema.GetTuplesNumber() / denominator_value;
        schema.SetTuplesNumber(cardinality);
    }
}

Select::~Select() {}

bool Select::GetNext(Record& _record)  {
    while (producer->GetNext(_record)) {
        if (predicate.Run(_record, constants)) {
            return true;
        }
    }
    return false;
}

ostream& Select::print(ostream& _os) {
    _os << "SELECT [ estimated cardinality: " << schema.GetTuplesNumber() << " ]" << endl;
	return _os << "\t\t\t\t\t[ " << *producer << "\t\t\t\t(end of selection operator) ]" << endl;
}

void Select::Swap(Select &_other) {
    SWAP(noPages, _other.noPages);
    OBJ_SWAP(schema, _other.schema);
    OBJ_SWAP(predicate, _other.predicate);
    OBJ_SWAP(constants, _other.constants);
    SWAP(producer, _other.producer);
}

Project::Project(Schema& _schemaIn, Schema& _schemaOut, int _numAttsInput,
	int _numAttsOutput, int* _keepMe, RelationalOp* _producer) :
	schemaIn(_schemaIn), schemaOut(_schemaOut), numAttsInput(_numAttsInput),
	numAttsOutput(_numAttsOutput), keepMe(_keepMe), producer(_producer) {

    //_keepMe - indices of columns that is from joined schema
    vector<int> attr_indices(keepMe, keepMe + numAttsOutput);
    if (schemaOut.Project(attr_indices) == -1) {
        cout << "failed to project attributes" << endl;
        exit(-1);
    }
}

Project::~Project() {}

bool Project::GetNext(Record& _record) {
    if (producer->GetNext(_record)) {
        _record.Project(keepMe, numAttsOutput, numAttsInput);
        return true;
    }
    return false;
}

ostream& Project::print(ostream& _os) {
    _os << "PROJECT [ projected attributes:";
    for (Attribute attr : schemaOut.GetAtts()) {
        _os << " " + attr.name;
    }
    _os << " ]" << endl;
	return _os << "\t\t\t[ " << *producer << "\t\t\t(end of projection operator) ]" << endl;
}

void Project::Swap(Project &_other) {
	SWAP(noPages, _other.noPages);
	OBJ_SWAP(schemaIn, _other.schemaIn);
	OBJ_SWAP(schemaOut, _other.schemaOut);
	SWAP(numAttsInput, _other.numAttsInput);
	SWAP(numAttsOutput, _other.numAttsOutput);
	SWAP(keepMe, _other.keepMe);
    SWAP(producer, _other.producer); //TODO: OBJ_SWAP??
}

Join::Join(Schema& _schemaLeft, Schema& _schemaRight, Schema& _schemaOut,
	CNF& _predicate, RelationalOp* _left, RelationalOp* _right) : schemaLeft(_schemaLeft),
	schemaRight(_schemaRight), predicate(_predicate), left(_left), right(_right) {

    //TODO: is a function for union two sketches provided?
    schemaOut = _schemaOut;

    for (auto attr : schemaLeft.GetAtts())
        schemaOut.GetAtts().emplace_back(attr);
    for (auto attr : schemaRight.GetAtts())
        schemaOut.GetAtts().emplace_back(attr);

    // assuming two relations join only
    Comparison comparison = predicate.andList[0];

    Target operand1 = comparison.operand1, operand2 = comparison.operand2;
    int no_distinct1 = 0, no_distinct2 = 0;

    // the order of left and right distinct values don't matter
    int attr_idx1 = comparison.whichAtt1;
    if (operand1 == Left) {
        string left_attr_name = schemaLeft.GetAtts()[attr_idx1].name;
        no_distinct1 = schemaLeft.GetDistincts(left_attr_name);
    } else if (operand1 == Right) {
        string right_attr_name = schemaRight.GetAtts()[attr_idx1].name;
        no_distinct1 = schemaRight.GetDistincts(right_attr_name);
    }

    int attr_idx2 = comparison.whichAtt2;
    if (operand2 == Left) {
        string left_attr_name = schemaLeft.GetAtts()[attr_idx2].name;
        no_distinct2 = schemaLeft.GetDistincts(left_attr_name);
    } else {
        string right_attr_name = schemaRight.GetAtts()[attr_idx2].name;
        no_distinct2 = schemaRight.GetDistincts(right_attr_name);
    }

    if (no_distinct1 == 0 || no_distinct2 == 0) {
        cout << "invalid number of distincts in join operator" << endl;
        exit(-1);
    }

    // estimating join of two relations
    unsigned long int max_distinct = max(no_distinct1, no_distinct2);
    unsigned long int left_no_tuples = schemaLeft.GetTuplesNumber();
    unsigned long int right_no_tuples = schemaRight.GetTuplesNumber();
    unsigned long int total_no_tuple = left_no_tuples * right_no_tuples;
    unsigned long int est_no_tuple = total_no_tuple / max_distinct; //TODO: may exceed the range
    schemaOut.SetTuplesNumber(est_no_tuple);

    string _table_path = "no path"; // this is empty since this is an intermediate object
    schemaOut.SetTablePath(_table_path);
}

Join::~Join() {}

ostream& Join::print(ostream& _os) {
    _os << "JOIN (left-deep tree) estimated join size: " << schemaOut.GetTuplesNumber() << endl;
    _os << "\t\t\t[ " << *left;
    _os << "\t\t\t --- join -- " << endl;
    _os << "\t\t\t" << *right;
	return _os << "\t\t\t(end of join operator) ]" << endl;
}

void Join::Swap(Join &_other) {
    SWAP(noPages, _other.noPages);
    OBJ_SWAP(schemaLeft, _other.schemaLeft);
    OBJ_SWAP(schemaRight, _other.schemaRight);
    OBJ_SWAP(predicate, _other.predicate);
//    left->Swap(*_other.left);
//    right->Swap(*_other.right);
    SWAP(left, _other.left);
    SWAP(right, _other.right);
}

DuplicateRemoval::DuplicateRemoval(Schema& _schema, RelationalOp* _producer) :
    schema(_schema), producer(_producer) {
    //TODO: get the distinct values only
    //TODO: get attr name
}

DuplicateRemoval::~DuplicateRemoval() {}

ostream& DuplicateRemoval::print(ostream& _os) {
    string attr_name = schema.GetAtts()[0].name;
    _os << "DISTINCT [ on attribute name: " << attr_name << " ]" << endl;
    return _os << "\t\t\t[ " << *producer << "\t\t(end of distinct operator) ]" << endl;
}

void DuplicateRemoval::Swap(DuplicateRemoval &_other) {
    SWAP(noPages, _other.noPages);
    OBJ_SWAP(schema, _other.schema);
    SWAP(producer, _other.producer);
}

Sum::Sum(Schema& _schemaIn, Schema& _schemaOut, Function& _compute, RelationalOp* _producer) :
    schemaIn(_schemaIn), schemaOut(_schemaOut), compute(_compute), producer(_producer) {
    //TODO: how do we compute a single value?
}

Sum::~Sum() {}

ostream& Sum::print(ostream& _os) {
    _os << "SUM [ value of sum" << " ]" << endl;
    return _os << "\t\t\t[ " << *producer << "\t\t(end of sum operator) ]" << endl;
}

void Sum::Swap(Sum &_other) {
    SWAP(noPages, _other.noPages);
    OBJ_SWAP(schemaIn, _other.schemaIn);
    OBJ_SWAP(schemaOut, _other.schemaOut);
    OBJ_SWAP(compute, _other.compute);
    SWAP(producer, _other.producer);
}

GroupBy::GroupBy(Schema& _schemaIn, Schema& _schemaOut, OrderMaker& _groupingAtts, Function& _compute,
    RelationalOp* _producer) : schemaIn(_schemaIn), schemaOut(_schemaOut), groupingAtts(_groupingAtts),
    compute(_compute), producer(_producer) {
    //TODO: how do we compute this?
}

GroupBy::~GroupBy() {}

ostream& GroupBy::print(ostream& _os) {
    _os << "GROUP BY [ " << groupingAtts << " ]" << endl;
    return _os << "\t\t\t[ " << *producer << "\t\t(end of group-by operator) ]" << endl;
}

void GroupBy::Swap(GroupBy &_other) {
    SWAP(noPages, _other.noPages);
    OBJ_SWAP(schemaIn, _other.schemaIn);
    OBJ_SWAP(schemaOut, _other.schemaOut);
    OBJ_SWAP(groupingAtts, _other.groupingAtts);
    OBJ_SWAP(compute, _other.compute);
    SWAP(producer, _other.producer);
}

WriteOut::WriteOut(Schema& _schema, string& _outFile, RelationalOp* _producer) :
    schema(_schema), outFile(_outFile), producer(_producer) {
    // keep output file opened for the query results
    outStream.open(outFile); //ios::out | ios::trunc
    if (!outStream.is_open()) {
        cout << "output file for the results failed to open" << endl;
        exit(-1);
    }
}

WriteOut::~WriteOut() {
    // close the output file
    if (!outStream.is_open()) {
        outStream.close();
    }
}

bool WriteOut::GetNext(Record& _record) {
    if (producer->GetNext(_record)) {
        if (!outStream.is_open()) {
            outStream.open(outFile);
        }
        _record.print(outStream, schema);
        outStream << endl;
        outStream.flush();
        return true;
    }
    return false;
}

ostream& WriteOut::print(ostream& _os) {
    _os << "WRITE OUT [ output file location: " << outFile  << " ]" << endl;
	return _os << "\t\t[ " << *producer << "\t(end of write-out operator) ]" << endl;
}

void WriteOut::Swap(WriteOut &_other) {
    SWAP(noPages, _other.noPages);
    OBJ_SWAP(schema, _other.schema);
    STL_SWAP(outFile, _other.outFile);
    STL_SWAP(outStream, _other.outStream);
    SWAP(producer, _other.producer);
}

void QueryExecutionTree::SetRoot(RelationalOp& _root) {
    root = &_root;
    //TODO: assign operator may not be impl.
}

void QueryExecutionTree::ExecuteQuery() {
    Record record;
    while (root->GetNext(record)) {}
}

ostream& operator<<(ostream& _os, QueryExecutionTree& _op) {
	return _os << "QUERY EXECUTION TREE" << endl << "\t[ " << *_op.root << " (end of execution tree) ]" << endl;
}
