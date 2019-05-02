
#include "RelOp.h"

#include "Swap.h"
#include "Config.h"
#include "Record.h"
#include "DBFile.h"
#include "InefficientMap.cc" //TODO:undefined reference (forced because of using 'template' in there)

#include <cstring>
#include <string>

#include <iostream>
#include <fstream>
#include <sstream>

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
            // left and right sides are columns. we treat the right side as literal
            CompOperator anOperator = _predicate.andList[0].op;
            string attr_name = schema.GetAtts()[_predicate.andList[0].whichAtt1].name;
            if (anOperator == LessThan || anOperator == GreaterThan) {
                denominator_value = 9; // since it is a column
            } else if (anOperator == Equals) {
                int no_distincts = schema.GetDistincts(attr_name); // we choose distinct of the left side
                if (no_distincts <= 0) {
                    cout << "Incorrect distinct value" << endl;
                    exit(-1);
                }
                denominator_value = no_distincts;
            } else {
                cout << "Unsupported operator" << endl;
                exit(-1);
            }
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
                // left and right sides are columns. we treat the right side as literal
                CompOperator anOperator = _predicate.andList[counter].op;
                string attr_name = schema.GetAtts()[_predicate.andList[counter].whichAtt1].name;
                if (anOperator == LessThan || anOperator == GreaterThan) {
                    denominator_value *= 9; // since it is a column
                } else if (anOperator == Equals) {
                    int no_distincts = schema.GetDistincts(attr_name); // we choose distinct of the left side
                    if (no_distincts <= 0) {
                        cout << "Incorrect distinct value" << endl;
                        exit(-1);
                    }
                    denominator_value *= no_distincts;
                } else {
                    cout << "Unsupported operator" << endl;
                    exit(-1);
                }
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
    attributes = new int[schemaLeft.GetNumAtts() + schemaRight.GetNumAtts()];
    int index = 0;

    auto left_attributes = schemaLeft.GetAtts();
    for (int i = 0; i < left_attributes.size(); i++) {
        schemaOut.GetAtts().emplace_back(left_attributes[i]);
        attributes[index] = i;
        index++;
    }

    rightSideIndexStart = index; // start index of right side schema

    auto right_attributes = schemaRight.GetAtts();
    for (int i = 0; i < right_attributes.size(); i++) {
        schemaOut.GetAtts().emplace_back(right_attributes[i]);
        attributes[index] = i;
        index++;
    }

    // assuming two relations join only
    if (predicate.numAnds > 1 || predicate.numAnds < 1) {
        cout << "the system doesn't support multi-join" << endl;
        exit(-1);
    }
    Comparison comparison = predicate.andList[0];
    join_attributes = new int[predicate.numAnds * 2];

    Target operand1 = comparison.operand1, operand2 = comparison.operand2;
    int no_distinct1 = 0, no_distinct2 = 0;

    // the order of left and right distinct values don't matter
    int attr_idx1 = comparison.whichAtt1;
    if (operand1 == Left) {
        string left_attr_name = schemaLeft.GetAtts()[attr_idx1].name;
        no_distinct1 = schemaLeft.GetDistincts(left_attr_name);
        if (compare_schema.GetNumAtts() == 0)
            compare_schema.GetAtts().insert(compare_schema.GetAtts().begin(), schemaLeft.GetAtts()[attr_idx1]);
        else
            compare_schema.GetAtts()[0] = schemaLeft.GetAtts()[attr_idx1];
        join_attributes[0] = attr_idx1;
    } else if (operand1 == Right) {
        string right_attr_name = schemaRight.GetAtts()[attr_idx1].name;
        no_distinct1 = schemaRight.GetDistincts(right_attr_name);
        if (compare_schema.GetNumAtts() == 0)
            compare_schema.GetAtts().insert(compare_schema.GetAtts().begin(), schemaRight.GetAtts()[attr_idx1]);
        compare_schema.GetAtts().insert(compare_schema.GetAtts().begin() + 1, schemaRight.GetAtts()[attr_idx1]);
        join_attributes[1] = attr_idx1;
    }

    int attr_idx2 = comparison.whichAtt2;
    if (operand2 == Left) {
        string left_attr_name = schemaLeft.GetAtts()[attr_idx2].name;
        no_distinct2 = schemaLeft.GetDistincts(left_attr_name);
        compare_schema.GetAtts()[0] = schemaLeft.GetAtts()[attr_idx2];
        join_attributes[0] = attr_idx2;
    } else {
        string right_attr_name = schemaRight.GetAtts()[attr_idx2].name;
        no_distinct2 = schemaRight.GetDistincts(right_attr_name);
        if (compare_schema.GetNumAtts() == 1)
            compare_schema.GetAtts().insert(compare_schema.GetAtts().begin() + 1, schemaRight.GetAtts()[attr_idx2]);
        else
            compare_schema.GetAtts()[1] = schemaRight.GetAtts()[attr_idx2];
        join_attributes[1] = attr_idx2;
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

    // TODO: check if there any case of having multiple joins
    // logic of join algorithm selection is needed here
    if (comparison.op == Equals) {
        // choosing between HJ and SHJ
        unsigned long int lift_size = left->GetSchemaOut().GetTuplesNumber();
        unsigned long int right_size = right->GetSchemaOut().GetTuplesNumber();

        //if (lift_size > 0 && right_size > 0) { //1000 or 10000
        if (lift_size > 10000000000 && right_size > 10000000000) {
            joinType = SHJ;
            sideCounter = 0;
            sideTurn = false;
            outerSide = right;
            cout << "SH Join" << endl;
        } else {
            joinType = HJ;
            cout << "H Join" << endl;
        }

    } else {
        joinType = NLJ; // by default
        cout << "NL Join" << endl;
    }
    isInnerTableExists = false;
}

Join::~Join() {}

bool Join::GetNext(Record& _record) {
    // both, either or neither side can be selection/scan/join operator
    if (joinType == SHJ) {
        return SHJoin(_record);
    } else if (joinType == HJ) {
        return HJoin(_record);
    }
    return NLJoin(_record);
}

bool Join::NLJoin(Record& _origin_record) {
    if (!isInnerTableExists) {
        isInnerTableExists = true;

        // need to choose smaller table to keep it in main memory
        unsigned long int lift_size = left->GetSchemaOut().GetTuplesNumber();
        unsigned long int right_size = right->GetSchemaOut().GetTuplesNumber();

        Record record;
        if (lift_size < right_size) {
            smallerSide = 0;
            while (left->GetNext(record)) {
                innerTable.Insert(record);
            }
        } else {
            smallerSide = 1;
            while (right->GetNext(record)) {
                innerTable.Insert(record);
            }
        }
        innerTable.MoveToStart();

        if (smallerSide == 0) {
            outerSide = right;
        } else {
            outerSide = left;
        }
        if (!outerSide->GetNext(currentOuterRecord)) {
            return false;
        }
    }

    // retrieve new tuple matches or remaining matches of the same tuple
    while (true) {
        if (innerTable.AtEnd()) {
            innerTable.MoveToStart();

            if (!outerSide->GetNext(currentOuterRecord)) {
                return false;
            }
        }

        while (!innerTable.AtEnd()) {
            Record& currentInnerRecord = innerTable.Current();
            // return original record with joined output
            if (smallerSide == 0 && predicate.Run(currentInnerRecord, currentOuterRecord)) {
                _origin_record.MergeRecords(currentInnerRecord, currentOuterRecord,
                    schemaLeft.GetNumAtts(), schemaRight.GetNumAtts(),
                    attributes, schemaOut.GetNumAtts(), rightSideIndexStart);
                innerTable.Advance();
                return true;
            } else if (predicate.Run(currentOuterRecord, currentInnerRecord)) {
                _origin_record.MergeRecords(currentOuterRecord, currentInnerRecord,
                    schemaLeft.GetNumAtts(), schemaRight.GetNumAtts(),
                    attributes, schemaOut.GetNumAtts(), rightSideIndexStart);
                innerTable.Advance();
                return true;
            }
            innerTable.Advance();
        }
    }
}

bool Join::HJoin(Record& _origin_record) {
    // TODO: duplicate records
    if (!isInnerTableExists) {
        isInnerTableExists = true;

        // need to choose smaller table to keep it in main memory
        unsigned long int lift_size = left->GetSchemaOut().GetTuplesNumber();
        unsigned long int right_size = right->GetSchemaOut().GetTuplesNumber();

        Record record;
        if (lift_size < right_size) {
            smallerSide = 0;
            while (left->GetNext(record)) {
                SwapInt val(0);
                ComplexKeyify<Record> recordToFind(record);
                innerHashedRecords.Insert(recordToFind, val);
            }
        } else {
            smallerSide = 1;
            while (right->GetNext(record)) {
                SwapInt val(0);
                ComplexKeyify<Record> recordToFind(record);
                innerHashedRecords.Insert(recordToFind, val);
            }

            SWAP(join_attributes[0], join_attributes[1]);
            OBJ_SWAP(compare_schema.GetAtts()[0], compare_schema.GetAtts()[1]);
        }
        innerHashedRecords.MoveToStart();

        OrderMaker orderMaker(compare_schema, join_attributes);
        compareRecords.Swap(orderMaker);

        if (smallerSide == 0) {
            outerSide = right;
        } else {
            outerSide = left;
        }

        isNextTupleNeeded = false;
    }

    // retrieve new tuple matches or remaining matches of the same tuple
    while (true) {

        if (!isNextTupleNeeded && !outerSide->GetNext(currentOuterRecord)) {
            return false;
        }

        isNextTupleNeeded = true;

        ComplexKeyify<Record> recordToFind(currentOuterRecord);
        while (innerHashedRecords.IsThereJoinRecord(recordToFind, compareRecords) == 1) {
            SwapInt value;
            ComplexKeyify<Record> removedKey;

            if (innerHashedRecords.RemoveJoinRecord(recordToFind, removedKey, value, compareRecords) == 0) {
                cout << "Record wasn't found after RecordJoinFind operator" << endl;
                continue;
            }

            Record currentInnerRecord = removedKey.GetData();

            // return original record with joined output
            if (smallerSide == 0) {
                _origin_record.MergeRecords(currentInnerRecord, currentOuterRecord,
                    schemaLeft.GetNumAtts(), schemaRight.GetNumAtts(),
                    attributes, schemaOut.GetNumAtts(), rightSideIndexStart);
            } else {
                _origin_record.MergeRecords(currentOuterRecord, currentInnerRecord,
                    schemaLeft.GetNumAtts(), schemaRight.GetNumAtts(),
                    attributes, schemaOut.GetNumAtts(), rightSideIndexStart);
            }
            usedRecords.Insert(currentInnerRecord);
            return true;
        }
        isNextTupleNeeded = false;

        if (usedRecords.Length() > 0) {
            // re-inserting used records
            usedRecords.MoveToStart();
            while (!usedRecords.AtEnd()) {
                SwapInt val(0);
                Record tempRecord;
                usedRecords.Remove(tempRecord);
                ComplexKeyify<Record> tempComplextRecord(tempRecord);
                innerHashedRecords.Insert(tempComplextRecord, val);
            }
        }
    }
}

bool Join::SHJoin(Record& _origin_record) {
    if (!isInnerTableExists) {
        isInnerTableExists = true;

        OrderMaker orderMaker_left(compare_schema, join_attributes);
        compareRecords_left.Swap(orderMaker_left);

        SWAP(join_attributes[0], join_attributes[1]);
        OBJ_SWAP(compare_schema.GetAtts()[0], compare_schema.GetAtts()[1]);

        OrderMaker orderMaker_right(compare_schema, join_attributes);
        compareRecords_right.Swap(orderMaker_right);

        compareRecordsCommon = &compareRecords_left;
        innerHashCommon = &innerHash_left;
        innerHashOther = &innerHash_right;
        usedRecordCommon = &usedRecords_left;
        isOtherFinished = false;
    }

    // Algorithm:
    // read 10 from left and put them into map
    // read 10 from right, probe and put them in the other map
    // repeat
    // probe the rest of left and right

    // retrieve new tuple matches or remaining matches of the same tuple
    while (true) {

        if (sideCounter == 10 && !isOtherFinished) {
            if (!sideTurn) {
                sideTurn = true;
                outerSide = left;
                compareRecordsCommon = &compareRecords_right;
                innerHashCommon = &innerHash_right;
                innerHashOther = &innerHash_left;
                usedRecordCommon = &usedRecords_right;
            } else {
                sideTurn = false;
                outerSide = right;
                compareRecordsCommon = &compareRecords_left;
                innerHashCommon = &innerHash_left;
                innerHashOther = &innerHash_right;
                usedRecordCommon = &usedRecords_left;
            }
            sideCounter = 0;
        }

        if (!isNextTupleNeeded && !outerSide->GetNext(currentOuterRecord)) {
            // process the rest of the other side
            if (!isOtherFinished) {

                if (!sideTurn) {
                    sideTurn = true;
                    outerSide = left;
                    compareRecordsCommon = &compareRecords_right;
                    innerHashCommon = &innerHash_right;
                    innerHashOther = &innerHash_left;
                    usedRecordCommon = &usedRecords_right;
                } else {
                    sideTurn = false;
                    outerSide = right;
                    compareRecordsCommon = &compareRecords_left;
                    innerHashCommon = &innerHash_left;
                    innerHashOther = &innerHash_right;
                    usedRecordCommon = &usedRecords_left;
                }
                sideCounter = 0;

                isOtherFinished = true;
                continue;
            } else {
                return false;
            }
        }

        isNextTupleNeeded = true;

        ComplexKeyify<Record> recordToFind(currentOuterRecord);
        while (innerHashCommon->IsThereJoinRecord(recordToFind, *compareRecordsCommon) == 1) {
            SwapInt value;
            ComplexKeyify<Record> removedKey;

            if (innerHashCommon->RemoveJoinRecord(recordToFind, removedKey, value, compareRecords) == 0) {
                cout << "Record wasn't found after RecordJoinFind operator" << endl;
                continue;
            }

            Record currentInnerRecord = removedKey.GetData();

            // return original record with joined output
            if (!sideTurn) {
                _origin_record.MergeRecords(currentInnerRecord, currentOuterRecord,
                    schemaLeft.GetNumAtts(), schemaRight.GetNumAtts(),
                    attributes, schemaOut.GetNumAtts(), rightSideIndexStart);
            } else {
                _origin_record.MergeRecords(currentOuterRecord, currentInnerRecord,
                    schemaLeft.GetNumAtts(), schemaRight.GetNumAtts(),
                    attributes, schemaOut.GetNumAtts(), rightSideIndexStart);
            }
            usedRecordCommon->Insert(currentInnerRecord);
            return true;
        }
        isNextTupleNeeded = false;
        sideCounter++;

        SwapInt val(0);
        ComplexKeyify<Record> recordToFindTemp(currentOuterRecord);
        innerHashOther->Insert(recordToFindTemp, val);

        if (usedRecordCommon->Length() > 0) {
            // re-inserting used records
            usedRecordCommon->MoveToStart();
            while (!usedRecordCommon->AtEnd()) {
                SwapInt val(0);
                Record tempRecord;
                usedRecordCommon->Remove(tempRecord);
                ComplexKeyify<Record> tempComplextRecord(tempRecord);
                innerHashCommon->Insert(tempComplextRecord, val);
            }
        }
    }
}

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

DuplicateRemoval::DuplicateRemoval(Schema& _schema, RelationalOp* _producer, OrderMaker& _compareRecords) :
    schema(_schema), producer(_producer), compareRecords(_compareRecords) {}

DuplicateRemoval::~DuplicateRemoval() {}

bool DuplicateRemoval::GetNext(Record& _record) {
    while (producer->GetNext(_record)) {
        ComplexKeyify<Record> recordToFind(_record);
        if (distinctRecords.IsThereRecord(recordToFind, compareRecords) == 0) {
            SwapInt val(0);
            distinctRecords.Insert(recordToFind, val);
            return true;
        }
    }
    return false;
}

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
    isComputed = false;
}

Sum::~Sum() {}

bool Sum::GetNext(Record& _record) {
    if (isComputed)
        return false;
    int sum_value1 = 0, temp1 = 0;
    double sum_value2 = 0, temp2 = 0;
    while (producer->GetNext(_record)) {
        compute.Apply(_record, temp1, temp2);
        sum_value1 += temp1;
        sum_value2 += temp2;
    }
    string res_str;
    if (schemaOut.GetAtts()[0].type == Integer)
        res_str = std::to_string(sum_value1);
    else
        res_str += std::to_string(sum_value2);

    vector<string> data;
    data.emplace_back(res_str);
    _record.MakeRecord(data, schemaOut.GetAtts(), schemaOut.GetNumAtts());

    isComputed = true;

    return true;
}

ostream& Sum::print(ostream& _os) {
    _os << "SUM [ value of sum" << " ]" << endl;
    return _os << "\t\t\t[ " << *producer << "\t\t(end of sum operator) ]" << endl;
}

void Sum::Swap(Sum &_other) {
    SWAP(noPages, _other.noPages);
    OBJ_SWAP(schemaIn, _other.schemaIn);
    OBJ_SWAP(schemaOut, _other.schemaOut);
    OBJ_SWAP(compute, _other.compute);
    SWAP(isComputed, _other.isComputed);
    SWAP(producer, _other.producer);
}

GroupBy::GroupBy(Schema& _schemaIn, Schema& _schemaOut, OrderMaker& _groupingAtts, Function& _compute,
    RelationalOp* _producer) : schemaIn(_schemaIn), schemaOut(_schemaOut), groupingAtts(_groupingAtts),
    compute(_compute), producer(_producer) { //, schemaOriginOut(_schemaOut)

    isComputed = false;
}

GroupBy::~GroupBy() {}

bool GroupBy::GetNext(Record& _record) {
    if (!isComputed) {

        Type agg_type = Float;

        while (producer->GetNext(_record)) {
            ComplexKeyify<Record> recordToFind(_record);
            if (recordStats.IsThereRecord(recordToFind, groupingAtts) == 0) {
                int temp1 = 0;
                double temp2 = 0;
                compute.Apply(_record, temp1, temp2);
                SwapDouble val(temp2);
                recordStats.Insert(recordToFind, val);
            } else {
                int temp1 = 0;
                double temp2 = 0;
                agg_type = compute.Apply(_record, temp1, temp2);
                recordStats.FindRecord(recordToFind, groupingAtts).GetData() += temp2;
            }
        }
        isComputed = true;
        recordStats.MoveToStart();

        for (int i = 0; i < groupingAtts.numAtts; i++)
            schemaOriginOut.GetAtts().emplace_back(schemaIn.GetAtts()[groupingAtts.whichAtts[i]]);
    }

    if (recordStats.Length() < 1) {
        return false;
    }

    //TODO: multiple attr grouping - use case
    ComplexKeyify<Record> find_key = recordStats.CurrentKey();

    // removing returning tuple
    ComplexKeyify<Record> key;
    SwapDouble value;
    if (recordStats.RemoveRecord(find_key, key, value, groupingAtts) == 1) {
        //return first tuple
        string res_str;
        if (schemaOut.GetAtts()[0].type == Integer) //TODO: this case is not covered so far
            res_str = std::to_string(value.GetData());
        else
            res_str += std::to_string(value.GetData());
        vector<string> data;
        data.emplace_back(res_str);
        stringstream os;
        key.GetData().printSet(os, schemaOriginOut, groupingAtts.whichAtts);
        string grouping_attr = os.str();
        data.emplace_back(grouping_attr);
        _record.MakeRecord(data, schemaOut.GetAtts(), schemaOut.GetNumAtts());
        return true;
    } else {
        return false;
    }
}

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
