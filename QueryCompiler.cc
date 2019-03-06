#include <string>

#include "QueryCompiler.h"
#include "QueryOptimizer.h"
#include "Schema.h"
#include "ParseTree.h"
#include "Record.h"
#include "DBFile.h"
#include "Comparison.h"
#include "Function.h"
#include "RelOp.h"
#include "TwoWayList.cc"
#include "InefficientMap.cc"
#include "Config.h"

using namespace std;


QueryCompiler::QueryCompiler(Catalog& _catalog, QueryOptimizer& _optimizer) :
	catalog(&_catalog), optimizer(&_optimizer) {
}

QueryCompiler::~QueryCompiler() {
}

void QueryCompiler::Compile(TableList* _tables, NameList* _attsToSelect,
	FuncOperator* _finalFunction, AndList* _predicate,
	NameList* _groupingAtts, int& _distinctAtts,
	QueryExecutionTree& _queryTree) {

	// create a SCAN operator for each table in the query
	CreateScans(*_tables);

	// push-down selections: create a SELECT operator wherever necessary
    CreateSelects(*_predicate);

	// call the optimizer to compute the join order
	OptimizationTree *root = new OptimizationTree();
	optimizer->Optimize(_tables, _predicate, root);

    // create the remaining operators based on the query
    //TODO: create instances in heap and convert the functions to void
    GroupBy *group_by_operator;
    Sum *sum_operator;
    Project *projection;
    DuplicateRemoval *distinct_operator;
    Join *root_join;

    //indicators
    bool group_by_check = false, sum_check = false, distinct_check = false, project_check = false;

	// create join operators based on the optimal order computed by the optimizer
    root_join = CreateJoins(*root, *_predicate);

    if (_groupingAtts) { // group-by operator
        group_by_operator = CreateGroupBy(*_groupingAtts, *root_join);
        // TODO: distinct happens here automatically?
        if (_finalFunction) { // sum operator
            sum_operator = CreateAggregators(*_finalFunction, *root_join);
        }
        group_by_check = true;
    } else if (_finalFunction) { // sum operator
        sum_operator = CreateAggregators(*_finalFunction, *root_join);
        sum_check = true;
    } else if (_attsToSelect) { // projection operator
        projection = CreateProjection(*_attsToSelect, *root_join);
        project_check = true;
        if (_distinctAtts && projection->GetNumAttsOutput() > 0) { // distinct operator
            distinct_operator = new DuplicateRemoval(projection->GetSchemaOut(), projection);
            distinct_check = true;
        }
    }
    //TODO: WriteOut is used after the actual execution
    string out_file_name = "out file path";
    WriteOut *write_out;

//    Schema write_schema;
//    string schema_out_file_name = "schema out file name";
//    write_schema.SetTablePath(schema_out_file_name);
    //TODO: new schema or schema of the producer?

    if (group_by_check) {
        write_out = new WriteOut(group_by_operator->GetSchemaOut(), out_file_name, group_by_operator);
    } else if (sum_check) {
        write_out = new WriteOut(sum_operator->GetSchemaOut(), out_file_name, sum_operator);
    } else if (distinct_check) {
        write_out = new WriteOut(distinct_operator->GetSchemaOut(), out_file_name, distinct_operator);
    } else if (project_check) {
        write_out = new WriteOut(projection->GetSchemaOut(), out_file_name, projection);
    } else {
        cout << "unsupported operator was provided" << endl;
        exit(-1);
    }

	// connect everything in the query execution tree and return
    QueryExecutionTree *queryTree = new QueryExecutionTree();
    //TODO: check for nulls, reset the root of QueryExecutionTree
    queryTree->SetRoot(*write_out);
    _queryTree = *queryTree;

	// free the memory occupied by the parse tree since it is not necessary anymore
	delete _tables;
	delete _attsToSelect;
	delete _finalFunction;
	delete _predicate;
	delete _groupingAtts;
	_distinctAtts = 0;

	//TODO: can we delete these now?
	//delete distinct_operator;
    //delete projection;
    //delete sum_operator;
    //delete group_by_operator;
	delete root; //TODO: does it delete nested struct objects in heap?
	//delete root_join;
	//selectionMap.Clear();
	//scanMap.Clear();
}

Project* QueryCompiler::CreateProjection(NameList& _attsToSelect, Join& _root_join) {

    vector<int> attr_names;
    int no_attr_out = 0;
    Schema join_schema = _root_join.GetSchemaOut();

    NameList* current_attr = &_attsToSelect;
    while (current_attr) {
        no_attr_out++;
        string attr_name = current_attr->name;
        int att_idx = join_schema.Index(attr_name);
        if (att_idx == -1) {
            cout << "Attribute wasn't found in projection" << endl;
            exit(-1);
        }

        attr_names.emplace_back(att_idx);
        current_attr = current_attr->next;
    }
    if (no_attr_out > 0) {
        Schema projection_schema(join_schema);
        int no_attr_in = (int) join_schema.GetAtts().size();
        Project *projection = new Project(join_schema, projection_schema, no_attr_in, no_attr_out, &attr_names[0], &_root_join);
        return projection;
    }
}

Sum* QueryCompiler::CreateAggregators(FuncOperator& _finalFunction, RelationalOp& _producer) {
    Function function;
    string attr_name = _finalFunction.leftOperand->value; //TODO: this is attr name, how to get the table name?
    Schema schema, agg_schema;

    //TODO: this part has to be implemented without iteration over the all tables
    // assumption: attribute name belongs to one table only
    vector<string> tables;
    catalog->GetTables(tables);
    for (string table : tables) {
        catalog->GetSchema(table, schema);
        if (schema.Index(attr_name) != -1)
            break;
    }

    function.GrowFromParseTree(&_finalFunction, schema);
    Sum *sum_operator = new Sum(schema, agg_schema, function, &_producer); //TODO: do insert the join or projection?
    return sum_operator;
}

GroupBy* QueryCompiler::CreateGroupBy(NameList& _groupingAtts, Join& _root_join) {
    //TODO: verify this method for correctness
    Schema *root_schema = &_root_join.GetSchemaOut();
    vector<int> col_indices;

    NameList* current_group = &_groupingAtts;
    while (current_group) {
        string attr_name = current_group->name;
        int col_idx = root_schema->Index(attr_name);
        if (col_idx == -1)
            continue;
        col_indices.push_back(col_idx);
        current_group = current_group->next;
    }

    int no_cols = (int) col_indices.size();
    OrderMaker orderMaker(*root_schema, &col_indices[0], no_cols);

    Schema group_by_schema;
    Function function; //TODO: usage?
    GroupBy *group_by_operator = new GroupBy(*root_schema, group_by_schema, orderMaker, function, &_root_join);
    return group_by_operator;
}

Join* QueryCompiler::CreateJoins(OptimizationTree& _root, AndList& _predicate) {
    OptimizationTree* node_ptr = &_root;
    while (node_ptr) {

        if(node_ptr->leftChild->leftChild && node_ptr->leftChild->rightChild) {
            Join *previous_join = CreateJoins(*node_ptr->leftChild, _predicate);

            if (node_ptr->tables.empty()) {
                cout << "right table is missing" << endl;
                exit(-1);
            }
            string right_table_name = node_ptr->tables.back();

            CNF joined_cnf;

            Keyify<string> right_table_key(right_table_name);
            int is_right_push = selectionMap.IsThere(right_table_key);

            Schema joined_schema;

            if (is_right_push) {
                Select *right_selection = &selectionMap.Find(right_table_key);
                Schema right_schema = right_selection->GetSchemaOut();
                if(joined_cnf.ExtractCNF(_predicate, previous_join->GetSchemaOut(), right_schema) == -1) {
                    cout << "failed in joined cnf function" << endl;
                    exit(-1);
                }
                Join *new_join = new Join(previous_join->GetSchemaOut(), right_schema, joined_schema, joined_cnf, previous_join, right_selection);
                return new_join;
            } else {
                if (!scanMap.IsThere(right_table_key)) {
                    cout << "right table has either scan or select" << endl;
                    exit(-1);
                }
                Scan *right_scan = &scanMap.Find(right_table_key);
                Schema right_schema = right_scan->GetSchemaOut();
                if(joined_cnf.ExtractCNF(_predicate, previous_join->GetSchemaOut(), right_schema) == -1) {
                    cout << "failed in joined cnf function" << endl;
                    exit(-1);
                }
                Join *new_join = new Join(previous_join->GetSchemaOut(), right_schema, joined_schema, joined_cnf, previous_join, right_scan);
                return new_join;
            }
        }

        if (node_ptr->tables.size() != 2) {
            cout << "wrong join structure" << endl;
            exit(-1);
        }

        string left_table_name = node_ptr->tables[0];
        string right_table_name = node_ptr->tables[1];

        Keyify<string> left_table_key(left_table_name);
        Keyify<string> right_table_key(right_table_name);

        int is_left_push = selectionMap.IsThere(left_table_key);
        int is_right_push = selectionMap.IsThere(right_table_key);

        Schema joined_schema;
        CNF joined_cnf;

        // base: two scan(select) to be joined
        // choosing select in case of push down, otherwise we choose scan
        if (is_left_push) {
            Select *left_selection = &selectionMap.Find(left_table_key);
            Schema left_schema = left_selection->GetSchemaOut();
            if (is_right_push) {
                Select *right_selection = &selectionMap.Find(right_table_key);
                Schema right_schema = right_selection->GetSchemaOut();
                if(joined_cnf.ExtractCNF(_predicate, left_schema, right_schema) == -1) {
                    cout << "failed in joined cnf function" << endl;
                    exit(-1);
                }
                Join *new_join = new Join(left_schema, right_schema, joined_schema, joined_cnf, left_selection, right_selection);
                return new_join;
            } else {
                if (!scanMap.IsThere(right_table_key)) {
                    cout << "right table has either scan or select" << endl;
                    exit(-1);
                }
                Scan *right_scan = &scanMap.Find(right_table_key);
                Schema right_schema = right_scan->GetSchemaOut();
                if(joined_cnf.ExtractCNF(_predicate, left_schema, right_schema) == -1) {
                    cout << "failed in joined cnf function" << endl;
                    exit(-1);
                }
                Join *new_join = new Join(left_schema, right_schema, joined_schema, joined_cnf, left_selection, right_scan);
                return new_join;
            }
        } else {
            if (!scanMap.IsThere(left_table_key)) {
                cout << "left table has either scan or select" << endl;
                exit(-1);
            }
            Scan *left_scan = &scanMap.Find(left_table_key);
            Schema left_schema = left_scan->GetSchemaOut();
            if (is_right_push) {
                Select *right_selection = &selectionMap.Find(right_table_key);
                Schema right_schema = right_selection->GetSchemaOut();
                if(joined_cnf.ExtractCNF(_predicate, left_schema, right_schema) == -1) {
                    cout << "failed in joined cnf function" << endl;
                    exit(-1);
                }
                Join *new_join = new Join(left_schema, right_schema, joined_schema, joined_cnf, left_scan, right_selection);
                return new_join;
            } else {
                if (!scanMap.IsThere(right_table_key)) {
                    cout << "right table has either scan or select" << endl;
                    exit(-1);
                }
                Scan *right_scan = &scanMap.Find(right_table_key);
                Schema right_schema = right_scan->GetSchemaOut();
                if(joined_cnf.ExtractCNF(_predicate, left_schema, right_schema) == -1) {
                    cout << "failed in joined cnf function" << endl;
                    exit(-1);
                }
                Join *new_join = new Join(left_schema, right_schema, joined_schema, joined_cnf, left_scan, right_scan);
                return new_join;
            }
        }
    }
}

void QueryCompiler::CreateScans(TableList& _tables) {
	TableList* current_table = &_tables;
	while (current_table) {

		Schema schema;
		DBFile table_file; //TODO: this must be created with input arguments
		string table_name = current_table->tableName;

		catalog->GetSchema(table_name, schema);
		Scan table_scan(schema, table_file);
		Keyify<string> table_key(table_name);
		scanMap.Insert(table_key, table_scan);

		current_table = current_table->next;
	}
}

void QueryCompiler::CreateSelects(AndList& _predicate) {
    scanMap.MoveToStart();
    while (!scanMap.AtEnd()) {
        CNF cnf;
        Record literal;
        string table_name = scanMap.CurrentKey();
        Schema schema;
        catalog->GetSchema(table_name, schema); //TODO: does select can start from scan's schema?
        if(cnf.ExtractCNF(_predicate, schema, literal) == -1) {
            cout << "failed in single cnf function" << endl;
            exit(-1);
        }

        if (cnf.numAnds > 0) {
            Scan *scan_operator = &scanMap.CurrentData();
            Select table_selection(schema, cnf, literal, scan_operator);
            Keyify<string> table_key(table_name);
            selectionMap.Insert(table_key, table_selection);
        }
        scanMap.Advance();
    }
}
