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

	// create join operators based on the optimal order computed by the optimizer
	Join root_join = CreateJoins(*root, *_predicate);

	// create the remaining operators based on the query

	// connect everything in the query execution tree and return

	// free the memory occupied by the parse tree since it is not necessary anymore
}

Join QueryCompiler::CreateJoins(OptimizationTree& _root, AndList& _predicate) {
    OptimizationTree* node_ptr = &_root;
    while (node_ptr) {

        if(node_ptr->leftChild->leftChild && node_ptr->leftChild->rightChild) {
            Join previous_join = CreateJoins(*node_ptr->leftChild, _predicate);

            if (node_ptr->tables.empty()) {
                cout << "right table is missing" << endl;
                exit(-1);
            }
            string right_table_name = node_ptr->tables.back();
            Schema right_schema;
            catalog->GetSchema(right_table_name, right_schema);

            CNF joined_cnf;
            if(joined_cnf.ExtractCNF(_predicate, previous_join.GetSchemaOut(), right_schema) == -1) {
                cout << "failed in joined cnf function" << endl;
                exit(-1);
            }

            Keyify<string> right_table_key(right_table_name);
            int is_right_push = selectionMap.IsThere(right_table_key);

            Schema joined_schema;

            if (is_right_push) {
                Select *right_selection = &selectionMap.Find(right_table_key);
                Join *new_join = new Join(previous_join.GetSchemaOut(), right_schema, joined_schema, joined_cnf, &previous_join, right_selection);
                return *new_join;
            } else {
                if (!scanMap.IsThere(right_table_key)) {
                    cout << "right table has either scan or select" << endl;
                    exit(-1);
                }
                Scan *right_scan = &scanMap.Find(right_table_key);
                Join *new_join = new Join(previous_join.GetSchemaOut(), right_schema, joined_schema, joined_cnf, &previous_join, right_scan);
                return *new_join;
            }
        }

        if (node_ptr->tables.size() != 2) {
            cout << "wrong join structure" << endl;
            exit(-1);
        }

        string left_table_name = node_ptr->tables[0];
        string right_table_name = node_ptr->tables[1];

        Schema left_schema, right_schema;
        catalog->GetSchema(left_table_name, left_schema);
        catalog->GetSchema(right_table_name, right_schema);

        CNF joined_cnf;
        if(joined_cnf.ExtractCNF(_predicate, left_schema, right_schema) == -1) {
            cout << "failed in joined cnf function" << endl;
            exit(-1);
        }

        Keyify<string> left_table_key(left_table_name);
        Keyify<string> right_table_key(right_table_name);

        int is_left_push = selectionMap.IsThere(left_table_key);
        int is_right_push = selectionMap.IsThere(right_table_key);

        Schema joined_schema;

        // base: two scan(select) to be joined
        // choosing select in case of push down, otherwise we choose scan
        if (is_left_push) {
            Select *left_selection = &selectionMap.Find(left_table_key);
            if (is_right_push) {
                Select *right_selection = &selectionMap.Find(right_table_key);
                Join *new_join = new Join(left_schema, right_schema, joined_schema, joined_cnf, left_selection, right_selection);
                return *new_join;
            } else {
                if (!scanMap.IsThere(right_table_key)) {
                    cout << "right table has either scan or select" << endl;
                    exit(-1);
                }
                Scan *right_scan = &scanMap.Find(right_table_key);
                Join *new_join = new Join(left_schema, right_schema, joined_schema, joined_cnf, left_selection, right_scan);
                return *new_join;
            }
        } else {
            if (!scanMap.IsThere(left_table_key)) {
                cout << "left table has either scan or select" << endl;
                exit(-1);
            }
            Scan *left_scan = &scanMap.Find(left_table_key);
            if (is_right_push) {
                Select *right_selection = &selectionMap.Find(right_table_key);
                Join *new_join = new Join(left_schema, right_schema, joined_schema, joined_cnf, left_scan, right_selection);
                return *new_join;
            } else {
                if (!scanMap.IsThere(right_table_key)) {
                    cout << "right table has either scan or select" << endl;
                    exit(-1);
                }
                Scan *right_scan = &scanMap.Find(right_table_key);
                Join *new_join = new Join(left_schema, right_schema, joined_schema, joined_cnf, left_scan, right_scan);
                return *new_join;
            }
        }
    }
}


void QueryCompiler::CreateScans(TableList& _tables) {
	TableList* current_table = &_tables;
	while (current_table) {

		Schema schema;
		DBFile table_file;
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
        catalog->GetSchema(table_name, schema);
        if(cnf.ExtractCNF(_predicate, schema, literal) == -1) {
            cout << "failed in single cnf function" << endl;
            exit(-1);
        }

        if (cnf.numAnds > 0) {
            RelationalOp *scan_operator = &scanMap.CurrentData();
            Select table_selection(schema, cnf, literal, scan_operator);

            Keyify<string> table_key(table_name);
            selectionMap.Insert(table_key, table_selection);
        }

        scanMap.Advance();
    }
}
