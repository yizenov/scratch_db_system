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

    //indicators
    bool join_check = false, group_by_check = false, sum_check = false, distinct_check = false, project_check = false;

	// call the optimizer to compute the join order
	OptimizationTree *root;
	if (scanMap.Length() < 1) {
	    cout << "no input tables provided" << endl;
	} else if (scanMap.Length() > 1) {
        root = new OptimizationTree();
        optimizer->Optimize(_tables, _predicate, root);
        join_check = true;
    }

    // create the remaining operators based on the query
    //TODO: create instances in heap and convert the functions to void
    Project *projection = new Project();
    Sum *sum = new Sum();
    RelationalOp* pre_out_operator = new WriteOut();

	// create join operators based on the optimal order computed by the optimizer
	if (join_check) {
        pre_out_operator = CreateJoins(*root, *_predicate);
    }

    //TODO: losing pointers to producers after functions (heap/stack objects/pointers)

    Select *single_selection;

    if (_groupingAtts) { // group-by operator
        if (join_check) {
            pre_out_operator = CreateGroupBy(*_groupingAtts, *_finalFunction, *_attsToSelect, *pre_out_operator);
        } else if (selectionMap.Length() == 1) {
            Select single_selection = selectionMap.CurrentData();
            pre_out_operator = CreateGroupBy(*_groupingAtts, *_finalFunction, *_attsToSelect, single_selection);
        } else if (scanMap.Length() == 1) {
            Scan single_scan = scanMap.CurrentData();
            pre_out_operator = CreateGroupBy(*_groupingAtts, *_finalFunction, *_attsToSelect, single_scan);
        } else {
            cout << "incorrect parent operator for group-by" << endl;
            exit(-1);
        }
        // TODO: distinct and sum happen here automatically?
    } else if (_finalFunction) { // sum operator
        if (join_check) {
            CreateAggregators(*_finalFunction, *pre_out_operator, *sum);
            pre_out_operator = sum;
        } else if (selectionMap.Length() == 1) {
            single_selection = &selectionMap.CurrentData();
            CreateAggregators(*_finalFunction, *single_selection, *sum);
            pre_out_operator = sum;
        } else if (scanMap.Length() == 1) {
            Scan single_scan = scanMap.CurrentData();
            CreateAggregators(*_finalFunction, single_scan, *sum);
            pre_out_operator = sum;
        } else {
            cout << "incorrect parent operator for sum aggregation" << endl;
            exit(-1);
        }
        //TODO: handle project+agg queries
    } else if (_attsToSelect) { // projection operator
        if (join_check) {
            CreateProjection(*_attsToSelect, *pre_out_operator, *projection);
            pre_out_operator = projection;
            project_check = true;
        } else if (selectionMap.Length() == 1) {
            single_selection = &selectionMap.CurrentData();
            CreateProjection(*_attsToSelect, *single_selection, *projection);
            pre_out_operator = projection;
            project_check = true;
        } else if (scanMap.Length() == 1) {
            Scan single_scan = scanMap.CurrentData();
            CreateProjection(*_attsToSelect, single_scan, *projection);
            pre_out_operator = projection;
            project_check = true;
        } else {
            cout << "incorrect parent operator for projection" << endl;
            exit(-1);
        }

        if (_distinctAtts && project_check) { // distinct operator
            pre_out_operator = new DuplicateRemoval(pre_out_operator->GetSchemaOut(), pre_out_operator);
        }
    } else {
        cout << "unsupported operator was provided" << endl;
        exit(-1);
    }

    //TODO: WriteOut is used after the actual execution
    //TODO: new schema or schema of the producer?
    string out_file_name = DB_QUERY_RESULT_OUT_FILE;
    WriteOut *write_out = new WriteOut(pre_out_operator->GetSchemaOut(), out_file_name, pre_out_operator);

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
	//delete root; //TODO: does it delete nested struct objects in heap?
	//delete root_join;
	//selectionMap.Clear();
	//scanMap.Clear();
}

void QueryCompiler::CreateProjection(NameList& _attsToSelect, RelationalOp& _producer, Project &_projection) {

    vector<int> attr_names;
    int no_attr_out = 0;

    Schema producer_schema = _producer.GetSchemaOut();

    NameList* current_attr = &_attsToSelect;
    while (current_attr) {
        no_attr_out++;
        string attr_name = current_attr->name;
        int att_idx = producer_schema.Index(attr_name);
        if (att_idx == -1) {
            cout << "Attribute wasn't found in projection" << endl;
            exit(-1);
        }

        attr_names.emplace_back(att_idx);
        current_attr = current_attr->next;
    }
    if (no_attr_out > 0) {
        Schema projection_schema(producer_schema);
        int no_attr_in = (int) producer_schema.GetAtts().size();
        Project project(producer_schema, projection_schema, no_attr_in, no_attr_out, &attr_names[0], &_producer);
        _projection.Swap(project);
    } else {
        cout << "no attribute to project" << endl;
        exit(-1);
    };
}

void QueryCompiler::CreateAggregators(FuncOperator& _finalFunction, RelationalOp& _producer, Sum &_sum) {

    FuncOperator* current_agg_func = &_finalFunction;
    if (!current_agg_func) {
        cout << "aggregate function doesn't exist" << endl;
        exit(-1);
    }
    Function function;
    Schema schema, agg_schema;
    schema = _producer.GetSchemaOut();
    function.GrowFromParseTree(&_finalFunction, schema); //TODO: agg schema or producer schema?
    Sum sum_op(schema, agg_schema, function, &_producer); //TODO: do insert the join or projection?
    _sum.Swap(sum_op);
}

GroupBy* QueryCompiler::CreateGroupBy(NameList& _groupingAtts, FuncOperator& _finalFunction, NameList& _attsToSelect, RelationalOp& _producer) {

    Schema *root_schema = &_producer.GetSchemaOut();

    vector<Attribute> schema_attributes = root_schema->GetAtts();
    unordered_set<string> existing_attributes;
    for (Attribute attr : schema_attributes)
        existing_attributes.insert(attr.name);

    NameList* current_attr = &_attsToSelect;
    while (current_attr) {
        if (existing_attributes.find(current_attr->name) == existing_attributes.end()) {
            cout << "select attribute: " << current_attr->name << " doesn't exist in the database" << endl;
            exit(-1);
        }
        current_attr = current_attr->next;
    }

    vector<int> col_indices;
    vector<string> attrs, attr_types;
    vector<unsigned int> distincts;

    NameList* current_group = &_groupingAtts;
    while (current_group) {
        string attr_name = current_group->name;
        int col_idx = root_schema->Index(attr_name);
        if (col_idx == -1) {
            cout << "grouping attribute: " << attr_name << " doesn't exist in the schema" << endl;
            exit(-1);
        }
        col_indices.push_back(col_idx);
        attrs.emplace_back(attr_name);
        Type attr_type = root_schema->FindType(attr_name);
        string type_name = catalog->GetTypeName(attr_type);
        attr_types.emplace_back(type_name);
        distincts.emplace_back(root_schema->GetDistincts(attr_name));

        current_group = current_group->next;
    }

    Schema group_by_schema(attrs, attr_types, distincts);

    // group by has to have an aggregator
    FuncOperator* current_agg_func = &_finalFunction;
    if (!current_agg_func) {
        cout << "aggregate function doesn't exist" << endl;
        exit(-1);
    }
    Function function;
    //function.GrowFromParseTree(&_finalFunction, group_by_schema);

    int no_cols = (int) col_indices.size();
    OrderMaker orderMaker(group_by_schema, &col_indices[0], no_cols);

    GroupBy *group_by_operator = new GroupBy(*root_schema, group_by_schema, orderMaker, function, &_producer);
    return group_by_operator;
}

RelationalOp* QueryCompiler::CreateJoins(OptimizationTree& _root, AndList& _predicate) {
    OptimizationTree* node_ptr = &_root;
    while (node_ptr) {

        RelationalOp *left_tree = NULL, *right_tree = NULL;
        if(node_ptr->leftChild) {
            left_tree = CreateJoins(*node_ptr->leftChild, _predicate);
        }

        if(node_ptr->rightChild) {
            right_tree = CreateJoins(*node_ptr->rightChild, _predicate);
        }

        if(left_tree && right_tree) {

            CNF joined_cnf;
            Schema joined_schema;

            if(joined_cnf.ExtractCNF(_predicate, left_tree->GetSchemaOut(), right_tree->GetSchemaOut()) == -1) {
                cout << "failed in joined cnf function" << endl;
                exit(-1);
            }
            Join *new_join = new Join(left_tree->GetSchemaOut(), right_tree->GetSchemaOut(), joined_schema, joined_cnf, left_tree, right_tree);
            return new_join;

        }

        if (node_ptr->tables.size() > 2 || node_ptr->tables.size() < 1) {
            cout << "wrong join structure" << endl;
            exit(-1);
        }

        if (node_ptr->tables.size() == 1) {
            string table_name = node_ptr->tables[0];
            Keyify<string> table_key(table_name);
            int is_push = selectionMap.IsThere(table_key);
            if (is_push) {
                Select *selection = &selectionMap.Find(table_key);
                return selection;
            } else {
                if (!scanMap.IsThere(table_key)) {
                    cout << "table doesn't have either scan or select" << endl;
                    exit(-1);
                }
                Scan *scan = &scanMap.Find(table_key);
                return scan;
            }
        } else {

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
                    if (joined_cnf.ExtractCNF(_predicate, left_schema, right_schema) == -1) {
                        cout << "failed in joined cnf function" << endl;
                        exit(-1);
                    }
                    Join *new_join = new Join(left_schema, right_schema, joined_schema, joined_cnf, left_selection, right_selection);
                    return new_join;
                } else {
                    if (!scanMap.IsThere(right_table_key)) {
                        cout << "right table doesn't have either scan or select" << endl;
                        exit(-1);
                    }
                    Scan *right_scan = &scanMap.Find(right_table_key);
                    Schema right_schema = right_scan->GetSchemaOut();
                    if (joined_cnf.ExtractCNF(_predicate, left_schema, right_schema) == -1) {
                        cout << "failed in joined cnf function" << endl;
                        exit(-1);
                    }
                    Join *new_join = new Join(left_schema, right_schema, joined_schema, joined_cnf, left_selection, right_scan);
                    return new_join;
                }
            } else {
                if (!scanMap.IsThere(left_table_key)) {
                    cout << "left table doesn't have either scan or select" << endl;
                    exit(-1);
                }
                Scan *left_scan = &scanMap.Find(left_table_key);
                Schema left_schema = left_scan->GetSchemaOut();
                if (is_right_push) {
                    Select *right_selection = &selectionMap.Find(right_table_key);
                    Schema right_schema = right_selection->GetSchemaOut();
                    if (joined_cnf.ExtractCNF(_predicate, left_schema, right_schema) == -1) {
                        cout << "failed in joined cnf function" << endl;
                        exit(-1);
                    }
                    Join *new_join = new Join(left_schema, right_schema, joined_schema, joined_cnf, left_scan, right_selection);
                    return new_join;
                } else {
                    if (!scanMap.IsThere(right_table_key)) {
                        cout << "right table doesn't have either scan or select" << endl;
                        exit(-1);
                    }
                    Scan *right_scan = &scanMap.Find(right_table_key);
                    Schema right_schema = right_scan->GetSchemaOut();
                    if (joined_cnf.ExtractCNF(_predicate, left_schema, right_schema) == -1) { //TODO: fails when there is no join 7.sql
                        cout << "failed in joined cnf function" << endl;
                        exit(-1);
                    }
                    Join *new_join = new Join(left_schema, right_schema, joined_schema, joined_cnf, left_scan, right_scan);
                    return new_join;
                }
            }
        }
    }
}

void QueryCompiler::CreateScans(TableList& _tables) {

    vector<string> db_tables;
    catalog->GetTables(db_tables);
    unordered_set<string> existing_tables;
    for (string t_name : db_tables)
        existing_tables.insert(t_name);

	TableList* current_table = &_tables;
	while (current_table) {

	    if (existing_tables.find(current_table->tableName) == existing_tables.end()) {
            cout << "table: " << current_table->tableName << " doesn't exist in the database" << endl;
            exit(-1);
        }

		Schema schema;
		DBFile table_file; //TODO: this must be created with input arguments
		string table_name = current_table->tableName;

		catalog->GetSchema(table_name, schema);
		Scan *table_scan = new Scan(schema, table_file);
		Keyify<string> table_key(table_name);
		scanMap.Insert(table_key, *table_scan);

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

        //TODO: check for typo in selection predicates

        if (cnf.numAnds > 0) {
            Scan *scan_operator = &scanMap.CurrentData();
            Select *table_selection = new Select(schema, cnf, literal, scan_operator);
            Keyify<string> table_key(table_name);
            selectionMap.Insert(table_key, *table_selection);
        }
        scanMap.Advance();
    }
}
