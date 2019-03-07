#include <string>
#include <vector>
#include <iostream>
#include <unordered_map>
#include <bits/stdc++.h>
#include <limits.h>

#include "Schema.h"
#include "Comparison.h"
#include "QueryOptimizer.h"

using namespace std;


QueryOptimizer::QueryOptimizer(Catalog& _catalog) : catalog(&_catalog) {
}

QueryOptimizer::~QueryOptimizer() {
}

void QueryOptimizer::Optimize(TableList* _tables, AndList* _predicate,
	OptimizationTree* _root) {
	// compute the optimal join order

	// this function is called only there are at least two tables in the query
	//TODO: assumptions: left-deep tree, always push-down

	int no_tables = 0;
	vector<string> table_names;
	vector<int> table_indices;
	unordered_map<string, pair<unsigned long int, vector<int>>> dp_map;
    unordered_map<string, Schema> schema_info;

	// single table cardinality estimation
	TableList* current_table = _tables;
	while (current_table) {
		string table_name = current_table->tableName;
		table_names.emplace_back(table_name);

		Schema table_schema;
		catalog->GetSchema(table_name, table_schema);

		CNF cnf;
		Record literal;
		if(cnf.ExtractCNF(*_predicate, table_schema, literal) == -1) {
			cout << "failed in single cnf function" << endl;
			exit(-1);
		}

		string table_key = "_" + to_string(no_tables);
		Keyify<string> order_key(table_key);
		DBFile db_file;
		Scan *table_scan = new Scan(table_schema, db_file);
		if (cnf.numAnds > 0) {
            Select table_select(table_scan->GetSchemaOut(), cnf, literal, table_scan);
			unsigned long int estimate = EstimateCardinality(cnf, table_select.GetSchemaOut(), *_predicate);
			Schema select_schema = table_select.GetSchemaOut();
            schema_info[table_key] = select_schema;
			dp_map[table_key] = {0, {no_tables}};
		} else {
            Schema scan_schema = table_scan->GetSchemaOut();
            schema_info[table_key] = scan_schema;
			dp_map[table_key] = {0, {no_tables}};
		}

		table_indices.emplace_back(no_tables);
		no_tables++;
		current_table = current_table->next;
	}

	// two table join estimations
	for (int i = 0; i < no_tables - 1; i++) {
        Schema left = schema_info["_" + to_string(i)];
		for (int j = i + 1; j < no_tables; j++) {
            string join_key = "_" + to_string(i) + "_" + to_string(j);
            Schema right = schema_info["_" + to_string(j)];
            Schema out;
            CNF cnf;
            if(cnf.ExtractCNF(*_predicate, left, right) == -1) {
                cout << "failed in single cnf function" << endl;
                exit(-1);
            }
            JoinSizeEstimation(cnf, left, right, out);
            schema_info[join_key] = out;
            dp_map[join_key] = {0, {i, j}};
		}
	}

	// creating all N! permutations of N tables
	vector<int> indices;
	for (auto i : table_indices)
		indices.emplace_back(i);

	find_optimal_plan(indices, *_predicate, schema_info, dp_map);
	sort(indices.begin(), indices.end());

	string plan_key = "";
	for (int idx : indices)
		plan_key += "_" + to_string(idx);
	vector<int> *optimal_plan = &dp_map[plan_key].second;

    vector<string> optimal_order;
    cout << "optimal plan:";
	for (int table_id : *optimal_plan) {
		cout << " " << table_names[table_id];
        optimal_order.emplace_back(table_names[table_id]);
	}
	cout << endl;

	// input tree is already an optimal at this point
	BuildTree(optimal_order, _root);
}

unsigned long int QueryOptimizer::EstimateCardinality(CNF& cnf, Schema& schema, AndList& _predicate) {

	// estimating the cardinality of a relation
	int denominator_value = 0;

	if (cnf.andList[0].operand2 == Literal) {
		CompOperator anOperator = cnf.andList[0].op;
		string attr_name = schema.GetAtts()[cnf.andList[0].whichAtt1].name;
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
	}

	unsigned counter = 1;
	while (counter < cnf.numAnds) {
		if (cnf.andList[counter].operand2 == Literal) {
			CompOperator anOperator = cnf.andList[counter].op;
			string attr_name = schema.GetAtts()[cnf.andList[counter].whichAtt1].name;
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
		}
		counter++;
	}

	unsigned long int cardinality = schema.GetTuplesNumber() / denominator_value;
    schema.SetTuplesNumber(cardinality);
	return cardinality;
}


unsigned long int QueryOptimizer::JoinSizeEstimation(CNF& predicate, Schema& schemaLeft, Schema& schemaRight, Schema& schemaOut) {

	for (auto attr : schemaLeft.GetAtts())
		schemaOut.GetAtts().emplace_back(attr);
	for (auto attr : schemaRight.GetAtts())
		schemaOut.GetAtts().emplace_back(attr);

    if (predicate.numAnds > 0) {

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
        unsigned long int est_no_tuple = total_no_tuple / max_distinct;

        schemaOut.SetTuplesNumber(est_no_tuple);
        return est_no_tuple;
    } else {
        unsigned long int left_no_tuples = schemaLeft.GetTuplesNumber();
        unsigned long int right_no_tuples = schemaRight.GetTuplesNumber();
        unsigned long int total_no_tuple = left_no_tuples * right_no_tuples;
        schemaOut.SetTuplesNumber(total_no_tuple);
    }
}

// Heap's algorithm to generate permutations
void QueryOptimizer::generate_permutations(vector<int>& indices, int size, int n, vector<vector<int>>& permutations) {

    if (size == 1) {
		permutations.emplace_back(indices);
        return;
    }

    for (int i = 0; i < size; i++) {
        generate_permutations(indices, size - 1, n, permutations);

        if (size % 2 == 1) {
            int temp = indices[0];
            indices[0] = indices[size - 1];
            indices[size - 1] = temp;
        } else {
            int temp = indices[i];
            indices[i] = indices[size - 1];
            indices[size - 1] = temp;
        }
    }
}

void QueryOptimizer::find_optimal_plan(vector<int>& indices, AndList& _predicate,
        unordered_map<string, Schema>& _schema_info,
		unordered_map<string, pair<unsigned long int, vector<int>>>& plan_info) {

    vector<int> local_indices = indices;
    sort(local_indices.begin(), local_indices.end());

    string sorted_order_key = "";
    for (auto i : local_indices)
		sorted_order_key += "_" + to_string(i);
    if (_schema_info.find(sorted_order_key) != _schema_info.end())
        return;

    vector<vector<int>> permutations;
    generate_permutations(indices, indices.size(), indices.size(), permutations);

	unsigned long int min_cost = ULONG_MAX;
	vector<int> optimal_order;
	Schema optimal_schema;

    for (vector<int> permutation : permutations) {

		for (int i = 0; i < permutation.size() - 1; i++) {

			vector<int> left_indices, right_indices;
			vector<int> origin_left_indices, origin_right_indices;

			for (int j = 0; j < permutation.size(); j++) {
				if (j <= i) {
					left_indices.emplace_back(permutation[j]);
					origin_left_indices.emplace_back(permutation[j]);
				} else {
					right_indices.emplace_back(permutation[j]);
					origin_right_indices.emplace_back(permutation[j]);
				}
			}

			sort(left_indices.begin(), left_indices.end());
			sort(right_indices.begin(), right_indices.end());

			string left_order_key = "";
			for (auto k : left_indices)
				left_order_key += "_" + to_string(k);
			string right_order_key = "";
			for (auto k : right_indices)
				right_order_key += "_" + to_string(k);

			if (_schema_info.find(left_order_key) == _schema_info.end())
				find_optimal_plan(origin_left_indices, _predicate, _schema_info, plan_info);
			if (_schema_info.find(right_order_key) == _schema_info.end())
				find_optimal_plan(origin_right_indices, _predicate, _schema_info, plan_info);

			Schema left = _schema_info[left_order_key];
			Schema right = _schema_info[right_order_key];

			unsigned left_cost = plan_info[left_order_key].first;
			unsigned right_cost = plan_info[right_order_key].first;

			unsigned long int new_cost = left_cost + right_cost;
			if (i != 0)
				new_cost += left.GetTuplesNumber();
			if (i != permutation.size() - 2)
				new_cost += right.GetTuplesNumber();

			if (new_cost < min_cost) {

				CNF cnf;
				Schema out;
				if (cnf.ExtractCNF(_predicate, left, right) == -1) {
					cout << "failed in single cnf function" << endl;
					exit(-1);
				}
				JoinSizeEstimation(cnf, left, right, out);

				vector<int> new_order;
				vector<int> *left_opt_order = &plan_info[left_order_key].second;
				vector<int> *right_opt_order = &plan_info[right_order_key].second;
				for (int idx : *left_opt_order)
					new_order.emplace_back(idx);
				for (int idx : *right_opt_order)
					new_order.emplace_back(idx);

				optimal_order = new_order;

				min_cost = new_cost;
				optimal_schema = out;
			}
		}
	}
	plan_info[sorted_order_key] = {min_cost, optimal_order};
	_schema_info[sorted_order_key] = optimal_schema;
}

void QueryOptimizer::BuildTree(vector<string>& _tables, OptimizationTree* _root) {

	//TableList* current_table = _tables;

	//TODO: use cardinality values here

	string table_name_base = _tables[0];
	//current_table = current_table->next;

	unsigned long int no_tuples;

	OptimizationTree *left_base_tree = new OptimizationTree();
	catalog->GetNoTuples(table_name_base, no_tuples);
	left_base_tree->noTuples = no_tuples;

	OptimizationTree *inner_tree_ptr;
	inner_tree_ptr = left_base_tree;

	OptimizationTree *right_base_tree;
	OptimizationTree *inner_tree;

	int counter = 1;
	string table_name;
	while (counter < _tables.size()) {

		table_name = _tables[counter]; // current_table->tableName;

		right_base_tree = new OptimizationTree();
		catalog->GetNoTuples(table_name, no_tuples);
		right_base_tree->noTuples = no_tuples;

		counter++;

		inner_tree = new OptimizationTree();
		if (counter > 2) {
			inner_tree_ptr->parent = inner_tree;

			inner_tree->leftChild = inner_tree_ptr;
			for (unsigned int i = 0; i < inner_tree->leftChild->tables.size(); i++) {
				inner_tree->tables.push_back(inner_tree->leftChild->tables[i]);
				inner_tree->tuples.push_back(inner_tree->leftChild->tuples[i]);
			}
		} else {
			inner_tree->leftChild = left_base_tree;
			inner_tree->tables.push_back(table_name_base);
			inner_tree->tuples.push_back(left_base_tree->noTuples);
		}

		inner_tree->rightChild = right_base_tree;
		inner_tree->tables.push_back(table_name);
		inner_tree->tuples.push_back(no_tuples);

		inner_tree->noTuples = inner_tree->leftChild->noTuples + inner_tree->rightChild->noTuples;

		inner_tree_ptr = inner_tree;
		//current_table = current_table->next;
	}

	_root->tables = inner_tree_ptr->tables;
	_root->tuples = inner_tree_ptr->tuples;
	_root->noTuples = inner_tree_ptr->noTuples;
	_root->leftChild = inner_tree_ptr->leftChild;
	_root->rightChild = inner_tree_ptr->rightChild;
}
