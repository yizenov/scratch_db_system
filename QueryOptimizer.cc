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
	vector<int> table_indices;
	unordered_map<string, pair<unsigned long int, vector<int>>> dp_map;
    unordered_map<string, Schema> schema_info;
    vector<string> table_names;
    vector<int> table_tuples;

	// single table cardinality estimation
	TableList* current_table = _tables;
	while (current_table) {
		string table_name = current_table->tableName;
		table_names.emplace_back(table_name);

		Schema table_schema;
		catalog->GetSchema(table_name, table_schema);

		//TODO: check for type in join predicates
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
            table_tuples.emplace_back(estimate);
			Schema select_schema = table_select.GetSchemaOut();
            select_schema.SetTuplesNumber(estimate);
            schema_info[table_key] = select_schema;
			dp_map[table_key] = {0, {no_tables}};
		} else {
            Schema scan_schema = table_scan->GetSchemaOut();
            table_tuples.emplace_back(scan_schema.GetTuplesNumber());
            schema_info[table_key] = scan_schema;
			dp_map[table_key] = {0, {no_tables}};
		}

		table_indices.emplace_back(no_tables);
		no_tables++;
		current_table = current_table->next;
	}

    //OptimizationTree *temp = new OptimizationTree();
	if (table_names.size() == 2) {
        _root->leftChild = new OptimizationTree();
        _root->leftChild->tables.emplace_back(table_names[0]);
        _root->leftChild->tuples.emplace_back(table_tuples[0]);
        _root->leftChild->noTuples = table_tuples[0];
        _root->leftChild->parent = _root;

        _root->rightChild = new OptimizationTree();
        _root->rightChild->tables.emplace_back(table_names[1]);
        _root->rightChild->tuples.emplace_back(table_tuples[1]);
        _root->rightChild->noTuples = table_tuples[1];
        _root->rightChild->parent = _root;

        _root->tables = table_names;
        _root->tuples = table_tuples;

        Schema left = schema_info["_" + to_string(0)];
        Schema right = schema_info["_" + to_string(1)];
        Schema out;
        CNF cnf;
        if (cnf.ExtractCNF(*_predicate, left, right) == -1) {
            cout << "failed in single cnf function" << endl;
            exit(-1);
        }
        JoinSizeEstimation(cnf, left, right, out);

        _root->noTuples = out.GetTuplesNumber();
	} else {

        // two table join estimations
        for (int i = 0; i < no_tables - 1; i++) {
            Schema left = schema_info["_" + to_string(i)];
            for (int j = i + 1; j < no_tables; j++) {
                string join_key = "_" + to_string(i) + "_" + to_string(j);
                Schema right = schema_info["_" + to_string(j)];
                Schema out;
                CNF cnf;
                if (cnf.ExtractCNF(*_predicate, left, right) == -1) {
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
        vector<int> indicators;
        for (auto i : table_indices) {
            indices.emplace_back(i);
            indicators.emplace_back(0);
        }

        unordered_map<string, string> trees;

        find_optimal_plan(indices, *_predicate, _root, table_names, table_tuples, indicators, trees, schema_info, dp_map);
        sort(indices.begin(), indices.end());

        string plan_key = "";
        for (int idx : indices)
            plan_key += "_" + to_string(idx);

        cout << endl << "Table Names:";
        for (string t_name : table_names)
            cout << " " << t_name;
        cout << endl << "OPTIMAL PLAN: " << trees[plan_key] << endl;
        cout << "COST: " << dp_map[plan_key].first << endl;
        unsigned long int cardinality = schema_info[plan_key].GetTuplesNumber();
        cout << "CARDINALITY: " << cardinality << endl << endl;

        // input tree is already an optimal at this point
        string plan = trees[plan_key];
        vector<string> elements;
        ParsePlan(plan, elements);

        int start = 0;
        vector<OptimizationTree*> values;
        BuildTree(elements, start, _root, values, table_names, table_tuples, schema_info);
    }
}

void QueryOptimizer::ParsePlan(string& plan, vector<string>& elements) {

    unsigned int j = 0;
    string temp = "";
    for (unsigned int i = 0; i < plan.length(); i++) {
        if (plan[i] == '(') {
            if (j < i) {
                elements.emplace_back(temp);
                temp = "";
            }
            elements.emplace_back("(");
            j = i + 1;
        } else if (plan[i] == ')') {
            if (j < i) {
                elements.emplace_back(temp);
                temp = "";
            }
            elements.emplace_back(")");
            j = i + 1;
        } else if (plan[i] == '-') {
            elements.emplace_back(temp);
            temp = "";
            j = i + 1;
        } else {
            temp += plan[i];
        }
    }

}

void QueryOptimizer::BuildTree(vector<string>& plan, int& start, OptimizationTree* _root, vector<OptimizationTree*>& values,
        vector<string>& _table_names, vector<int>& _table_tuples, unordered_map<string, Schema>& _schema_info) {

    vector<OptimizationTree*> vec;

	while (start < plan.size()) {

		if (plan[start] == "(") {
			int l_start = start + 1;
			BuildTree(plan, l_start, _root, values, _table_names, _table_tuples, _schema_info);
			for (auto sub_tree : values) {
			    vec.emplace_back(sub_tree);
                values.pop_back();
			}
			start = l_start;

            if (vec.size() == 2) {

                OptimizationTree *join_tree = new OptimizationTree();

                vec[0]->parent = join_tree;
                join_tree->rightChild = vec.back();
                vec.pop_back();
                vec[1]->parent = join_tree;
                join_tree->leftChild = vec.back();
                vec.pop_back();

                for (string t_name : join_tree->leftChild->tables)
                    join_tree->tables.emplace_back(t_name);
                for (string t_name : join_tree->rightChild->tables)
                    join_tree->tables.emplace_back(t_name);
                for (int t_tuple : join_tree->leftChild->tuples)
                    join_tree->tuples.emplace_back(t_tuple);
                for (int t_tuple : join_tree->rightChild->tuples)
                    join_tree->tuples.emplace_back(t_tuple);
                join_tree->parent = _root;

                vector<int> local_indices;
                for (auto t_name : join_tree->leftChild->tables) {
                    for (auto i = 0; i < _table_names.size(); i++) {
                        if (t_name == _table_names[i]) {
                            local_indices.emplace_back(i);
                            break;
                        }
                    }
                }
                for (auto t_name : join_tree->rightChild->tables) {
                    for (auto i = 0; i < _table_names.size(); i++) {
                        if (t_name == _table_names[i]) {
                            local_indices.emplace_back(i);
                            break;
                        }
                    }
                }
                sort(local_indices.begin(), local_indices.end());
                string sorted_order_key = "";
                for (auto i : local_indices)
                    sorted_order_key += "_" + to_string(i);
                if (_schema_info.find(sorted_order_key) == _schema_info.end()) {
                    cout << "estimation result wasn't found" << endl;
                    exit(-1);
                } else {
                    unsigned long int join_est = _schema_info[sorted_order_key].GetTuplesNumber();
                    join_tree->noTuples = join_est;
                }

                vec.emplace_back(join_tree);

            } else if (vec.size() < 1 || vec.size() > 2) {
                cout << "bad parsing" << endl;
                exit(-1);
            }

		} else if (plan[start] == ")") {
		    start += 1;
            values.emplace_back(vec[0]);
            return;
        } else {
            int idx = -1;
            stringstream val(plan[start]);
            val >> idx;

            if (plan[start+1] == ")" || plan[start+1] == "(") {

                OptimizationTree *tree = new OptimizationTree();
                tree->parent = _root;
                string table_name = _table_names[idx];
                int no_tuples = _table_tuples[idx];
                tree->tables.emplace_back(table_name);
                tree->tuples.emplace_back(no_tuples);
                tree->noTuples = no_tuples;
                vec.emplace_back(tree);

                if (vec.size() == 2) {

                    OptimizationTree *join_tree = new OptimizationTree();

                    vec[0]->parent = join_tree;
                    join_tree->rightChild = vec.back();
                    vec.pop_back();
                    vec[1]->parent = join_tree;
                    join_tree->leftChild = vec.back();
                    vec.pop_back();

                    for (string t_name : join_tree->leftChild->tables)
                        join_tree->tables.emplace_back(t_name);
                    for (string t_name : join_tree->rightChild->tables)
                        join_tree->tables.emplace_back(t_name);
                    for (int t_tuple : join_tree->leftChild->tuples)
                        join_tree->tuples.emplace_back(t_tuple);
                    for (int t_tuple : join_tree->rightChild->tuples)
                        join_tree->tuples.emplace_back(t_tuple);
                    join_tree->parent = _root;

                    vector<int> local_indices;
                    for (auto t_name : join_tree->leftChild->tables) {
                        for (auto i = 0; i < _table_names.size(); i++) {
                            if (t_name == _table_names[i]) {
                                local_indices.emplace_back(i);
                                break;
                            }
                        }
                    }
                    local_indices.emplace_back(idx);
                    sort(local_indices.begin(), local_indices.end());
                    string sorted_order_key = "";
                    for (auto i : local_indices)
                        sorted_order_key += "_" + to_string(i);
                    if (_schema_info.find(sorted_order_key) == _schema_info.end()) {
                        cout << "estimation result wasn't found" << endl;
                        exit(-1);
                    } else {
                        unsigned long int join_est = _schema_info[sorted_order_key].GetTuplesNumber();
                        join_tree->noTuples = join_est;
                    }

                    vec.emplace_back(join_tree);
                }
                start += 1;
            } else {

                OptimizationTree *join_tree = new OptimizationTree();
                join_tree->parent = _root;

                join_tree->rightChild = new OptimizationTree();
                int right_idx = -1;
                stringstream val(plan[start+1]);
                val >> right_idx;
                string right_table_name = _table_names[right_idx];
                int right_no_tuples = _table_tuples[right_idx];
                join_tree->rightChild->tables.emplace_back(right_table_name);
                join_tree->rightChild->tuples.emplace_back(right_no_tuples);
                join_tree->rightChild->noTuples = right_no_tuples;
                join_tree->rightChild->parent = join_tree;

                join_tree->leftChild = new OptimizationTree();
                string left_table_name = _table_names[idx];
                int left_no_tuples = _table_tuples[idx];
                join_tree->leftChild->tables.emplace_back(left_table_name);
                join_tree->leftChild->tuples.emplace_back(left_no_tuples);
                join_tree->leftChild->noTuples = left_no_tuples;
                join_tree->leftChild->parent = join_tree;

                vector<int> local_indices = {idx, right_idx};
                sort(local_indices.begin(), local_indices.end());
                string sorted_order_key = "";
                for (auto i : local_indices)
                    sorted_order_key += "_" + to_string(i);
                if (_schema_info.find(sorted_order_key) == _schema_info.end()) {
                    cout << "estimation result wasn't found" << endl;
                    exit(-1);
                } else {
                    unsigned long int join_est = _schema_info[sorted_order_key].GetTuplesNumber();
                    join_tree->noTuples = join_est;
                }

                join_tree->tables.emplace_back(left_table_name);
                join_tree->tables.emplace_back(right_table_name);
                join_tree->tuples.emplace_back(left_no_tuples);
                join_tree->tuples.emplace_back(right_no_tuples);

                vec.emplace_back(join_tree);
                start += 2;
            }
		}
	}
    _root->tables = vec[0]->tables;
    _root->tuples = vec[0]->tuples;
    _root->noTuples = vec[0]->noTuples;
    _root->leftChild = vec[0]->leftChild;
    _root->leftChild->parent = _root;
    _root->rightChild = vec[0]->rightChild;
    _root->rightChild->parent = _root;
	return;
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

void QueryOptimizer::find_optimal_plan(vector<int>& indices, AndList& _predicate, OptimizationTree* _root,
        vector<string>& _table_names, vector<int>& _table_tuples, vector<int>& _indicators,
									   unordered_map<string, string>& trees,
        unordered_map<string, Schema>& _schema_info,
		unordered_map<string, pair<unsigned long int, vector<int>>>& plan_info) {

    vector<int> local_indices = indices;
    sort(local_indices.begin(), local_indices.end());

    string sorted_order_key = "";
    for (auto i : local_indices)
		sorted_order_key += "_" + to_string(i);
    if (_schema_info.find(sorted_order_key) != _schema_info.end()) {
        //_root == NULL;
        return;
    }

    vector<vector<int>> permutations;
    if (_indicators[indices.size() - 1] == 0) {
		generate_permutations(indices, indices.size(), indices.size(), permutations);
		_indicators[indices.size() - 1] = 1;
	} else {
        permutations.emplace_back(indices);
    }

	unsigned long int min_cost = ULONG_MAX;
	vector<int> optimal_order;
	Schema optimal_schema;
    vector<int> *left_opt_order, *right_opt_order;
    OptimizationTree *left_tree = NULL, *right_tree = NULL;

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

            left_tree = new OptimizationTree();
            right_tree = new OptimizationTree();
			if (_schema_info.find(left_order_key) == _schema_info.end())
				find_optimal_plan(origin_left_indices, _predicate, left_tree, _table_names, _table_tuples, _indicators, trees, _schema_info, plan_info);
			if (_schema_info.find(right_order_key) == _schema_info.end())
				find_optimal_plan(origin_right_indices, _predicate, right_tree, _table_names, _table_tuples, _indicators, trees, _schema_info, plan_info);

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

				left_opt_order = &plan_info[left_order_key].second;
				right_opt_order = &plan_info[right_order_key].second;

				string new_order_str = "(";
                vector<int> new_order;
				vector<string> left_table_names, right_table_names;

				vector<int> left_table_tuples, right_table_tuples;
				for (int idx : *left_opt_order) {
                    left_table_names.push_back(_table_names[idx]);
					left_table_tuples.push_back(_table_tuples[idx]);
                    new_order.emplace_back(idx);
                }

				for (int idx : *right_opt_order) {
					right_table_names.push_back(_table_names[idx]);
					right_table_tuples.push_back(_table_tuples[idx]);
                    new_order.emplace_back(idx);
                }

				min_cost = new_cost;
				optimal_schema = out;

				if (origin_left_indices.size() == 1) {
                    new_order_str += to_string(origin_left_indices[0]);
				} else if (origin_left_indices.size() == 2) {
                    new_order_str += "(" + to_string(origin_left_indices[0]);
                    new_order_str += "-" + to_string(origin_left_indices[1]) + ")";
				} else {
                    new_order_str += trees[left_order_key];
				}

                if (origin_right_indices.size() == 1) {
                    new_order_str +=  to_string(origin_right_indices[0]);
                } else if (origin_right_indices.size() == 2) {
                    new_order_str += "(" + to_string(origin_right_indices[0]);
                    new_order_str += "-" + to_string(origin_right_indices[1]) + ")";
                } else {
                    new_order_str += trees[right_order_key];
                }
                new_order_str += ")";

				trees[sorted_order_key] = new_order_str;

                optimal_order = new_order;
			}
		}
	}

	if (_schema_info.find(sorted_order_key) == _schema_info.end()) {
        plan_info[sorted_order_key] = {min_cost, optimal_order};
        _schema_info[sorted_order_key] = optimal_schema;
    } else {
	    //cout << sorted_order_key << endl;
	}

//    cout << "OPTIMAL JOIN PLAN (size: " << indices.size() << "):";
//    for (int table_id : optimal_order)
//        cout << " " << table_id;
//    cout << endl << "COST: " << min_cost << endl;
//    unsigned long int cardinality = optimal_schema.GetTuplesNumber();
//    cout << "CARDINALITY: " << cardinality << endl << endl;
}
