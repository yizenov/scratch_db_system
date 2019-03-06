#include <string>
#include <vector>
#include <iostream>

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

	//TODO: in case of single table, no join happens

	//TODO: assumptions: left-deep tree, no optimization so far, no push-down

	TableList* current_table = _tables;

	string table_name_base = current_table->tableName;
	current_table = current_table->next;

	//TODO: choose formula to estimate survived tuples
	unsigned int no_tuples;

	OptimizationTree *left_base_tree = new OptimizationTree();
	catalog->GetNoTuples(table_name_base, no_tuples);
	left_base_tree->noTuples = no_tuples;

	OptimizationTree *inner_tree_ptr;
	inner_tree_ptr = left_base_tree;

    OptimizationTree *right_base_tree;
    OptimizationTree *inner_tree;

	int counter = 1;
	string table_name;
	while (current_table) {

		table_name = current_table->tableName;

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
		current_table = current_table->next;
	}

	_root->tables = inner_tree_ptr->tables;
	_root->tuples = inner_tree_ptr->tuples;
	_root->noTuples = inner_tree_ptr->noTuples;
	_root->leftChild = inner_tree_ptr->leftChild;
	_root->rightChild = inner_tree_ptr->rightChild;
}
