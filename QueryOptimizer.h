#ifndef _QUERY_OPTIMIZER_H
#define _QUERY_OPTIMIZER_H

#include "Schema.h"
#include "Catalog.h"
#include "ParseTree.h"
#include "RelOp.h"

#include <string>
#include <vector>
#include <unordered_map>


// data structure used by the optimizer to compute join ordering
struct OptimizationTree {
	// list of tables joined up to this node
	vector<string> tables;
	// number of tuples in each of the tables (after selection predicates)
	vector<int> tuples;
	// number of tuples at this node
	unsigned long int noTuples;

	// connections to children and parent
	OptimizationTree* parent;
	OptimizationTree* leftChild;
	OptimizationTree* rightChild;
};

class QueryOptimizer {
private:
	Catalog* catalog;

public:
	QueryOptimizer(Catalog& _catalog);
	virtual ~QueryOptimizer();

	void Optimize(TableList* _tables, AndList* _predicate, OptimizationTree* _root);

    void BuildTree(vector<string>& _tables, OptimizationTree* _root);
    unsigned long int EstimateCardinality(CNF& cnf, Schema& schema, AndList& _predicate);
    unsigned long int JoinSizeEstimation(CNF& predicate, Schema& schemaLeft, Schema& schemaRight, Schema& schemaOut);
    void generate_permutations(vector<int>& indices, int size, int n,
            vector<vector<int>>& permutations);
    void find_optimal_plan(vector<int>& indices, AndList& _predicate,
            unordered_map<string, Schema>& _schema_info,
            unordered_map<string, pair<unsigned long int, vector<int>>>& plan_info);
};

#endif // _QUERY_OPTIMIZER_H
