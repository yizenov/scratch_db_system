#ifndef _QUERY_OPTIMIZER_H
#define _QUERY_OPTIMIZER_H

#include "Catalog.h"
#include "RelOp.h"
#include "ParseTree.h"
#include "Comparison.h"

#include <string>
#include <vector>
#include <unordered_map>

//class DBFile;
//class Scan;

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

    void ParsePlan(string& plan, vector<string>& elements);
	void BuildTree(vector<string>& plan, int& start, OptimizationTree* _root, vector<OptimizationTree*>& values,
            vector<string>& _table_names, vector<int>& _table_tuples, std::unordered_map<string, Schema>& _schema_info);

    unsigned long int EstimateCardinality(CNF& cnf, Schema& schema, AndList& _predicate);
    unsigned long int JoinSizeEstimation(CNF& predicate, Schema& schemaLeft, Schema& schemaRight, Schema& schemaOut);
    void generate_permutations(vector<int>& indices, int size, int n,
            vector<vector<int>>& permutations);
    void find_optimal_plan(vector<int>& indices, AndList& _predicate, OptimizationTree* _root,
            vector<string>& _table_names, vector<int>& _table_tuples, vector<int>& _indicators,
                           std::unordered_map<string, string>& trees,
            std::unordered_map<string, Schema>& _schema_info,
            std::unordered_map<string, std::pair<unsigned long int, vector<int>>>& plan_info);
};

#endif // _QUERY_OPTIMIZER_H
