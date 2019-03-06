#ifndef _QUERY_COMPILER_H
#define _QUERY_COMPILER_H

/* Take as input the query tokens produced by the query parser and generate
 * the query execution tree. This requires instantiating relational operators
 * with the correct parameters, based on the query.
 * Two issues have to be addressed:
 *  1) Identify the schema(s) for each operator.
 *  2) Identify the parameters of the operation the operator is executing.
 *     For example, identify the predicate in a SELECT. Or the JOIN PREDICATE.
 */
#include "Catalog.h"
#include "ParseTree.h"
#include "QueryOptimizer.h"
#include "RelOp.h"
#include "InefficientMap.h"
#include "TwoWayList.h"
#include "Keyify.h"
#include "Config.h"


class QueryCompiler {
private:
	Catalog* catalog;
	QueryOptimizer* optimizer;

	ScanMap scanMap;
	SelectionMap selectionMap;

public:
	QueryCompiler(Catalog& _catalog, QueryOptimizer& _optimizer);
	virtual ~QueryCompiler();

	void Compile(TableList* _tables, NameList* _attsToSelect,
		FuncOperator* _finalFunction, AndList* _predicate,
		NameList* _groupingAtts, int& _distinctAtts,
		QueryExecutionTree& _queryTree);

    void CreateScans(TableList& _tables);
    void CreateSelects(AndList& _predicate);
    Join* CreateJoins(OptimizationTree& _root, AndList& _predicate);
    Project* CreateProjection(NameList& _attsToSelec, RelationalOp& _producer);
    Sum* CreateAggregators(FuncOperator& _finalFunction, RelationalOp& _producer);
    GroupBy* CreateGroupBy(NameList& _groupingAtts, RelationalOp& _producer);
};

#endif // _QUERY_COMPILER_H
