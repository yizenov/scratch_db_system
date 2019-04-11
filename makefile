#CC = gcc -g -O0 -Wno-deprecated
CC = g++ -g -O0 -Wno-deprecated -std=gnu++14
LIBS = -lsqlite3 -lfl

tag = -i

ifdef linux
	tag = -n
endif

# first line is the order how individual .o files are compiled
main.out: QueryParser.o QueryLexer.o Swapify.o ComplexSwapify.o Keyify.o Schema.o Record.o ComplexKeyify.o Catalog.o InefficientMap.o TwoWayList.o File.o DBFile.o Comparison.o Function.o RelOp.o QueryOptimizer.o QueryCompiler.o main.o
	$(CC) -o main.out main.o QueryParser.o QueryLexer.o Swapify.o ComplexSwapify.o Keyify.o Schema.o TwoWayList.o InefficientMap.o Catalog.o Record.o Comparison.o ComplexKeyify.o Function.o File.o DBFile.o RelOp.o QueryOptimizer.o QueryCompiler.o $(LIBS)
# linking logic: dependents are mantioned first and then independent libraries
# $(CC) main.o QueryCompiler.o QueryOptimizer.o RelOp.o Function.o DBFile.o File.o Comparison.o ComplexKeyify.o Record.o Schema.o Catalog.o InefficientMap.o TwoWayList.o Keyify.o Swapify.o ComplexSwapify.o QueryParser.o QueryLexer.o $(LIBS) -o main.out

QueryParser.o: QueryParser.y
	yacc --defines=QueryParser.h -o QueryParser.c QueryParser.y
	sed $(tag) QueryParser.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/"
	g++ -c QueryParser.c

QueryLexer.o: QueryLexer.l
	lex -o QueryLexer.c QueryLexer.l
	gcc -c QueryLexer.c

Swapify.o: Swapify.h Swapify.cc
	$(CC) -c Swapify.cc

ComplexSwapify.o: ComplexSwapify.h ComplexSwapify.cc
	$(CC) -c ComplexSwapify.cc

Keyify.o: Keyify.h Keyify.cc
	$(CC) -c Keyify.cc

TwoWayList.o: Swapify.h ComplexSwapify.h TwoWayList.h TwoWayList.cc
	$(CC) -c TwoWayList.cc

Schema.o: Swap.h Config.h Schema.h Schema.cc
	$(CC) -c Schema.cc

#EfficientMap.o: EfficientMap.h EfficientMap.cc
#	$(CC) -c EfficientMap.cc

InefficientMap.o: Schema.h TwoWayList.h Swapify.h Keyify.h InefficientMap.h InefficientMap.cc
	$(CC) -c InefficientMap.cc

Catalog.o: TwoWayList.h InefficientMap.o Catalog.h Catalog.cc
	$(CC) -c Catalog.cc
#InefficientMap.cc comile this as well

Record.o: Swap.h Config.h Schema.h Comparison.h Record.h Record.cc
	$(CC) -c Record.cc

ComplexKeyify.o: Record.h ComplexKeyify.h ComplexKeyify.cc
	$(CC) -c ComplexKeyify.cc

File.o: Config.h Keyify.h Record.h TwoWayList.h File.h File.cc
	$(CC) -c File.cc

DBFile.o: Config.h Swap.h Schema.h Record.h File.h DBFile.h DBFile.cc
	$(CC) -c DBFile.cc

Comparison.o: Swap.h Config.h ParseTree.h Schema.h Record.h Comparison.h Comparison.cc
	$(CC) -c Comparison.cc

Function.o: Config.h ParseTree.h Schema.h Record.h Function.h Function.cc
	$(CC) -c Function.cc

RelOp.o: Swap.h Config.h Schema.h Record.h DBFile.h Function.h Comparison.h Keyify.h Swapify.h InefficientMap.h RelOp.h RelOp.cc
	$(CC) -c RelOp.cc

QueryOptimizer.o: Schema.h Catalog.h Comparison.h RelOp.h ParseTree.h QueryOptimizer.h QueryOptimizer.cc
	$(CC) -c QueryOptimizer.cc

# define the order of its dependencies
QueryCompiler.o: Schema.h Record.h Comparison.h RelOp.h QueryOptimizer.h QueryCompiler.h QueryCompiler.cc
	$(CC) -c QueryCompiler.cc

# replacing dependeies .h and .cpp with .o
main.o:	Catalog.h QueryOptimizer.h QueryCompiler.h main.cc
	$(CC) -c main.cc

clean: 
	rm -f *.o
	rm -f *.out
	rm -f QueryLexer.c
	rm -f QueryParser.c
	rm -f QueryParser.h
