CC = g++ -g -O0 -Wno-deprecated -std=gnu++14
LIBS = -lsqlite3

tag = -i

ifdef linux
	tag = -n
endif

main.out:	QueryParser.o QueryLexer.o Schema.o Swapify.o TwoWayList.o Record.o File.o DBFile.o Comparison.o Function.o RelOp.o Catalog.o ComplexSwapify.o InefficientMap.o Keyify.o QueryOptimizer.o QueryCompiler.o main.o
	$(CC) -o main.out main.o QueryParser.o QueryLexer.o Catalog.o ComplexSwapify.o InefficientMap.o Keyify.o Schema.o Record.o File.o DBFile.o Comparison.o Function.o RelOp.o Swapify.o TwoWayList.o QueryOptimizer.o QueryCompiler.o $(LIBS)
	
main.o:	main.cc
	$(CC) -c main.cc
	
Catalog.o: Catalog.cc InefficientMap.cc TwoWayList.cc
	$(CC) -c Catalog.cc

ComplexSwapify.o: ComplexSwapify.cc
	$(CC) -c ComplexSwapify.cc

#EfficientMap.o: EfficientMap.cc
#	$(CC) -c EfficientMap.cc

InefficientMap.o: InefficientMap.cc Keyify.cc
	$(CC) -c InefficientMap.cc

Keyify.o: Keyify.cc
	$(CC) -c Keyify.cc
	
Schema.o: Schema.cc
	$(CC) -c Schema.cc

Swapify.o: Swapify.cc
	$(CC) -c Swapify.cc

TwoWayList.o: TwoWayList.cc ComplexSwapify.cc
	$(CC) -c TwoWayList.cc

Record.o: Schema.cc Record.cc
	$(CC) -c Record.cc

File.o: Schema.cc Record.cc File.cc
	$(CC) -c File.cc

DBFile.o: Schema.cc Record.cc File.cc DBFile.cc
	$(CC) -c DBFile.cc

Comparison.o: Schema.cc Record.cc Comparison.cc
	$(CC) -c Comparison.cc

Function.o: Schema.cc Record.cc Function.cc
	$(CC) -c Function.cc

RelOp.o: Schema.cc Record.cc Comparison.cc RelOp.cc
	$(CC) -c RelOp.cc

QueryOptimizer.o: Schema.cc Record.cc Comparison.cc RelOp.cc QueryOptimizer.cc
	$(CC) -c QueryOptimizer.cc

QueryCompiler.o: Schema.cc Record.cc Comparison.cc RelOp.cc QueryOptimizer.cc QueryCompiler.cc
	$(CC) -c QueryCompiler.cc

QueryParser.o: QueryParser.y
	g++ -c QueryParser.c
# yacc --defines=QueryParser.h -o QueryParser.c QueryParser.y
# sed $(tag) QueryParser.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/"

QueryLexer.o: QueryLexer.l
	gcc -c QueryLexer.c
# lex -o QueryLexer.c QueryLexer.l

clean: 
	rm -f *.o
	rm -f *.out

#rm -f QueryLexer.c
#rm -f QueryParser.c
#rm -f QueryParser.h
