CC = g++ -g -O0 -Wno-deprecated -std=gnu++11
LIBS = -lsqlite3

tag = -i

ifdef linux
	tag = -n
endif

main.out:	Schema.o Catalog.o main.o
	$(CC) -o main.out main.o Catalog.o Schema.o $(LIBS)
	
main.o:	main.cc
	$(CC) -c main.cc
	
Catalog.o: Catalog.h Catalog.cc	
	$(CC) -c Catalog.cc
	
Schema.o: Config.h Schema.h Schema.cc	
	$(CC) -c Schema.cc

clean: 
	rm -f *.o
	rm -f *.out
