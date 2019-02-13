CC = g++ -g -O0 -Wno-deprecated -std=gnu++11
LIBS = -lsqlite3

tag = -i

ifdef linux
	tag = -n
endif

main.out:	main.o Catalog.o ComplexSwapify.o InefficientMap.o Keyify.o Schema.o Swapify.o TwoWayList.o
	$(CC) -o main.out main.o Catalog.o ComplexSwapify.o InefficientMap.o Keyify.o Schema.o Swapify.o TwoWayList.o $(LIBS)
	
main.o:	main.cc
	$(CC) -c main.cc
	
Catalog.o: Catalog.h Catalog.cc	InefficientMap.cc TwoWayList.cc
	$(CC) -c Catalog.cc

ComplexSwapify.o: ComplexSwapify.h ComplexSwapify.cc Schema.h
	$(CC) -c ComplexSwapify.cc

#EfficientMap.o: EfficientMap.h EfficientMap.cc
#	$(CC) -c EfficientMap.cc

InefficientMap.o: InefficientMap.h InefficientMap.cc TwoWayList.h Keyify.cc
	$(CC) -c InefficientMap.cc

Keyify.o: Keyify.h Keyify.cc
	$(CC) -c Keyify.cc
	
Schema.o: Schema.h Schema.cc Swap.h	Config.h
	$(CC) -c Schema.cc

Swapify.o: Swapify.h Swapify.cc
	$(CC) -c Swapify.cc

TwoWayList.o: TwoWayList.h TwoWayList.cc ComplexSwapify.cc Swapify.h
	$(CC) -c TwoWayList.cc

clean: 
	rm -f *.o
	rm -f *.out
