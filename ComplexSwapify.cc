#include "ComplexSwapify.h"
#include "Swap.h"

#include <iostream>

using namespace std;


template <class Type>
ComplexSwapify <Type> :: ComplexSwapify () {
}

template <class Type>
ComplexSwapify <Type> :: ComplexSwapify (const Type &castFromMe) {
    data = castFromMe;
}

template <class Type>
ComplexSwapify <Type> :: ~ComplexSwapify () {
}

template <class Type>
ComplexSwapify <Type> :: operator Type () {
    return data;
}

template <class Type> int
ComplexSwapify <Type> :: IsEqual(ComplexSwapify &checkMe) {
    return (data == checkMe.data);
}

template <class Type> int
ComplexSwapify <Type> :: LessThan (ComplexSwapify &checkMe) {
    return (data < checkMe.data);
}

template <class Type> void
ComplexSwapify <Type> :: Swap (ComplexSwapify &withMe) {
    OBJ_SWAP(data, withMe.data);
}

template <class Type> void
ComplexSwapify <Type> :: CopyFrom (ComplexSwapify &fromMe) {
    data = fromMe.data;
}

// redefine operator << for printing
template <class Type> ostream&
operator<<(ostream& output, const ComplexSwapify<Type>& _s) {
    ComplexSwapify<Type> newObject;
    newObject.Swap(const_cast<ComplexSwapify<Type>&>(_s));

    Type st = newObject;
    output << st;

    newObject.Swap(const_cast<ComplexSwapify<Type>&>(_s));

    return output;
}
