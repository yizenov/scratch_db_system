#include "ComplexKeyify.h"
#include "Swap.h"

using namespace std;

template <class Type>
ComplexKeyify <Type> :: ComplexKeyify () {
}

template <class Type>
ComplexKeyify <Type> :: ComplexKeyify (const Type castFromMe) {
    data = castFromMe;
}

template <class Type>
ComplexKeyify <Type> :: ~ComplexKeyify () {
}

template <class Type>
ComplexKeyify <Type> :: operator Type () {
    return data;
}

template <class Type> void
ComplexKeyify <Type> :: Swap (ComplexKeyify &withMe) {
    OBJ_SWAP(data, withMe.data);
}

template <class Type> void
ComplexKeyify <Type> :: CopyFrom (ComplexKeyify &withMe) {
    data = withMe.data;
}

template <class Type> int
ComplexKeyify <Type> :: IsEqual(ComplexKeyify &checkMe) {
    return (data == checkMe.data);
}

template <class Type> int
ComplexKeyify <Type> :: LessThan (ComplexKeyify &checkMe) {
    return (data < checkMe.data);
}

// redefine operator << for printing
template <class Type> ostream&
operator<<(ostream& output, const ComplexKeyify<Type>& _s) {
    ComplexKeyify<Type> newObject;
    newObject.Swap(const_cast<ComplexKeyify<Type>&>(_s));

    Type st = newObject;
    output << st;

    newObject.Swap(const_cast<ComplexKeyify<Type>&>(_s));

    return output;
}