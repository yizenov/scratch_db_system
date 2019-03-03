#ifndef _COMPLEX_SWAPIFY_H
#define _COMPLEX_SWAPIFY_H

#include "Schema.h"

template <class Type>
class ComplexSwapify {
private:
    Type data;

public:
    void Swap (ComplexSwapify& withMe);
    ComplexSwapify (const Type &castFromMe);
    void CopyFrom(ComplexSwapify& FromMe);
    operator Type();
    Type& GetData() { return data; };

    ComplexSwapify ();
    virtual ~ComplexSwapify ();
};


#endif //_COMPLEX_SWAPIFY_H
