#ifndef DB_SYSTEMS_COMPLEXSWAPIFY_H
#define DB_SYSTEMS_COMPLEXSWAPIFY_H

//
// Created by yizenov on 2/10/19.
//

#include <iostream>

#include "Schema.h"
#include "Swap.h"

using namespace std;

template <class Type>
class ComplexSwapify {
private:
    Type data;

public:
    void Swap (ComplexSwapify& withMe);
    ComplexSwapify (const Type castFromMe);
    void CopyFrom(ComplexSwapify& FromMe);
    operator Type();
    Type& getData() { return data; };

    ComplexSwapify ();
    virtual ~ComplexSwapify ();
};

typedef ComplexSwapify<Schema> SwapSchema;

#endif //DB_SYSTEMS_COMPLEXSWAPIFY_H
