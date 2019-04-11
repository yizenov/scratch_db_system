#ifndef _COMPLEXKEYIFY_H
#define _COMPLEXKEYIFY_H

//
// Class for comparing objects in Inefficient/Efficient map data structures
//

#include <iostream>

template <class Type>
class ComplexKeyify {
private:
    Type data;

public:
    void CopyFrom (ComplexKeyify &withMe);
    ComplexKeyify (const Type castFromMe);

    Type GetData() { return data; }
    operator Type();
    void Swap (ComplexKeyify &withMe);
    int IsEqual(ComplexKeyify &checkMe);
    int LessThan(ComplexKeyify &checkMe);

    ComplexKeyify();
    virtual ~ComplexKeyify ();
};

#endif //_COMPLEXKEYIFY_H
