//============================================================================
// Name        : aos1.cpp
// Author      : kaushik sirineni
// Version     :
// Copyright   : all rights reserved @ kaushik.me
// Description : Mutual Exclusion
//============================================================================

#include <iostream>
#include <cstdlib>
#include <pthread.h>
#include "Starter.h"

using namespace std;

int main() {
	cout << "Mutual Exclusion" << endl; // prints Mutual Exclusion

	Starter s;
	s.init();

	return 0;
}
/**/


