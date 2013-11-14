/*
 * Starter.h
 *
 *  Created on: Nov 6, 2013
 *      Author: kaushik sirineni
 */

#ifndef STARTER_H_
#define STARTER_H_

#include <vector>
#include <string>
#include <iostream>
#include <pthread.h>
#include "Algo9.h"
#include "communication.h"
#include "MaekawaAlgorithm.h"

using namespace std;

class Starter {
private:
	Torum *node;
    MaekawaAlgorithm *mnode;
	int quorumSize;
	int NumNodes;
	int **Quorum;
	int id;
	char **mapIDtoIP;
	void parseMsg(const string& s,const string& delim,std::vector<string>& tokens);
public:
	Starter();
	virtual ~Starter();
	void init();
	void registerAtController(char controllerIP[],int port);
	void decideAlgorithm();
	void Algorithm1();
	void Algorithm2();
	void processing();
    
};

#endif /* STARTER_H_ */
