#ifndef ALGO9_H_
#define ALGO9_H_

#include <iostream>
#include <vector>
#include "MessageFormat.h"
#include "communication.h"
#include "LexiQueue.h"
#include <pthread.h>

 using namespace std;
class Torum
{
protected:
	int ID;
	long sequenceNo;
	int HOLDER;
	int **quorum;
	int quorumsize;
	int NumNodes;
	bool inCS;
	//vector<vector<int>> quorum;;
	pthread_mutex_t sharedQLock;
	LexiQueue *queue;


public:
	communication com;
	static Torum* getInstance();
	void init();
	bool setID(int id);
	bool getQuorumTable(int **quorumtable,int qsize,int nnodes);
	bool requestCS();
	bool receiveRequest(Packet request);
	bool receiveToken(Packet request);
	bool receiveRelease(Packet request);
	bool receiveHaveTkn(Packet request);
	bool sendToken();

private:
	Torum(){};
	Torum(Torum const& copy){};
	Torum& operator=(Torum const& copy){};
	static Torum* instance;
	bool EnterTheCS();
};

#endif;
