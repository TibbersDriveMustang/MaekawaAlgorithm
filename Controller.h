#pragma once
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include<string>
#include<errno.h>
#include <stdio.h>      /* for printf() and fprintf() */
#include <sys/socket.h> /* for socket(), bind(), and connect() */
#include <arpa/inet.h>  /* for sockaddr_in and inet_ntoa() */
#include <stdlib.h>     /* for atoi() and exit() */
#include <string.h>     /* for memset() */
#include <unistd.h>
#include "MessageFormat.h"
#include"communication.h"
using namespace std;

#define MAXNODES 16
#define QuorumSize 7
class Controller
{
private:
	int Algorithm;
    
public:
    
	int QuorumTable[MAXNODES][QuorumSize];
	char mapIPtoID[MAXNODES][MAXLENGTH_IP_ADDR];
    
	Controller(void);
	void handle(int clntSock1,char* client_ip,int counter,Controller *con);
	~Controller(void);
	void initiate(Controller *c);
	void decideAlgorithm();
	void Algorithm1();
	void Algorithm2();
	void sendTokenToNode();
	void UserInput();
	void sendCSrequests(int node);
};

