#ifndef COMMUNICATION_H_
#define COMMUNICATION_H_

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
#include <unistd.h>     /* for close() */

#include "wqueue.h"
#include "MessageFormat.h"

#define MAXPENDING 5    /* Maximum outstanding connection requests */
#define RCVBUFSIZE 32   /* Size of receive buffer */

using namespace std;


class communication {
private:
	//000
	char **mapIDtoIP;
	void HandleTCPClient(int clntSocket, wqueue<Packet*>* m_queue);
	void DieWithError(string errorMessage);
public:
	void setMapIDtoIP(char** map);
	int sendMessageToID(struct Packet message,int id);
	int connectToServer(char dest_IP_Address[],int dest_port);
	int writeToSocket(int sockfd, void *buffer, int size);
	int readFromSocket(int sockfd, void *buffer, int size);
	int closeSocket(int sockfd);
	int sendMessage(Packet message,char dest_IP_Address[13],int dest_port);
	int serverListen(int portNum,wqueue<Packet*>* queue);
	int OpenListener(int& serfd,int port);


};

void *TorumListen(void* queue);
void *TorumProcess(void* queue);
void *MaekawaListen(void* queue);
void *MaekawaProcess(void* queue);

#endif /*COMMUNICATION_H_*/
