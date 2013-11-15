

#include"communication.h"
#include <iostream>
using namespace std;

/*struct AlgoMsg{
	int TYPE; // 0: request_token, 1: have-token, 2: release, 3:send-token
	int ORIGIN;
	long SEQ;
	int sender;
}__attribute__((packed));*/





/*int main(int argc, char *argv[])
{
	AlgoMsg message;
	message.TYPE=1;
	message.ORIGIN=2;
	message.SEQ=12121;
	message.sender=4;
	communication c;
	
	
	strcpy(c.dest_IP_Address,"10.176.67.65");
	printf(c.dest_IP_Address);
	c.dest_port=1235;
	c.sendMessage(message);
	return 0;
}*/
void communication::setMapIDtoIP(char **map){
	mapIDtoIP = map;
}
int communication::connectToServer(char dest_IP_Address[15],int dest_port){
		int sockfd;
		struct sockaddr_in servaddr;

		/* creating a socket */
		if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
			printf("\n Error in socket");
			exit(0);
		}
	    printf("Socket created, ");

	    /* configuring server address structure */
		bzero(&servaddr, sizeof(servaddr));
		servaddr.sin_family = AF_INET;
		servaddr.sin_port = htons(dest_port);
		printf("Socket configured, ");
		if (inet_pton(AF_INET, dest_IP_Address, &servaddr.sin_addr) <= 0) {
				printf("\n Error in inet_pton");
				exit(0);
			}

		/* connecting to the server */
		if (connect(sockfd, (struct sockaddr *) &servaddr, sizeof(servaddr)) < 0) {
			printf("\nError in connect");
			exit(0);
		}
		printf("Socket connected to server \n");
		return sockfd;
}

int communication::writeToSocket(int sockfd, void *buffer, int size){
	ssize_t n_send = send(sockfd,(void*) buffer, size, 0);
	if (n_send < 0) {
		DieWithError("\nError in Sending\n");
	}
	// printf("message sent");
	return n_send;
}
int communication::readFromSocket(int sockfd, void *buffer, int size){
	ssize_t n_recv = recv(sockfd, (void*) buffer, size, 0);
		if (n_recv < 0) {
			DieWithError("\nError in Receiving\n");
		}
	cout<<"--Msg Received "<<buffer<<"---"<<endl;
	return n_recv;
}

int communication::closeSocket(int sockfd){
	return close(sockfd);
}

int communication::sendMessage(struct Packet message, char dest_IP_Address[15],int dest_port){
	int sockfd = connectToServer(dest_IP_Address,dest_port);
	writeToSocket(sockfd,(void*)&message,sizeof(message));
	return closeSocket(sockfd);

}
//000
int communication::sendMessageToID(struct Packet message,int id){
	printf("sending a message to %d , IP: %s\n",id,mapIDtoIP[id]);
	return sendMessage(message,mapIDtoIP[id],LISTEN_PORT3);
}


void communication::DieWithError(string errorMessage)/* Error handling function */
{

    perror(errorMessage.c_str());
    exit(1);
}

void communication::HandleTCPClient(int clntSocket, wqueue<Packet*>* m_queue)
{
	printf("Entered handling client");

    //char echoBuffer[RCVBUFSIZE] = {'\0'};        /* Buffer for echo string */
    int recvMsgSize;                    /* Size of received message */
    Packet message;
    

    /* Receive message from client */
   /* if ((recvMsgSize = recv(clntSocket, echoBuffer, RCVBUFSIZE, 0)) < 0)
        DieWithError("recv() failed");*/
    if ((recvMsgSize = recv(clntSocket, &message, sizeof(message), 0)) < 0)
           DieWithError("recv() failed");
	printf("Message type %d\n",message.TYPE);
	printf("Message Originator %d\n", message.ORIGIN);
	printf("Message Sequ %ld\n", message.SEQ);
	printf("Message sender %d\n", message.sender);

	//if message is an Algorithm message add to queue
	if(message.TYPE >=0 && message.TYPE <100){
	Packet *message1;
	message1 = (struct Packet *)malloc(sizeof(struct Packet));
	message1->TYPE=message.TYPE;
	message1->ORIGIN=message.ORIGIN;
	message1->SEQ=message.SEQ;
	message1->sender=message.sender;
	printf("adding to recv queue\n");
	m_queue->add(message1);
	printf("queue size form listener:%d\n",m_queue->size());
	}//is message a technical message
	else if(message.TYPE >=100 && message.TYPE < 1000 ){

	}
    /* Send received string and receive again until end of transmission */
    
  /*  while (recvMsgSize > 0)       zero indicates end of transmission 
    {
         Echo message back to client 
        if (send(clntSocket, echoBuffer, recvMsgSize, 0) != recvMsgSize)
            DieWithError("send() failed");

         See if there is more data to receive 
        if ((recvMsgSize = recv(clntSocket, echoBuffer, RCVBUFSIZE, 0)) < 0)
            	DieWithError("recv() failed");

             Terminate the string! 
        	printf("%s\n", echoBuffer);


    }*/


}

int communication::serverListen(int portNum,wqueue<Packet*>* queue)
{
    int servSock;                    /* Socket descriptor for server */
    int clntSock;                    /* Socket descriptor for client */
    struct sockaddr_in echoServAddr; /* Local address */
    struct sockaddr_in echoClntAddr; /* Client address */
    unsigned short echoServPort;     /* Server port */
    socklen_t clntLen;            /* Length of client address data structure */

   
    echoServPort = portNum;  /* First arg:  local port */

    /* Create socket for incoming connections */
    if ((servSock = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0)
        DieWithError("socket() failed");

    /* Construct local address structure */
    memset(&echoServAddr, 0, sizeof(echoServAddr));   /* Zero out structure */
    echoServAddr.sin_family = AF_INET;                /* Internet address family */
    echoServAddr.sin_addr.s_addr = htonl(INADDR_ANY); /* Any incoming interface */
    echoServAddr.sin_port = htons(echoServPort);      /* Local port */

    /* Bind to the local address */
    if (bind(servSock, (struct sockaddr *) &echoServAddr, sizeof(echoServAddr)) < 0)
        DieWithError("bind() failed");

    /* Mark the socket so it will listen for incoming connections */
    if (listen(servSock, MAXPENDING) < 0)
        DieWithError("listen() failed");
    //int del=0;
    for (;;) /* Run forever */
    {
        /* Set the size of the in-out parameter */
        clntLen = sizeof(echoClntAddr);

        /* Wait for a client to connect */
        if ((clntSock = accept(servSock, (struct sockaddr *) &echoClntAddr,&clntLen)) < 0)
            DieWithError("accept() failed");

        /* clntSock is connected to a client! */
        char *client_ip = inet_ntoa(echoClntAddr.sin_addr);
        printf("\nRequesting client socket %d, addlen : %d %s\n",clntSock,sizeof(client_ip),client_ip);
        printf("Handling Node at %s\n", inet_ntoa(echoClntAddr.sin_addr));
        HandleTCPClient(clntSock,queue);
        //del++;
        close(clntSock);    /* Close client socket */

    }
    return 0;
    /* NOT REACHED */
}

int communication::OpenListener(int& serSock, int portNum){
	int servSock;                    /* Socket descriptor for server */
	int clntSock;                    /* Socket descriptor for client */
	close(servSock);
	struct sockaddr_in echoServAddr; /* Local address */
	struct sockaddr_in echoClntAddr; /* Client address */
	unsigned short echoServPort;     /* Server port */
	socklen_t clntLen;            /* Length of client address data structure */

	echoServPort = portNum;  /* First arg:  local port */

	/* Create socket for incoming connections */
	if ((servSock = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0)
		DieWithError("socket() failed");

	/* Construct local address structure */
	memset(&echoServAddr, 0, sizeof(echoServAddr));   /* Zero out structure */
	echoServAddr.sin_family = AF_INET;                /* Internet address family */
	echoServAddr.sin_addr.s_addr = htonl(INADDR_ANY); /* Any incoming interface */
	echoServAddr.sin_port = htons(echoServPort);      /* Local port */

    /* Bind to the local address */
    if (bind(servSock, (struct sockaddr *) &echoServAddr, sizeof(echoServAddr)) < 0)
        DieWithError("bind() failed");

    /* Mark the socket so it will listen for incoming connections */
    if (listen(servSock, MAXPENDING) < 0)
        DieWithError("listen() failed");
    /* Set the size of the in-out parameter */
	clntLen = sizeof(echoClntAddr);

	/* Wait for a client to connect */
	if ((clntSock = accept(servSock, (struct sockaddr *) &echoClntAddr,&clntLen)) < 0)
		DieWithError("accept() failed");

	/* clntSock is connected to a client! */
	char *client_ip = inet_ntoa(echoClntAddr.sin_addr);
	printf("\nRequesting client socket %d, addlen : %d %s\n",clntSock,sizeof(client_ip),client_ip);
	printf("Handling message from Node at %s\n", inet_ntoa(echoClntAddr.sin_addr));

	serSock = servSock;
	return clntSock;
}




