#include "Starter.h"

Starter::Starter() {
	printf("In Starter\n");
	
	//decideAlgorithm();
}

Starter::~Starter() {
	for(int i = 0; i < NumNodes; ++i)
		delete Quorum[i];
	delete Quorum;
}

void Starter::init(){
	char controllerIP[15] = "10.176.67.93";		//net30
	//char controllerIP[15] = "10.176.67.108";	//net45
	//char controllerIP[16] = "10.176.67.88";	//net25
	int port = LISTEN_PORT;
	registerAtController(controllerIP, port);
	decideAlgorithm();
}

void Starter::parseMsg(const string& s,const string& delim,std::vector<string>& tokens)
{
    size_t s0 = 0, s1 = string::npos;
    string token;
    while(s0 != string::npos)
    {
        
        s1 = s.find_first_of(delim, s0);
        
        // if(s1 != s0 && s1 != string::npos) // the second check added for space
        if(s1 != s0) // no need check Defect FIX  1,2 -> 2 will not be added in the list
        {
            token = s.substr(s0, s1 - s0);
            
            if( token != "")
            {
                //cout<<"Token :"<<token<<" length:"<<token.length()<<endl;
                //cout<<token<<endl;
                tokens.push_back(token);
            }
        }
        s0 = s.find_first_not_of(delim, s1);
    }
    //EXIT
    token.clear();
    return;
    
}

/*
 * send a message to controller when a node starts on a amachine.
 */
void Starter::registerAtController(char controllerIP[15],int port){
	//communication
    
	communication com;
	int sockfd = com.connectToServer(controllerIP,port);
    
	int z;
	char buf[128]={'\0',};
	z = gethostname(buf,sizeof buf);
	if ( z == -1 ) {
		fprintf(stderr, "%s: gethostname(2)\n",
                strerror(errno));
		exit(1);
	}
	int sd = 999;
	printf("\nContacted Controller, sent my hostname:%s\n",buf);
	com.writeToSocket(sockfd,buf,128);
	com.readFromSocket(sockfd,&id,sizeof(int));
	int qsize,nsize;
	com.readFromSocket(sockfd,&qsize,sizeof(int));
	com.readFromSocket(sockfd,&nsize,sizeof(int));
	//int size;
	//com.readFromSocket(sockfd,&size,sizeof(int));
	printf("Received ID: %d, Qsize: %d, NumNodes: %d\n",id,qsize,nsize);
	NumNodes = nsize;
	quorumSize = qsize;
	Quorum = new int*[nsize];
	for(int i = 0; i < nsize; ++i)
		Quorum[i] = new int[qsize];
	
	for (int i = 0; i < nsize; ++i) {
        for (int j = 0; j < qsize; ++j) {
            Quorum[i][j] = 0;
        }
    }
    
	puts("Getting quorum table\n");
	char recvdMsg[4096] = {'\0',};
	com.readFromSocket(sockfd,recvdMsg,4095);
	string recvdStr = recvdMsg;
	vector<string> recvdValues;
	string delimiter = ":";
	parseMsg(recvdStr,delimiter,recvdValues);
	/*
     for (int i=0; i<recvdValues.size();i++)
     {
     cout<<recvdValues[i]<<endl; //<-- extract values from this vector and push into the attay in 123 line
     
     }*/
	for(int i=0; i<nsize;i++)
	{
		for(int j=0; j<qsize;j++)
		{
			//cout<<"Here"<<endl;
			Quorum[i][j] = atoi(recvdValues[i*qsize+j].c_str());
			//cout<<"Here1"<<endl;
		}
	}
	
	for(int i=0;i<nsize;i++){
		for(int j=0;j<qsize;j++){
            
			printf("%d\t",Quorum[i][j]);
		}
		printf("\n");
		
	}
	shutdown(sockfd,2);
	int k = com.closeSocket(sockfd);
	if (k < 0) {
        printf("\nError in Closing");
        exit(0);
	}else
		printf("Node disconnected from Controller\n");
    
}

void Starter::decideAlgorithm(){
	printf("\nIn decideAlgorithm() of Node\n");
	communication com;
	int serfd;
	int clifd = com.OpenListener(serfd,LISTEN_PORT2);
	//char buffer[4095]={'\0',};
	int intbuf;
	com.readFromSocket(clifd,&intbuf,sizeof(int));
	printf("recvd: %d\n",intbuf);
	puts("Getting ID to IP map\n");
	//assigining memory to mapIDtoIP
	mapIDtoIP = new char*[NumNodes];
	for(int i = 0; i < NumNodes; ++i){
		mapIDtoIP[i] = new char[MAXLENGTH_IP_ADDR];
	}
    
	char recvdMsg[4096] = {'\0',};
	strcpy(mapIDtoIP[0],"hello");
	com.readFromSocket(clifd,recvdMsg,4095);
	string recvdStr = recvdMsg;
	vector<string> recvdValues;
	string delimiter = ":";
	parseMsg(recvdStr,delimiter,recvdValues);
	for(int i=0;i<NumNodes;i++){
		strcpy(mapIDtoIP[i],recvdValues[i].c_str());
		printf("%d\t%s\n",i,mapIDtoIP[i]);
	}
	shutdown(clifd,2);
	com.closeSocket(clifd);
	com.closeSocket(serfd);
	int algo = intbuf;
	if(algo == 1){
		Algorithm1();
	}else if(algo == 2){
		Algorithm2();
        
	}else
		printf("Invalid input\n");
    
}

void Starter::Algorithm1(){
	printf("Starting Algorithm 1: Maekawa\n");
    mnode = MaekawaAlgorithm::getInstance();
    mnode->initialization();
    mnode->setProcessID(id);
    mnode->getQuorumTable(Quorum,quorumSize, NumNodes);
    
    mnode->com.setMapIDtoIP(mapIDtoIP);
    
    wqueue<Packet*> queue;
    pthread_t ListenerThread;
	pthread_create(&ListenerThread, NULL, MaekawaListen, (void *)&queue);
    
	sleep(1);
	pthread_t ProcessingThread;
	pthread_create(&ProcessingThread, NULL, MaekawaProcess,(void *)&queue);
    
	pthread_join(ListenerThread, NULL);
	pthread_join(ProcessingThread, NULL);
    
	printf("Maekawa Algorithm execution done\n");
}

void Starter::Algorithm2(){
	printf("Starting Algorithm 2: Torum\n");
	node = Torum::getInstance();
	node->init();
	node->setID(id);
	node->getQuorumTable(Quorum,quorumSize,NumNodes);
	//000
	node->com.setMapIDtoIP(mapIDtoIP);
    
    
	wqueue<Packet*> queue;
    
	pthread_t ListenerThread;
	pthread_create(&ListenerThread, NULL, TorumListen, (void *)&queue);
    
	sleep(1);
	pthread_t ProcessingThread;
	pthread_create(&ProcessingThread, NULL, TorumProcess,(void *)&queue);
    
	pthread_join(ListenerThread, NULL);
	pthread_join(ProcessingThread, NULL);
    
	printf("Torum execution done\n");
}

void *TorumListen(void* queue) {
	printf("TorumListener Thread created\n");
	wqueue<Packet*> *m_queue=((wqueue<Packet*>*)queue);
	communication com;
	com.serverListen(LISTEN_PORT3,m_queue);
	return NULL;
}


void *TorumProcess(void* queue) {
	printf("TorumProcess Thread created\n");
	wqueue<Packet*>*m_queue=(wqueue<Packet*>*)queue;
	Torum *node = Torum::getInstance();
	sleep(5);
	// Remove 1 item at a time and process it. Blocks if no items are
	// available to process.
	for (int i = 0;; i++) {
		printf("TorumProcessing Thread , loop %d - waiting for item...\n", i);
		Packet* item = m_queue->remove();
		//printf("thread  loop %d - got one item\n", i);
		//printf("Received: messageType - %d, SEQ number - %ld\n",item->TYPE, item->SEQ);
        
		if (item->TYPE == SEND_TOKEN){
			printf("SEND_TOKEN recieved from Node %d and packet type is %d\n",item->ORIGIN,item->TYPE);
			node->receiveToken(*item);
		}else if (item->TYPE == MAKE_REQUEST){
			printf("MAKE_REQUEST recieved from Controller %d and packet type is %d\n",item->ORIGIN,item->TYPE);
			node->requestCS();
		}else if (item->TYPE == HAVE_TOKEN){
			printf("HAVE_TOKEN recieved from Node %d and packet type is %d\n",item->ORIGIN,item->TYPE);
			node->receiveHaveTkn(*item);
		}else if (item->TYPE == RELEASE){
			printf("RELEASE recieved from Node %d and packet type is %d\n",item->ORIGIN,item->TYPE);
			node->receiveRelease(*item);
		}else if (item->TYPE == REQUEST){
			printf("REQUEST recieved from Node %d and packet type is %d\n",item->ORIGIN,item->TYPE);
			node->receiveRequest(*item);
		}
		delete item;
	}
	return NULL;
}

void *MaekawaListen(void* queue) {
	printf("MaekawaListener Thread created\n");
	wqueue<Packet*> *m_queue=((wqueue<Packet*>*)queue);
	communication com;
	com.serverListen(LISTEN_PORT3,m_queue);
	return NULL;
}


void *MaekawaProcess(void* queue) {
	printf("MaekawaProcess Thread created\n");
	wqueue<Packet*>*m_queue=(wqueue<Packet*>*)queue;
	MaekawaAlgorithm *mnode = MaekawaAlgorithm::getInstance();
	sleep(5);
	// Remove 1 item at a time and process it. Blocks if no items are
	// available to process.
	for (int i = 0;; i++) {
		printf("MaekawaProcessing Thread , loop %d - waiting for item...\n", i);
		Packet* item = m_queue->remove();
		//printf("thread  loop %d - got one item\n", i);
		printf("Received: messageType - %d, SEQ number - %ld\n",item->TYPE, item->SEQ);
        
        switch (item->TYPE) {
            case MAKE_REQUEST:
                printf(" ## MAKE_REQUEST received from Node %d and packet type is %d\n",item->ORIGIN,item->TYPE);
                mnode->receiveMakeRequest(*item);
                break;
            case REQUEST:
                printf(" ## REQUEST received from Node %d and packet type is %d\n",item->ORIGIN,item->TYPE);
                mnode->receiveRequest(*item);
                break;
            case INQUIRE:
                printf(" ## INQUIRE received from Node %d and packet type is %d\n",item->ORIGIN,item->TYPE);
                mnode->receiveInquire(*item);
                break;
            case FAILED:
                printf(" ## FAILED received from Node %d and packet type is %d\n",item->ORIGIN,item->TYPE);
                mnode->receiveFailed(*item);
                break;
            case RELEASE:
                printf(" ## RELEASE received from Node %d and packet type is %d\n",item->ORIGIN,item->TYPE);
                mnode->receiveRelease(*item);
                break;
            case RELINQUISH:
                printf(" ## RELINQUISH received from Node %d and packet type is %d\n",item->ORIGIN,item->TYPE);
                mnode->receiveRelinquish(*item);
                break;
            case LOCKED:
                printf(" ## LOCKED received from Node %d and packet type is %d\n",item->ORIGIN,item->TYPE);
                mnode->receiveLocked(*item);
                break;
                
            default:
                break;
        }
        delete item;
	}
	return NULL;
}

//#include "Starter.h"
//
//Starter::Starter() {
//	printf("In Starter\n");
//	
//	//decideAlgorithm();
//}
//
//Starter::~Starter() {
//	for(int i = 0; i < NumNodes; ++i)
//		delete Quorum[i];
//	delete Quorum;
//}
//
//void Starter::init(){
//	char controllerIP[15] = "10.176.67.108";
//	//char controllerIP[16] = "10.176.67.88";
//	int port = LISTEN_PORT;
//	registerAtController(controllerIP, port);
//	decideAlgorithm();
//}
//
//void Starter::parseMsg(const string& s,const string& delim,std::vector<string>& tokens)
//{
//        size_t s0 = 0, s1 = string::npos;
//        string token;
//        while(s0 != string::npos)
//          {
//
//            s1 = s.find_first_of(delim, s0);
//
//                // if(s1 != s0 && s1 != string::npos) // the second check added for space
//				if(s1 != s0) // no need check Defect FIX  1,2 -> 2 will not be added in the list
//            {
//                  token = s.substr(s0, s1 - s0);
//
//                if( token != "") 	
//                {
//                    //cout<<"Token :"<<token<<" length:"<<token.length()<<endl;
//						//cout<<token<<endl;
//					   tokens.push_back(token);
//                }
//            }
//            s0 = s.find_first_not_of(delim, s1);
//        }
//        //EXIT
//        token.clear();
//        return;
//
//    }
//
///*
// * send a message to controller when a node starts on a amachine.
// */
//void Starter::registerAtController(char controllerIP[15],int port){
//	//communication
//
//	communication com;
//	int sockfd = com.connectToServer(controllerIP,port);
//
//	int z;
//	char buf[128]={'\0',};
//	z = gethostname(buf,sizeof buf);
//	if ( z == -1 ) {
//		fprintf(stderr, "%s: gethostname(2)\n",
//		strerror(errno));
//		exit(1);
//	}
//	int sd = 999;
//	printf("\nContacted Controller, sent my hostname:%s\n",buf);
//	com.writeToSocket(sockfd,buf,128);
//	com.readFromSocket(sockfd,&id,sizeof(int));
//	int qsize,nsize;
//	com.readFromSocket(sockfd,&qsize,sizeof(int));
//	com.readFromSocket(sockfd,&nsize,sizeof(int));
//	//int size;
//	//com.readFromSocket(sockfd,&size,sizeof(int));
//	printf("Received ID: %d, Qsize: %d, NumNodes: %d\n",id,qsize,nsize);
//	NumNodes = nsize;
//	quorumSize = qsize;
//	Quorum = new int*[nsize];
//	for(int i = 0; i < nsize; ++i)
//		Quorum[i] = new int[qsize];
//	
//	for (int i = 0; i < nsize; ++i) {
//        for (int j = 0; j < qsize; ++j) {
//            Quorum[i][j] = 0;
//        }
//    }
//	/*for(int i=0;i<nsize;i++){
//		for(int j=0;j<qsize;j++){
//			printf("%d\t",Quorum[i][j]);
//		}
//		printf("\n");			
//	}*/
//	puts("Getting quorum table\n");
//	char recvdMsg[4096] = {'\0',};
//	com.readFromSocket(sockfd,recvdMsg,4095);
//	string recvdStr = recvdMsg;
//	vector<string> recvdValues;
//	string delimiter = ":";
//	parseMsg(recvdStr,delimiter,recvdValues);
//	/*
//	for (int i=0; i<recvdValues.size();i++)
//	{
//		cout<<recvdValues[i]<<endl; //<-- extract values from this vector and push into the attay in 123 line
//		
//	}*/
//	for(int i=0; i<nsize;i++)
//	{
//		for(int j=0; j<qsize;j++)
//		{
//			//cout<<"Here"<<endl;
//			Quorum[i][j] = atoi(recvdValues[i*qsize+j].c_str());
//			//cout<<"Here1"<<endl;
//		}
//	}
//	
//	for(int i=0;i<nsize;i++){
//		for(int j=0;j<qsize;j++){
//	
//			printf("%d\t",Quorum[i][j]);
//		}
//		printf("\n");	
//		
//	}
//	shutdown(sockfd,2);
//	int k = com.closeSocket(sockfd);
//	if (k < 0) {
//			printf("\nError in Closing");
//			exit(0);
//	}else
//		printf("Node disconnected from Controller\n");
//
//}
//
//void Starter::decideAlgorithm(){
//	printf("\nIn decideAlgorithm() of Node\n");
//	communication com;
//	int serfd;
//	int clifd = com.OpenListener(serfd,LISTEN_PORT2);
//	//char buffer[4095]={'\0',};
//	int intbuf;
//	com.readFromSocket(clifd,&intbuf,sizeof(int));
//	printf("recvd: %d\n",intbuf);
//	puts("Getting ID to IP map\n");
//	//assigining memory to mapIDtoIP
//	mapIDtoIP = new char*[NumNodes];
//	for(int i = 0; i < NumNodes; ++i){
//		mapIDtoIP[i] = new char[MAXLENGTH_IP_ADDR];
//	}
//
//	char recvdMsg[4096] = {'\0',};
//	strcpy(mapIDtoIP[0],"hello");
//	com.readFromSocket(clifd,recvdMsg,4095);
//	string recvdStr = recvdMsg;
//	vector<string> recvdValues;
//	string delimiter = ":";
//	parseMsg(recvdStr,delimiter,recvdValues);
//	for(int i=0;i<NumNodes;i++){
//		strcpy(mapIDtoIP[i],recvdValues[i].c_str());
//		printf("%d\t%s\n",i,mapIDtoIP[i]);
//	}
//	shutdown(clifd,2);
//	com.closeSocket(clifd);
//	com.closeSocket(serfd);
//	int algo = intbuf;
//	if(algo == 1){
//		Algorithm1();
//	}else if(algo == 2){
//		Algorithm2();
//
//	}else
//		printf("Invalid input\n");
//
//}
//
//void Starter::Algorithm1(){
//	printf("Starting Algorithm 1: Maekawa\n");
//    mnode = MaekawaAlgorithm::getInstance();
//    mnode->initialization();
//    mnode->setProcessID(id);
//    mnode->getQuorumTable(Quorum,quorumSize, NumNodes);
//    
//    mnode->com.setMapIDtoIP(mapIDtoIP);
//    
//    wqueue<Packet*> queue;
//    pthread_t ListenerThread;
//	pthread_create(&ListenerThread, NULL, MaekawaListen, (void *)&queue);
//    
//	sleep(1);
//	pthread_t ProcessingThread;
//	pthread_create(&ProcessingThread, NULL, MaekawaProcess,(void *)&queue);
//    
//	pthread_join(ListenerThread, NULL);
//	pthread_join(ProcessingThread, NULL);
//    
//	printf("Maekawa Algorithm execution done\n");
//
//    
//}
//
//void Starter::Algorithm2(){
//	printf("Starting Algorithm 2: Torum\n");
//	node = Torum::getInstance();
//	node->init();
//	node->setID(id);
//	node->getQuorumTable(Quorum,quorumSize,NumNodes);
//	//000
//	node->com.setMapIDtoIP(mapIDtoIP);
//
//
//	wqueue<Packet*> queue;
//
//	pthread_t ListenerThread;
//	pthread_create(&ListenerThread, NULL, TorumListen, (void *)&queue);
//
//	sleep(1);
//	pthread_t ProcessingThread;
//	pthread_create(&ProcessingThread, NULL, TorumProcess,(void *)&queue);
//
//	pthread_join(ListenerThread, NULL);
//	pthread_join(ProcessingThread, NULL);
//
//	printf("Torum execution done\n");
//}
//
//void *TorumListen(void* queue) {
//	printf("TorumListener Thread created\n");
//	wqueue<Packet*> *m_queue=((wqueue<Packet*>*)queue);
//	communication com;
//	com.serverListen(LISTEN_PORT3,m_queue);
//	return NULL;
//}
//
//
//void *TorumProcess(void* queue) {
//	printf("TorumProcess Thread created\n");
//	wqueue<Packet*>*m_queue=(wqueue<Packet*>*)queue;
//	Torum *node = Torum::getInstance();
//	sleep(5);
//	// Remove 1 item at a time and process it. Blocks if no items are
//	// available to process.
//	for (int i = 0;; i++) {
//		printf("TorumProcessing Thread , loop %d - waiting for item...\n", i);
//		Packet* item = m_queue->remove();
//		//printf("thread  loop %d - got one item\n", i);
//		printf("Received: messageType - %d, SEQ number - %ld\n",item->TYPE, item->SEQ);
//
//		if (item->TYPE == SEND_TOKEN){
//			printf("SEND_TOKEN recieved from Node %d and packet type is %d\n",item->ORIGIN,item->TYPE);
//			node->receiveToken(*item);
//		}else if (item->TYPE == MAKE_REQUEST){
//			printf("MAKE_REQUEST recieved from Controller %d and packet type is %d\n",item->ORIGIN,item->TYPE);
//			node->requestCS();
//		}else if (item->TYPE == HAVE_TOKEN){
//			printf("HAVE_TOKEN recieved from Node %d and packet type is %d\n",item->ORIGIN,item->TYPE);
//			node->receiveHaveTkn(*item);
//		}else if (item->TYPE == RELEASE){
//			printf("RELEASE recieved from Node %d and packet type is %d\n",item->ORIGIN,item->TYPE);
//			node->receiveRelease(*item);
//		}else if (item->TYPE == REQUEST){
//			printf("REQUEST recieved from Node %d and packet type is %d\n",item->ORIGIN,item->TYPE);
//			node->receiveRequest(*item);
//		}
//		delete item;
//	}
//	return NULL;
//}
//
//void *MaekawaListen(void* queue) {
//	printf("MaekawaListener Thread created\n");
//	wqueue<Packet*> *m_queue=((wqueue<Packet*>*)queue);
//	communication com;
//	com.serverListen(LISTEN_PORT3,m_queue);
//	return NULL;
//}
//
//
//void *MaekawaProcess(void* queue) {
//	printf("MaekawaProcess Thread created\n");
//	wqueue<Packet*>*m_queue=(wqueue<Packet*>*)queue;
//	MaekawaAlgorithm *mnode = MaekawaAlgorithm::getInstance();
//	sleep(5);
//	// Remove 1 item at a time and process it. Blocks if no items are
//	// available to process.
//	for (int i = 0;; i++) {
//		printf("MaekawaProcessing Thread , loop %d - waiting for item...\n", i);
//		Packet* item = m_queue->remove();
//		//printf("thread  loop %d - got one item\n", i);
//		printf("Received: messageType - %d, SEQ number - %ld\n",item->TYPE, item->SEQ);
//        
//        switch (item->TYPE) {
//            case REQUEST:
//                printf("REQUEST received from Node %d and packet type is %d\n",item->ORIGIN,item->TYPE);
//                mnode->receiveRequest(*item);
//                break;
//            case INQUIRE:
//                printf("INQUIRE received from Node %d and packet type is %d\n",item->ORIGIN,item->TYPE);
//                mnode->receiveInquire(*item);
//                break;
//            case FAILED:
//                printf("FAILED received from Node %d and packet type is %d\n",item->ORIGIN,item->TYPE);
//                mnode->receiveFailed(*item);
//                break;
//            case RELEASE:
//                printf("RELEASE received from Node %d and packet type is %d\n",item->ORIGIN,item->TYPE);
//                mnode->receiveRelease(*item);
//                break;
//            case RELINQUISH:
//                printf("RELINQUISH received from Node %d and packet type is %d\n",item->ORIGIN,item->TYPE);
//                mnode->receiveRelinquish(*item);
//                break;
//            case LOCKED:
//                printf("LOCKED received from Node %d and packet type is %d\n",item->ORIGIN,item->TYPE);
//                mnode->receiveLocked(*item);
//                break;
//                
//            default:
//                break;
//        }
//        delete item;
//        
////		if (item->TYPE == SEND_TOKEN){
////			printf("SEND_TOKEN recieved from Node %d and packet type is %d\n",item->ORIGIN,item->TYPE);
////			node->receiveToken(*item);
////		}else if (item->TYPE == MAKE_REQUEST){
////			printf("MAKE_REQUEST recieved from Controller %d and packet type is %d\n",item->ORIGIN,item->TYPE);
////			node->requestCS();
////		}else if (item->TYPE == HAVE_TOKEN){
////			printf("HAVE_TOKEN recieved from Node %d and packet type is %d\n",item->ORIGIN,item->TYPE);
////			node->receiveHaveTkn(*item);
////		}else if (item->TYPE == RELEASE){
////			printf("RELEASE recieved from Node %d and packet type is %d\n",item->ORIGIN,item->TYPE);
////			node->receiveRelease(*item);
////		}else if (item->TYPE == REQUEST){
////			printf("REQUEST recieved from Node %d and packet type is %d\n",item->ORIGIN,item->TYPE);
////			node->receiveRequest(*item);
////		}
////		delete item;
//	}
//	return NULL;
//}
