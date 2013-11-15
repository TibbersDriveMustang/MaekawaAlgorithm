#include "Algo9.h"

Torum* Torum::instance = NULL;

void Torum::init()
{
	sequenceNo = 0;
	HOLDER = -1;
	inCS = false;
	queue = new LexiQueue();
	pthread_mutex_init(&sharedQLock,NULL);
}

Torum* Torum::getInstance()
        {
            if(!instance){
            	instance = new Torum();
            }
            return instance;
        }

bool Torum::setID(int id){
	ID = id;
	return true;
}
bool Torum::getQuorumTable(int **quorumtable,int qsize,int Nnodes){
	quorum = quorumtable;
	quorumsize = qsize;
	NumNodes = Nnodes;
	return true;
}

bool Torum::requestCS(){
	sequenceNo++;

	struct Packet request;
	request.TYPE = REQUEST;
	request.ORIGIN = ID;
	request.sender = ID;
	request.SEQ = sequenceNo;

	/*
	pthread_mutex_lock(&sharedQLock);
	queue->add(request);
	pthread_mutex_unlock(&sharedQLock);
	*/
	if( HOLDER== -1 ){
		for(int i=0;i<quorumsize;i++){
			com.sendMessageToID(request,quorum[ID][i]);
		}
	}else{
		com.sendMessageToID(request,HOLDER);
	}
	return true;
}

bool Torum::receiveRequest(Packet request){
	if(sequenceNo<request.SEQ) sequenceNo = request.SEQ;
	//check if the token is with this node
	// token is not with this node
		//request from its master(within quorum request)
		if(request.ORIGIN == request.sender){
			pthread_mutex_lock(&sharedQLock);
			queue->add(request);
			pthread_mutex_unlock(&sharedQLock);
			if(HOLDER == ID){
				if(!inCS){// Node is idle after completing CS.
					Packet top = queue->top();

					sendToken();
				}
			}
			if(HOLDER != -1){
				request.sender=ID;
				com.sendMessageToID(request,HOLDER);
			}
		}else{// request originated from out of this quorum
			if(HOLDER==ID){
					pthread_mutex_lock(&sharedQLock);
					queue->add(request);
					pthread_mutex_unlock(&sharedQLock);

				}
			//else drop the request
		}


	return true;
}

bool Torum::receiveToken(Packet token){
	if(sequenceNo<token.SEQ) sequenceNo = token.SEQ;
	pthread_mutex_lock(&sharedQLock);
	Packet top = queue->top();
	if(top.TYPE == -1){
		printf("receiveToken: queue top returned empty\n");
	}
	pthread_mutex_unlock(&sharedQLock);

	if(ID == top.ORIGIN || top.ORIGIN == CONTROLLER_ID){// if current node is top of the queue
		HOLDER = ID;
		struct Packet havetkn;
		havetkn.TYPE = HAVE_TOKEN;
		havetkn.ORIGIN = ID;
		havetkn.sender = ID;
		havetkn.SEQ = sequenceNo;
		for(int i=0;i<quorumsize;i++){
			com.sendMessageToID(havetkn,quorum[ID][i]);
		}
		if(ID == top.ORIGIN)
			EnterTheCS();
	}else{
		// sends token to the request on top of the queue
		//Note: we are not passing the same packet that we received
		sendToken();
	}
	return true;
}

bool Torum::receiveRelease(Packet release){
	if(sequenceNo<release.SEQ) sequenceNo = release.SEQ;
	HOLDER = -1;
	pthread_mutex_lock(&sharedQLock);
	queue->update(quorum,quorumsize,ID);
	pthread_mutex_unlock(&sharedQLock);
	return true;
}

bool Torum::receiveHaveTkn(Packet havtkn){
	if(sequenceNo<havtkn.SEQ) sequenceNo = havtkn.SEQ;
	HOLDER = havtkn.ORIGIN;
	pthread_mutex_lock(&sharedQLock);
	Packet ret = queue->remove(havtkn.ORIGIN);
	if(ret.TYPE != -1){
	Packet top = queue->top();
	if(top.TYPE == -1){
			printf("receiveToken: queue top returned empty\n");
					return false;
		}
	pthread_mutex_unlock(&sharedQLock);
	if(top.TYPE!=-1)
		com.sendMessageToID(top,HOLDER);
	return true;
	}else{
		printf("receiveHaveTkn: queue top returned empty/not found\n");
		return false;
	}
}

bool Torum::sendToken(){
	Packet top = queue->top();
	if(top.TYPE == -1){
			printf("receiveToken: queue top returned empty\n");
					//return false;
		}
	Packet ret = queue->remove(top.ORIGIN);
	if(ret.TYPE==-1){
		printf("sendToken: queue remove returned empty/not found\n");
		//return false;
	}else{
	struct Packet release;
		release.TYPE = RELEASE;
		release.ORIGIN = ID;
		release.sender = ID;
		release.SEQ = sequenceNo;
	for(int i=0;i<quorumsize;i++){
			com.sendMessageToID(release,quorum[ID][i]);
		}
	pthread_mutex_lock(&sharedQLock);
	queue->update(quorum,quorumsize,ID);
	pthread_mutex_unlock(&sharedQLock);
	
	struct Packet token;
		token.TYPE = SEND_TOKEN;
		token.ORIGIN = ID;
		token.sender = ID;
		token.SEQ = sequenceNo;
		com.sendMessageToID(token,top.sender);
		return true;
	}
}

bool Torum::EnterTheCS(){
	inCS = true;
	printf("Node %d in Critical Section\n",ID);
	sleep(1);
	inCS = false;
}

