//
//  MaekawaAlgorithm.cpp
//  AOSProject
//
//  Created by Antony on 11/10/13.
//  Copyright (c) 2013 Antony. All rights reserved.
//

#include "MaekawaAlgorithm.h"
#include "wqueue.h"
#include <pthread.h>
#include "communication.h"

MaekawaAlgorithm* MaekawaAlgorithm::instance = NULL;

MaekawaAlgorithm* MaekawaAlgorithm::getInstance()
{
    if(!instance){
        instance = new MaekawaAlgorithm();
    }
    return instance;
}

void MaekawaAlgorithm::initialization(){
	sequenceNo = 0;
	queue = new LexiQueue();
	hasFailed = false;
    
    //the current node has finished entering the critical section. It needs to send RELEASE messages.
	hasCompletedCriticalSection = false;
    
    //the current node has ever sent a locked message to other nodes. It has been lock by some nodes. When it receives another request, it needs to send INQUIRE messages.
    hasSentLockedMessage = false;
    
    hasSentRequestMessage = 0;
    hasReceivedLockedMessage = 0;
	pthread_mutex_init(&sharedLock, NULL);
}

bool MaekawaAlgorithm::setProcessID(int pid){
	processID = pid;
	return true;
    
}

bool MaekawaAlgorithm::getQuorumTable(int **quorumtable,int qsize,int Nnodes){
    quorum = quorumtable;
    quorumsize = qsize;
    NoOfnodes = Nnodes;
    
    printf("--^^--quorumVote Table--^^-- \n");
    //initialize quorumVote
    for(int i = 0; i < quorumsize; i++){
        vector<int> temp;
        temp.push_back(quorum[processID][i]);
        temp.push_back(0);
        quorumVote.push_back(temp);
        //quorumVote[i].assign(temp[0],temp[1]);
        printf("%d , %d \n",quorumVote[i][0],quorumVote[i][1]);
        
//        quorumVote[i][1] = 100000;
//        printf("%d , %d \n",quorumVote[i][0],quorumVote[i][1]);
        
    }
    
    
    
    return true;
}

bool MaekawaAlgorithm::requestCriticalSection(){
    
    printf("----Node %d is trying to request critical section\n",processID);
    sequenceNo++;
	struct Packet request;
	request.TYPE = REQUEST;
	request.ORIGIN = processID;
    request.SEQ = sequenceNo;
    request.sender = -1;
    //reset relinquishList
    for(int i = 0; i < relinquishList.size(); i++){
        printf("----relinquishList size: %lu. Member removed: %d \n",relinquishList.size(),relinquishList.back());
        relinquishList.pop_back();
    }
    
    //add request to the node queue
	queue->add(request);
    
    //broadcast request to all processes in its quorum
	for(int j = 0; j < quorumsize; j++){
        com.sendMessageToID(request,quorum[processID][j]);
        printf("----Node %d has sent REQUEST message to %d \n",processID,quorum[processID][j]);
	}
	return true;
}

void MaekawaAlgorithm::receiveMakeRequest(Packet makeRequest){
    
    printf("----Node %d has received MAKE_REQUEST message from %d \n",processID,makeRequest.ORIGIN);
    //Compare and maximize the sequence number
    if(sequenceNo < makeRequest.SEQ)
		sequenceNo = makeRequest.SEQ - 1;
    
    //
    hasSentRequestMessage ++;
    printf("----Node %d has %d request to send.\n",processID,hasSentRequestMessage);
    
    //Call requestCriticalSection to broadcast request
    if(hasSentRequestMessage == 1)
        requestCriticalSection();
    
}

bool MaekawaAlgorithm::receiveRequest(Packet request){
	
    printf("----Node %d has received REQUEST message from %d \n",processID,request.ORIGIN);
    //Compare and maximize the sequence number
    if(sequenceNo<request.SEQ)
		sequenceNo = request.SEQ;
    
    //add request to the node queue
    if(request.ORIGIN != processID){
        queue->add(request);
    }

    
    //(i) Check if it has been locked
    //(ii)Check if current input message is the top in the queue
    if(hasSentLockedMessage == false){
        
        //send LOCKED message back to sender of request
        struct Packet locked;
        locked.TYPE = LOCKED;
        locked.ORIGIN = processID;
        locked.SEQ = sequenceNo;
        locked.sender = -1;
        
        printf("----Node %d has sent LOCKED message to %d \n",processID,request.ORIGIN);
        com.sendMessageToID(locked, request.ORIGIN);
        
        lockedBy = request.ORIGIN;
        hasSentLockedMessage = true;
    }
    else if(queue->equalsTo(queue->top(), request) == true){
        
        //send INQUIRE message to the process that locked the current process
        struct Packet inquire;
        inquire.TYPE = INQUIRE;
        inquire.ORIGIN = processID;
        inquire.SEQ = sequenceNo;
        inquire.sender = -1;
        
        printf("----Node %d has sent INQUIRE message to %d \n",processID,lockedBy);
        com.sendMessageToID(inquire, lockedBy);
    }
    else if(queue->equalsTo(queue->top(), request) == false){
        
        //send FAILED message to sender of request
        struct Packet failed;
        failed.TYPE = FAILED;
        failed.ORIGIN = processID;
        failed.SEQ = sequenceNo;
        failed.sender = -1;
        
        printf("----Node %d has sent FAILED message to %d \n",processID,request.ORIGIN);
        com.sendMessageToID(failed, request.ORIGIN);
    }
    return true;
}

bool MaekawaAlgorithm::receiveInquire(Packet inquire) {
    
    printf("----Node %d has received INQUIRE message from %d \n",processID,inquire.ORIGIN);
    //Compare and maximize the sequence number
	if(sequenceNo<inquire.SEQ)
		sequenceNo = inquire.SEQ;
    
    //Put the ORIGIN of inquire into list
    relinquishList.push_back(inquire.ORIGIN);
    
	return true;
}

bool MaekawaAlgorithm::receiveFailed(Packet failed) {

    printf("----Node %d has received FAILED message from %d \n",processID,failed.ORIGIN);
    //Compare and maximize the sequence number
    if(sequenceNo<failed.SEQ)
		sequenceNo = failed.SEQ;
    
    int temp = -1;
    for(int i = 0; i < quorumsize; i++){
        if(quorumVote[i][0] == failed.ORIGIN) temp = i;
    }
    quorumVote[temp][1] = 0;
    for(int i = 0; i < quorumsize; i++){
        printf("%d , %d \n",quorumVote[i][0],quorumVote[i][1]);
    }
    
    //Send RELINQUISH message to all the members in relinquishList
    for(int i = 0; i < relinquishList.size(); i++){
        struct Packet relinquish;
        relinquish.TYPE = RELINQUISH;
        relinquish.ORIGIN = processID;
        relinquish.SEQ = sequenceNo;
        relinquish.sender = -1;
        
        //send(relinquish);
        com.sendMessageToID(relinquish, relinquishList.back());
        printf("----Node %d has sent RELINQUISH message to %d \n",processID,relinquishList.back());
        
        relinquishList.pop_back();
    }
    return true;
}

bool MaekawaAlgorithm::receiveRelinquish(Packet relinquish){
    
    printf("----Node %d has received RELINQUISH message from %d \n",processID,relinquish.ORIGIN);
    //Compare and maximize the sequence number
	if(sequenceNo < relinquish.SEQ)
        sequenceNo = relinquish.SEQ;

    //Send LOCKED message to the top priority member in queue and send message to its ORIGIN
    struct Packet locked;
    locked.TYPE = LOCKED;
    locked.ORIGIN = processID;
    locked.SEQ = sequenceNo;
    locked.sender = -1;
    
    com.sendMessageToID(locked, queue->top().ORIGIN);
    printf("----Node %d has sent LOCKED message to %d \n",processID,queue->top().ORIGIN);

	return true;
}

bool MaekawaAlgorithm::receiveLocked(Packet locked){
    
    printf("----Node %d has received LOCKED message from %d \n",processID,locked.ORIGIN);
    //Compare and maximize the sequence number
    if(sequenceNo < locked.SEQ)
        sequenceNo = locked.SEQ;
    
    //Increase hasReceivedLockedMessage by 1. If this variable reaches K-1, then the node can enter the critical section.
    //hasReceivedLockedMessage++;
    
    //Update quorumVote table
    
    int temp = -1;
    for(int i = 0; i < quorumsize; i++){
        if(quorumVote[i][0] == locked.ORIGIN) temp = i;
    }
    quorumVote[temp][1] = 1;
    
    hasReceivedLockedMessage = 0;
    
    printf("--^^--quorumVote Table--^^-- \n");
    
    //Check if the current node has received all the locked messages
    for(int i = 0; i < quorumsize; i++){
        printf("%d , %d \n",quorumVote[i][0],quorumVote[i][1]);
        if(quorumVote[i][1] == 1){
            hasReceivedLockedMessage++;
        }
    }
    printf("----Node %d has received %d locked messages \n",processID,hasReceivedLockedMessage);
    
    if(hasReceivedLockedMessage == quorumsize){
        
        printf("----Node %d has entered critical section \n",processID);
        enterCriticalSection();
        //printf("Node %d has exited critical section \n",processID);
        return true;
    }
    return true;
}

bool MaekawaAlgorithm::receiveRelease(Packet release){
    
    printf("----Node %d has received RELEASE message from %d \n",processID,release.ORIGIN);
    
    //Compare and maximize the sequence number
	if(sequenceNo < release.SEQ)
        sequenceNo = release.SEQ;
    
//    //Discard the release message from itself
//    if(release.ORIGIN == processID){
//        printf("----Node %d has received RELEASE message from %d. The message is discarded \n",processID,release.ORIGIN);
//        hasCompletedCriticalSection = false;
//        return true;
//    }
    
    //(i)       deletes release message's node from the queue
    //(ii) a)   lock for the most preceding request in the queue. It sends LOCK message.
    //(ii) b)   No request in the queue. Unlock itself. Do nothing.
    if(release.ORIGIN != processID){
        queue->remove(release.ORIGIN);
    }
    
    if(queue->top().TYPE == -1 ){ //-1 means nothing in the queue
        
        printf("----After Node %d received release message, there is no more request in the queue \n",processID);
        hasSentLockedMessage = false;
        return true;
    }
    else{
        struct Packet locked;
        locked.TYPE = LOCKED;
        locked.ORIGIN = processID;
        locked.SEQ = sequenceNo;
        locked.sender = -1;
        
        printf("----The next request in the queue: %d \n",queue->top().ORIGIN);
        printf("----After Node %d received release message, there is at least one request in the queue \n",processID);
        com.sendMessageToID(locked, queue->top().ORIGIN);
        printf("----Node %d has sent LOCKED message to %d \n",processID,queue->top().ORIGIN);
        
        lockedBy = queue->top().ORIGIN;
        hasSentLockedMessage = true;
        return true;
    }
    
	return true;
}

void MaekawaAlgorithm::enterCriticalSection(){
    printf("----Node %d has entered its critical section\n",processID);
    sleep(15);
    hasCompletedCriticalSection = true;
    hasSentLockedMessage = false;
    
    printf("----Queue size before remove is: %d\n", queue->size());
    queue->remove(processID);
    
    printf("----Queue size after remove is: %d\n", queue->size());
    printf("----Node %d has delete itself from the queue\n",processID);
    printf("----Node %d has received 0 locked message\n",processID);
    printf("----Node %d has exited its critical section\n",processID);
    
    //reset quorumVote Table
    printf("--^^--quorumVote Table--^^-- \n");
    
    for(int i = 0; i < quorumsize; i++){
        quorumVote[i][1] = 0;
        printf("%d , %d \n",quorumVote[i][0],quorumVote[i][1]);
    }
    sendRelease();
    
    hasSentRequestMessage--;
    printf("----Node %d has %d request left to send.\n",processID,hasSentRequestMessage);
    //Call requestCriticalSection to broadcast request
    if(hasSentRequestMessage > 0)
        requestCriticalSection();
}

bool MaekawaAlgorithm::sendRelease(){
    
	//Packet top = queue->top();
	if(hasCompletedCriticalSection == true){
        
		struct Packet release;
		release.TYPE = RELEASE;
		release.ORIGIN = processID;
        release.sender = -1;
        release.SEQ = sequenceNo;
        
		for(int j = 0 ; j < quorumsize ; j++){
            
            printf("----Node %d send release message to node %d \n",processID,quorum[processID][j]);
            com.sendMessageToID(release, quorum[processID][j]);
            hasCompletedCriticalSection = false;
		}
        printf("----Node %d has sent all the release messages to its quorum members\n",processID);
	}
    
	return true;
    
}




