#include "LexiQueue.h"


LexiQueue::LexiQueue(void)
{

}


LexiQueue::~LexiQueue(void)
{

}

Packet LexiQueue::remove(int origin){
	tempq = pq;
	Packet top;
	Packet ret;
	ret.TYPE = -1;
	bool removed = false;
	while(!tempq.empty())
		tempq.pop();
	while(!pq.empty()){
		top = pq.top();
		pq.pop();
		if(top.ORIGIN == origin && removed == false){
			ret =top;
			removed = true;
		}else
			tempq.push(top);
	}
	pq = tempq;

	return ret;
}

Packet LexiQueue::top(){
	Packet top;
	top.TYPE = -1;
	if(!pq.empty()){
		top = pq.top();
		return top;
	}
	return top;
}

bool LexiQueue::equalsTo(Packet m1, Packet m2){
    if(m1.ORIGIN == m2.ORIGIN && m1.SEQ == m2.SEQ && m1.TYPE == m2.TYPE)
        return true;
    else
        return false;
    }

bool LexiQueue::add(Packet in){
	pq.push(in);
	return true;
}

bool LexiQueue::update(int **quorum,int qsize,int ID){


	while(!pq.empty()){
		Packet top = pq.top();
		pq.pop();
		for(int j=0;j<qsize;j++){
			if(quorum[top.ORIGIN][j] == ID){
				tempq.push(top);
				break;
			}
		}

	}
	pq = tempq;
	return true;
}

int LexiQueue::size(){
	int ret = pq.size();
	return ret;
}

bool LexiQueue::isEmpty(){
	return pq.empty();
}

