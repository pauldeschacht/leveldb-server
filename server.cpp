#include <iostream>
#include <sstream>
#include <fstream>
#include <iostream>
#include <sstream>

#include <zmq.hpp>
#include <leveldb/db.h>

#include <pthread.h>
#include <unistd.h>
#include <sys/syscall.h>

leveldb::DB* db;
/**
 * open/create the database
 **/
void create_db(const std::string& folder) {
  leveldb::Options options;
  options.create_if_missing = true;
  leveldb::Status status=leveldb::DB::Open(options,folder + "/leveldb.ldb",&db);
  if(!status.ok()) {
    std::cerr << "Unable to create database, " << status.ToString() << std::endl;
  }
}
/**
 * add new key/value to the database
 **/
bool put(const std::string& key, const std::string& value) {
  leveldb::Status s = db->Put(leveldb::WriteOptions(),key,value);
  if(!s.ok()) { 
    std::cerr << s.ToString() << std::endl; 
  }
  return s.ok();
}
/**
 * retreive single key/valie from the database
 **/
bool get(const std::string& key, std::string& value) {
   
  leveldb::Status s = db->Get(leveldb::ReadOptions(),key,&value);
  if(!s.ok()) { 
    std::cerr << s.ToString() << std::endl; 
  }
  return s.ok();
}
/**
 * retrieve all the values associated with a key
 **/
bool multi_get(const std::string& key, std::string& value) {
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    it->Seek(key);
    if(!it->Valid()) {
        delete it;
        return false;
    }
    while(it->Valid() && it->key().ToString() == key) {
        value = it->value().ToString() + ":";
        it->Next();
    }
    delete it;
    return true;
}


typedef enum {
  ERROR = 0,
  GET = 1,
  PUT = 2,
  MGET = 3
} OperationType;

typedef struct {
  OperationType _type;
  bool _status;
  std::string _key;
  std::string _value;
} Operation;

bool do_operation(const std::string& command, Operation& operation) {

  std::stringstream ss(command);
  const char delim=',';
  std::string action,key,value;
  if(std::getline(ss,action,delim)) {
    if(action=="put") {
      
      std::getline(ss,key,delim);
      std::getline(ss,value,delim);
    
      operation._type = PUT;
      operation._key = key;
      operation._value = value;
      operation._status = put(key,value);
      return operation._status;
    }
    else if(action=="get") {
      std::getline(ss,key,delim);
      
      operation._type = GET;
      operation._key = key;
      operation._status = get(key,value);
      operation._value = value;

      return operation._status;
    }
    else if(action=="mget") {
      std::getline(ss,key,delim);
      
      operation._type = MGET;
      operation._key = key;
      operation._status = multi_get(key,value);
      operation._value = value;

      return operation._status;
    }
     else {
      std::cerr << "unknown action " << action << std::endl;
    }
  }
  else {
    std::cerr << "Unable to parse command " << command << std::endl;
  }
  return false;
}

void send_data(zmq::socket_t& socket, const std::string& data) {

  zmq::message_t reply(data.size());
  memcpy((void*)reply.data(),data.data(),data.size());
  socket.send(reply);
}

void* worker_routine(void* arg) {

    //create file for duration of operations    
    zmq::context_t* context = (zmq::context_t*)arg;
    zmq::socket_t socket(*context,ZMQ_REP);

    socket.connect( "inproc://workers");

    while(true) {
        zmq::message_t request;
        socket.recv(&request);

        Operation operation;
        std::string command((char*)request.data(),request.size());
        bool success=do_operation(std::string((char*)request.data(),request.size()),operation);
        if(success) {
            if(operation._type==GET) {
                send_data(socket,operation._value);
            }
            else if(operation._type==PUT) {
                send_data(socket,"OK");
             }
            else if(operation._type==MGET) {
                send_data(socket,operation._value);
             }
            else {
                send_data(socket,"Wrong action");
            }
        }
        else {
            send_data(socket,"Operation failed");
        }
    }
}
void run_mono_thread(const std::string& folder) {

  create_db(folder);
  zmq::context_t context(1);

  zmq::socket_t socket(context,ZMQ_REP);
  std::string pipe = "ipc://" + folder + "/pipe.ipc";
  socket.bind(pipe.c_str());

  std::cout << "mono-thread server is ready " << std::endl;
  while(true) {
    zmq::message_t request;
    socket.recv(&request);

    Operation operation;
    std::string command((char*)request.data(),request.size());
    bool success=do_operation(std::string((char*)request.data(),request.size()),operation);
    zmq::message_t reply;

    if(success) {
      if(operation._type==GET) {
        send_data(socket,operation._value);
      }
      else if(operation._type==PUT) {
        send_data(socket,"OK");
      }
      else {
        send_data(socket,"Wrong action");
      }
    }
    else {
      send_data(socket,"Operation failed");
    }
  }
  delete db;
}

void run_multi_thread(const std::string& folder,int nbWorkers) {
    create_db(folder);
    zmq::context_t context(1);

  //
  //client application use IPC socket to connect to the server
  //
  zmq::socket_t socket(context,ZMQ_XREP);
  std::string pipe = "ipc://" + folder + "/pipe.ipc";
  socket.bind(pipe.c_str());

  //
  //create end point for worker threads
  //
  zmq::socket_t workers(context,ZMQ_XREQ);
  workers.bind("inproc://workers");

  //
  // create the worker threads
  //
  if (nbWorkers>0) {
      for(int i=0;i<nbWorkers;i++) {
        pthread_t worker;
        int r = pthread_create(&worker,NULL,worker_routine,(void*)&context);
        if(r!=0) {
            std::cerr << "Unable to start worker " << i+1 << std::endl;
        }
    }
  }

    std::cout << "server is ready (" << nbWorkers << " threads)" << std::endl;

    zmq::device(ZMQ_QUEUE,socket,workers);
    
    std::cout << "server done" << std::endl;

}
int main(int argc, char** argv) {

    int nbWorkers = 1;
    if(argc==1) {
        std::cout << "server folder_database number_of_threads" << std::endl;
        return -1;
    }

    std::string folder = argv[1];

    if (argc==3) {
        std::string strWorkers = argv[2];
        //nbWorkers = boost::lexical_cast<int>(strWorkers);
        nbWorkers = atoi(strWorkers.c_str());
    }

    if(nbWorkers==1) {
        run_mono_thread(folder);
    }
    else {
        run_multi_thread(folder,nbWorkers);
    }
}

