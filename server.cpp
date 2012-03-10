#include <iostream>
#include <sstream>
#include <zmq.hpp>
#include <leveldb/db.h>


leveldb::DB* db;
void create_db() {
  leveldb::Options options;
  options.create_if_missing = true;
  leveldb::Status status=leveldb::DB::Open(options,"/tmp/test.ldb",&db);
  if(!status.ok()) {
    std::cerr << "Unable to create database, " << status.ToString() << std::endl;
  }
}

bool put(const std::string& key, const std::string& value) {
  std::cout << "put operation " << key << ":" << value << std::endl;
  leveldb::Status s = db->Put(leveldb::WriteOptions(),key,value);
  if(!s.ok()) { 
    std::cout << s.ToString() << std::endl; 
  }
  else {
    std::cout << "put " << key << "," << value << " ok " << std::endl;
  }
  return s.ok();
}

bool get(const std::string& key, std::string& value) {
  leveldb::Status s = db->Get(leveldb::ReadOptions(),key,&value);
  if(!s.ok()) { 
    std::cout << s.ToString() << std::endl; 
  }
  else {
    std::cout << "get " << key << " --> " << value << " is ok " << std::endl;
  }
  return s.ok();
}

typedef enum {
  ERROR = 0,
  GET = 1,
  PUT = 2
} OperationType;

typedef struct {
  OperationType _type;
  bool _status;
  std::string _key;
  std::string _value;
} Operation;

bool do_operation(const std::string& command, Operation& operation) {

  std::cout << "Received command " << command << std::endl;
  std::stringstream ss(command);
  const char delim=',';
  std::string action,key,value;
  if(std::getline(ss,action,delim)) {
    if(action=="put") {
      
      std::getline(ss,key,delim);
      std::getline(ss,value,delim);
    
      std::cout << "put " << key << "," << value << std::endl;
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
    else {
      std::cout << "unknown action " << action << std::endl;
    }
  }
  else {
    std::cout << "Unable to parse command " << command << std::endl;
  }
  return false;
}

void send_data(zmq::socket_t& socket, const std::string& data) {

  zmq::message_t reply(data.size());
  memcpy((void*)reply.data(),data.data(),data.size());
  socket.send(reply);
}

int main() {
  create_db();
  zmq::context_t context(1);

  zmq::socket_t socket(context,ZMQ_REP);
  socket.bind("ipc://test.ipc");

  while(true) {
    zmq::message_t request;
    socket.recv(&request);

    Operation operation;
    std::string command((char*)request.data(),request.size());
    std::cout << "server recieved command " << command << " of size " << request.size() << std::endl;
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
  return 0;
}
