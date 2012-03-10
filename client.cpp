#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <zmq.hpp>


void s(const std::string& s) {
  std::stringstream ss(s);
  const char delim = ',';
  std::string token;

  while(std::getline(ss,token,delim)) {
      std::cout << "token: " << token << std::endl;
  };
}

void c() {

  std::stringstream ss;

  std::string s1 = "First string";
  std::string s2 = "Second string";
  ss << s1 << "," << 123 << "," << s2 << std::endl << "second line " << "," << 456 << std::endl;

  std::string result = ss.str();

  s(result);
}

int main() {

  zmq::context_t context(1);
  zmq::socket_t socket(context,ZMQ_REQ);

  std::vector<std::string> commands;
  commands.push_back("put,DCX1234,23456fev64g");
  commands.push_back("put,DCX1235,rgine5-8u3");
  commands.push_back("put,DCX1234,second key");
  commands.push_back("get,DCX1234");
  commands.push_back("get,DCX1235");
  commands.push_back("get,DCX0000");

  socket.connect("ipc://test.ipc");

  for(int i=0;i<commands.size();i++) {

    const std::string& command = commands[i];
    zmq::message_t request(command.size());
    memcpy((void*)request.data(),command.data(),command.size());
    std::cout << "sending command " << std::string((char*)request.data(),request.size()) << std::endl;
    socket.send(request);

    zmq::message_t reply;
    socket.recv(&reply);
    std::cout << "received        " << std::string((char*)reply.data(),reply.size()) << std::endl;
    
  }

}
