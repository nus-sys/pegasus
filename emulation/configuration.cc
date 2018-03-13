#include <assert.h>
#include <netdb.h>
#include "logger.h"
#include "configuration.h"

using namespace std;

NodeAddress::NodeAddress()
    : address(""), port("")
{
}

NodeAddress::NodeAddress(const string &address, const string &port)
    : address(address), port(port)
{
    struct addrinfo hints, *res;
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;
    hints.ai_protocol = 0;
    hints.ai_flags = AI_PASSIVE;
    if (getaddrinfo(address.c_str(),
                    port.c_str(),
                    &hints,
                    &res) != 0) {
        panic("Failed to get address info");
    }
    assert(res->ai_family == AF_INET);
    assert(res->ai_socktype == SOCK_DGRAM);
    assert(res->ai_addr->sa_family == AF_INET);
    this->sin = *(sockaddr_in *)res->ai_addr;
    freeaddrinfo(res);
}
