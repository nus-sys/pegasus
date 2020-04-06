#ifndef _UDP_CONFIGURATION_H_
#define _UDP_CONFIGURATION_H_

#include <netinet/in.h>

#include <configuration.h>

class UDPAddress : public Address {
public:
    UDPAddress(const std::string &address, const std::string &port);
    UDPAddress(const struct sockaddr &saddr);

    struct sockaddr saddr;
};

class UDPConfiguration : public Configuration {
public:
    UDPConfiguration(const char *file_path);
};

#endif /* _UDP_CONFIGURATION_H_ */
