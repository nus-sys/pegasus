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
    virtual void load_from_file(const char *file_path) override final;
};

#endif /* _UDP_CONFIGURATION_H_ */
