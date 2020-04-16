#ifndef __APPLICATION_H__
#define __APPLICATION_H__

#include "transport.h"

class Application : public TransportReceiver {
public:
    virtual ~Application() {};
    virtual void run() = 0;
    virtual void run_thread(int tid) = 0;
};

#endif /* __APPLICATION_H__ */
