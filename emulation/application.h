#ifndef _APPLICATION_H_
#define _APPLICATION_H_

#include "transport.h"

class Application : public TransportReceiver {
public:
    virtual ~Application() {};
    virtual void run() = 0;
    virtual void run_thread(int tid) = 0;
};

#endif /* _APPLICATION_H_ */
