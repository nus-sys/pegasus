#ifndef DCNC_DEFINES
#define DCNC_DEFINES

#define DCNC_READ_REQUEST               1
#define DCNC_WRITE_REQUEST              2
#define DCNC_HOT_READ_REQUEST           3
#define DCNC_DEL_REQUEST                4

#define DCNC_READ_REPLY                 5
#define DCNC_READ_REPLY_NA              6
#define DCNC_WRITE_REPLY                7
#define DCNC_WRITE_REPLY_NA             8
#define DCNC_ADD_REPLY                  9
#define DCNC_DEL_REPLY                  10

#define DCNC_UPDATE_INVALIDATE_REQUEST  11
#define DCNC_UPDATE_VALUE_REQUEST       12
#define DCNC_UPDATE_INVALIDATE_REPLY    13
#define DCNC_UPDATE_VALUE_REPLY         14

#define DCNC_VALUE_WIDTH                32
#define DCNC_CACHE_NUM                  16384
#define DCNC_SWITCH_NUM                 4196

#define PORT_TOR_CLI_TO_CLIENT          188 //Physical Port 1 188
#define PORT_TOR_SER_TO_SERVER          184 //Physical Port 2 184
#define PORT_SPINE_TO_TOR_SER           284 //Physical Port 33 284
#define PORT_TOR_SER_TO_SPINE           280 //Physical Port 34 280
#define PORT_SPINE_TO_TOR_CLI           276 //Physical Port 35 276
#define PORT_TOR_CLI_TO_SPINE           272 //Physical Port 36 272



#endif
