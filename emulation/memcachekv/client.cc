#include "utils.h"
#include "memcachekv/client.h"
#include "memcachekv/memcachekv.pb.h"

using std::string;

namespace memcachekv {
using namespace proto;

NextOperation
KVWorkloadGenerator::next_operation()
{
    Operation op;
    op.set_key("k1");
    if (counter == 0) {
        op.set_op_type(Operation_Type_PUT);
        op.set_value("v1");
    } else {
        op.set_op_type(Operation_Type_GET);
    }
    counter = (counter + 1) % 2;
    return NextOperation(1000, op);
}

Client::Client(Transport *transport,
               MemcacheKVStats *stats,
               KVWorkloadGenerator *gen)
    : transport(transport), stats(stats), gen(gen), req_id(1) {}

void
Client::receive_message(const string &message, const sockaddr &src_addr)
{
    MemcacheKVReply reply;
    reply.ParseFromString(message);
    assert(this->pending_requests.count(reply.req_id()) > 0);
    PendingRequest &pending_request = this->pending_requests.at(reply.req_id());

    if (pending_request.op_type == Operation_Type_GET) {
        complete_op(reply.req_id(), reply.result());
    } else {
        pending_request.received_acks += 1;
        if (pending_request.received_acks >= pending_request.expected_acks) {
            complete_op(reply.req_id(), reply.result());
        }
    }
}

void
Client::run(int duration)
{
    struct timeval start, now;
    gettimeofday(&start, nullptr);

    this->stats->start();
    do {
        NextOperation next_op = this->gen->next_operation();
        wait(next_op.time);
        execute_op(next_op.op);
        gettimeofday(&now, nullptr);
    } while (latency(start, now) / 1000000 < duration);

    this->stats->done();
    this->stats->dump();
}

void
Client::execute_op(const proto::Operation &op)
{
    PendingRequest pending_request;
    gettimeofday(&pending_request.start_time, nullptr);
    pending_request.op_type = op.op_type();
    pending_request.expected_acks = 1;
    this->pending_requests[this->req_id] = pending_request;

    MemcacheKVRequest request;
    string request_str;
    request.set_req_id(this->req_id);
    *(request.mutable_op()) = op;
    request.SerializeToString(&request_str);
    this->transport->send_message_to_node(request_str, 0);

    this->req_id++;
}

void
Client::complete_op(uint64_t req_id, Result result)
{
    PendingRequest &pending_request = this->pending_requests.at(req_id);
    struct timeval end_time;
    gettimeofday(&end_time, nullptr);
    this->stats->report_op(pending_request.op_type,
                           latency(pending_request.start_time, end_time),
                           result == Result::OK);
    this->pending_requests.erase(req_id);
}

} // namespace memcachekv
