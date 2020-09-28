import os
import time
import math
import operator
import pyrem.host
import pyrem.task

# Modify the following
clients = ['client0hostname', 'client1hostname']
servers = ['server0hostname', 'server1hostname']

# Experiment parameters
n_servers = 2 # Number of servers
node_config_mode = 'pegasus' # one of 'pegasus' 'netcache' or 'static'
alpha = 1.2 # Zipfian coefficient
get_ratio = 1.0 # Percentage of get requests (0.0 - 1.0)
n_server_threads = 1 # Number of server threads
n_client_threads = 1 # Number of client threads
interval = 100 # Interval (us) between client requests. Lower means higher request rate
key_type = 'zipf' # Key distribution. Either 'unif' (uniform) or 'zipf' (zipfian)
value_len = '128' # Value size (bytes)
n_keys = '1000000' # Number of keys (max 1M)
num_rkeys = 64 # Number of replicated keys
duration = '10' # Duration of the experiment (seconds)

# Do not modify anything below
log_dir = '/tmp/'
cur_dir = os.path.dirname(os.path.abspath(__file__))
config_file = cur_dir + '/testbed.config'
key_file = cur_dir + '/keys'
emulator = cur_dir + '/../emulation/bin/emulator'
reset = cur_dir + '/reset.py'
forcekill = ['sudo', 'killall', 'emulator']

class Task(object):
    def __init__(self, n_clients, n_servers, interval, get_ratio, alpha, num_rkeys):
        self.n_clients = n_clients
        self.n_servers = n_servers
        self.interval = interval
        self.get_ratio = get_ratio
        self.alpha = alpha
        self.num_rkeys = num_rkeys


def parse_result(task):
    # Overall result
    latencies = {}
    total_ops = 0
    completed_ops = 0
    issued_ops = 0
    for i in range(task.n_clients):
        first_line = True
        for line in open(log_dir+'stats_'+str(i)+'.log'):
            if first_line:
                ops_a, _, ops_b = line.partition(' ')
                completed_ops += int(ops_a)
                issued_ops += int(ops_b)
                first_line = False
            else:
                str_latency, _, str_count = line.partition(' ')
                latency = int(str_latency)
                count = int(str_count)
                current_count = latencies.get(latency, 0)
                latencies[latency] = current_count + count
                total_ops += count

    count = 0
    total_latency = 0
    med_latency = -1
    n_latency = -1
    nn_latency = -1
    for latency in sorted(latencies.keys()):
        total_latency += (latency * latencies[latency])
        count += latencies[latency]
        if count >= total_ops // 2 and med_latency == -1:
            med_latency = latency
        if count >= total_ops * 0.9 and n_latency == -1:
            n_latency = latency
        if count >= total_ops * 0.99 and nn_latency == -1:
            nn_latency = latency
    throughput = total_ops / int(duration)
    avg_latency = total_latency / total_ops
    completion_rate = math.ceil((float(completed_ops)/issued_ops) * 100)

    print "Throughput:", "{0:.2f}".format(throughput)
    print "Average Latency:", "{0:.2f}".format(avg_latency)
    print "Median Latency:", med_latency
    print "90% Latency:", n_latency
    print "99% Latency:", nn_latency
    print "Completion Rate:", completion_rate


if __name__ == "__main__":
    if key_type == 'zipf':
        print "Running", n_servers, "servers", get_ratio, "get", alpha, "zipf alpha"
    else:
        print "Running", n_servers, "servers", get_ratio, "get", "unif"

    n_clients = len(clients)
    job = Task(n_clients = n_clients,
               n_servers = n_servers,
               interval = interval,
               get_ratio = get_ratio,
               alpha = alpha,
               num_rkeys = num_rkeys)

    # Start all servers
    server_tasks = []
    for i in range(n_servers):
        server = servers[i]
        server_host = pyrem.host.RemoteHost(server)
        command = ['sudo', emulator,
                   '-b', '0',
                   '-c', config_file,
                   '-f', key_file,
                   '-r', '0',
                   '-e', str(i),
                   '-k', '0',
                   '-l', '0',
                   '-m', 'server',
                   '-n', n_keys,
                   '-o', 'dpdk',
                   '-q', 'kv',
                   '-v', value_len,
                   '-w', node_config_mode,
                   '-x', '1',
                   '-z', str(n_servers),
                   '-I', str(0),
                   '-J', str(0),
                   '-K', str(0),
                   '-L', str(n_server_threads),
                   '-M', str(0),
                   '-N', str(1),
                   '-O', '0',
                   '>', log_dir+'server_'+str(i)+'.log',
                   '2>&1']
        task = server_host.run(command, quiet=True)
        server_tasks.append(task)

    all_server_tasks = pyrem.task.Parallel(server_tasks, aggregate=False)
    all_server_tasks.start()

    # Reset controller
    command = ['sudo', reset,
               str(n_servers),
               str(num_rkeys)]
    pyrem.host.LocalHost().run(command).start(wait=True)

    time.sleep(5)
    # Start clients
    i = 0
    client_tasks = []
    for client in clients:
        client_host = pyrem.host.RemoteHost(client)
        command = ['sudo', emulator,
                   '-a', str(alpha),
                   '-b', '0',
                   '-c', config_file,
                   '-d', duration,
                   '-e', str(i),
                   '-f', key_file,
                   '-g', str(get_ratio),
                   '-i', str(interval),
                   '-k', '0',
                   '-m', 'client',
                   '-n', n_keys,
                   '-o', 'dpdk',
                   '-p', log_dir+'nodeops_'+str(i)+'.log',
                   '-q', 'kv',
                   '-s', log_dir+'stats_'+str(i)+'.log',
                   '-t', key_type,
                   '-v', value_len,
                   '-w', node_config_mode,
                   '-y', 'fixed',
                   '-z', str(n_servers),
                   '-D', str(0),
                   '-F', 'none',
                   '-I', str(0),
                   '-J', str(n_client_threads),
                   '-K', str(n_client_threads),
                   '-L', str(n_client_threads),
                   '-M', str(0),
                   '-N', str(1),
                   '-O', '0',
                   '>', log_dir+'client_'+str(i)+'.log',
                   '2>&1']
        client_tasks.append(client_host.run(command, quiet=True))
        i += 1
    pyrem.task.Parallel(client_tasks, aggregate=False).start(wait=True)

    # Collect client logs
    i = 0
    client_tasks = []
    for client in clients:
        client_host = pyrem.host.RemoteHost(client)
        client_tasks.append(client_host.get_file(log_dir+'stats_'+str(i)+'.log'))
        i += 1
    pyrem.task.Parallel(client_tasks, aggregate=False).start(wait=True)

    # Stop server and router
    all_server_tasks.stop()
    stop_tasks = []
    for i in range(n_servers):
        stop_tasks.append(pyrem.task.RemoteTask(servers[i], forcekill))
    pyrem.task.Parallel(stop_tasks, aggregate=False).start(wait=True)

    # Parse result
    parse_result(job)
