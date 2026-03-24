/*
 * This test is used for benchmarking Derecho for the Syntony project.
 *
 * This test measures the bandwidth of Derecho raw (uncooked) sends in GB/s as a function of
 * 1. the number of nodes 2. the number of senders (all sending, half nodes sending, one sending)
 * 3. message size 4. window size 5. number of messages sent per sender
 * 6. delivery mode (atomic multicast or unordered)
 * The test waits for every node to join and then each sender starts sending messages continuously
 * in the only subgroup that consists of all the nodes
 * Upon completion, the results are appended to file data_derecho_bw on the leader
 *
 * Test parameters can be configured in the config file under [SYNTONY_TEST] section:
 *   num_nodes, sender_selector, num_messages, delivery_mode, proc_name
 * Command line arguments override config file settings.
 */
#include "aggregate_bandwidth.hpp"
#include "partial_senders_allocator.hpp"

#include <derecho/core/derecho.hpp>
#include <derecho/utils/logger.hpp>

#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <vector>

using std::cout;
using std::endl;
using std::map;
using std::vector;

using namespace derecho;

// Config keys for SYNTONY_TEST section
static constexpr const char* SYNTONY_TEST_NUM_NODES = "SYNTONY_TEST/num_nodes";
static constexpr const char* SYNTONY_TEST_SENDER_SELECTOR = "SYNTONY_TEST/sender_selector";
static constexpr const char* SYNTONY_TEST_NUM_MESSAGES = "SYNTONY_TEST/num_messages";
static constexpr const char* SYNTONY_TEST_DELIVERY_MODE = "SYNTONY_TEST/delivery_mode";
static constexpr const char* SYNTONY_TEST_MSG_SIZE = "SYNTONY_TEST/msg_size";
static constexpr const char* SYNTONY_TEST_PROC_NAME = "SYNTONY_TEST/proc_name";
static constexpr const char* SYNTONY_TEST_LATENCY_BUF_SZ = "SYNTONY_TEST/latency_buf_sz";
static constexpr const char* SYNTONY_TEST_PROFILING_START = "SYNTONY_TEST/profiling_start";
static constexpr const char* SYNTONY_TEST_MAX_OUTSTANDING_REQS = "SYNTONY_TEST/max_outstanding_reqs";

#define DEFAULT_PROC_NAME "bw_test"

int main(int argc, char* argv[]) {
    // Read configurations from the command line options as well as the default config file
    Conf::initialize(argc, argv);

    // Find position of "--" separator for legacy command line arguments
    int dashdash_pos = argc - 1;
    while(dashdash_pos > 0) {
        if(strcmp(argv[dashdash_pos], "--") == 0) {
            break;
        }
        dashdash_pos--;
    }

    // Determine test parameters: config file values with command line overrides
    uint32_t num_nodes;
    uint32_t num_senders_selector;
    uint32_t num_messages;
    uint32_t delivery_mode;
    std::string proc_name = DEFAULT_PROC_NAME;

    // Check if command line arguments are provided (legacy mode)
    bool has_cmdline_args = (argc - dashdash_pos) >= 5;

    if(has_cmdline_args) {
        // Command line arguments override config file
        num_nodes = std::stoi(argv[dashdash_pos + 1]);
        num_senders_selector = std::stoi(argv[dashdash_pos + 2]);
        num_messages = std::stoi(argv[dashdash_pos + 3]);
        delivery_mode = std::stoi(argv[dashdash_pos + 4]);
        if(dashdash_pos + 5 < argc) {
            proc_name = argv[dashdash_pos + 5];
        }
    } else {
        // Read from config file
        if(!hasCustomizedConfKey(SYNTONY_TEST_NUM_NODES)) {
            cout << "Error: num_nodes not specified in config or command line." << endl;
            cout << "USAGE: " << argv[0] << " [ derecho-config-list -- ] num_nodes sender_selector num_messages delivery_mode [proc_name]" << endl;
            cout << "Or set [SYNTONY_TEST] section in config file with: num_nodes, sender_selector, num_messages, delivery_mode" << endl;
            return -1;
        }
        num_nodes = getConfUInt32(SYNTONY_TEST_NUM_NODES);
        num_senders_selector = hasCustomizedConfKey(SYNTONY_TEST_SENDER_SELECTOR)
            ? getConfUInt32(SYNTONY_TEST_SENDER_SELECTOR) : 0;
        num_messages = hasCustomizedConfKey(SYNTONY_TEST_NUM_MESSAGES)
            ? getConfUInt32(SYNTONY_TEST_NUM_MESSAGES) : 1000;
        delivery_mode = hasCustomizedConfKey(SYNTONY_TEST_DELIVERY_MODE)
            ? getConfUInt32(SYNTONY_TEST_DELIVERY_MODE) : 0;
        if(hasCustomizedConfKey(SYNTONY_TEST_PROC_NAME)) {
            proc_name = getConfString(SYNTONY_TEST_PROC_NAME);
        }
    }

    uint32_t latency_buf_sz = hasCustomizedConfKey(SYNTONY_TEST_LATENCY_BUF_SZ)
        ? getConfUInt32(SYNTONY_TEST_LATENCY_BUF_SZ) : 0;
    uint32_t profiling_start = hasCustomizedConfKey(SYNTONY_TEST_PROFILING_START)
        ? getConfUInt32(SYNTONY_TEST_PROFILING_START) : 0;
    // 0 means no application-level limit (only Derecho's window_size applies)
    uint32_t max_outstanding_reqs = hasCustomizedConfKey(SYNTONY_TEST_MAX_OUTSTANDING_REQS)
        ? getConfUInt32(SYNTONY_TEST_MAX_OUTSTANDING_REQS) : 0;

    // Convert sender_selector to enum
    const PartialSendMode senders_mode = num_senders_selector == 0
                                                 ? PartialSendMode::ALL_SENDERS
                                                 : (num_senders_selector == 1
                                                            ? PartialSendMode::HALF_SENDERS
                                                            : PartialSendMode::ONE_SENDER);

    pthread_setname_np(pthread_self(), proc_name.c_str());

    // Compute the total number of messages that should be delivered
    uint64_t total_num_messages = 0;
    switch(senders_mode) {
        case PartialSendMode::ALL_SENDERS:
            total_num_messages = num_messages * num_nodes;
            break;
        case PartialSendMode::HALF_SENDERS:
            total_num_messages = num_messages * (num_nodes / 2);
            break;
        case PartialSendMode::ONE_SENDER:
            total_num_messages = num_messages;
            break;
    }

    // Same pattern as DPDK LocalServer::RequestHeader
    struct RequestHeader {
        uint32_t src;
        uint64_t timestamp_ns;
    };

    // variable 'done' tracks the end of the test
    std::atomic<bool> done = false;

    // Per-message latency tracking (for locally sent messages only)
    std::vector<uint64_t> latencies(latency_buf_sz);
    std::atomic<uint32_t> latency_cnt{0};
    // Node ID (not rank) for matching sender_id in callbacks
    uint32_t my_id;

    // Application-level outstanding request tracking
    std::atomic<uint32_t> outstanding{0};

    // Bandwidth profiling: record time at profiling_start-th and last delivery
    std::atomic<bool> profiling_started{profiling_start == 0};
    std::chrono::steady_clock::time_point profiling_start_time;

    // callback into the application code at each message delivery
    // Compute total warmup deliveries based on send mode
    uint64_t total_profiling_start = 0;
    switch(senders_mode) {
        case PartialSendMode::ALL_SENDERS:
            total_profiling_start = static_cast<uint64_t>(profiling_start) * num_nodes;
            break;
        case PartialSendMode::HALF_SENDERS:
            total_profiling_start = static_cast<uint64_t>(profiling_start) * (num_nodes / 2);
            break;
        case PartialSendMode::ONE_SENDER:
            total_profiling_start = profiling_start;
            break;
    }

    auto stability_callback = [&done, &latencies, &latency_cnt, &my_id,
                               &profiling_started, &profiling_start_time,
                               &outstanding,
                               total_num_messages, latency_buf_sz, profiling_start,
                               total_profiling_start,
                               num_delivered = 0u](uint32_t subgroup,
                                                   uint32_t sender_id,
                                                   long long int index,
                                                   std::optional<std::pair<uint8_t*, long long int>> data,
                                                   persistent::version_t ver) mutable {
        // Record latency for locally sent messages by reading RequestHeader from payload.
        // Skip the first profiling_start messages as warmup.
        if(latency_buf_sz > 0 && data.has_value() &&
           data->second >= static_cast<long long int>(sizeof(RequestHeader)) &&
           index >= static_cast<long long int>(profiling_start)) {
            RequestHeader hdr;
            std::memcpy(&hdr, data->first, sizeof(hdr));
            if(hdr.src == my_id) {
                uint32_t idx = latency_cnt.load(std::memory_order_relaxed);
                if(idx < latency_buf_sz) {
                    auto now = std::chrono::steady_clock::now();
                    uint64_t now_ns = static_cast<uint64_t>(
                        std::chrono::duration_cast<std::chrono::nanoseconds>(
                            now.time_since_epoch()).count());
                    latencies[idx] = now_ns - hdr.timestamp_ns;
                    latency_cnt.store(idx + 1, std::memory_order_relaxed);
                }
            }
        }
        // Decrement outstanding counter for locally sent messages
        if(sender_id == my_id) {
            outstanding.fetch_sub(1, std::memory_order_relaxed);
        }
        // Count the total number of messages delivered
        ++num_delivered;
        // Record bandwidth profiling start time after warmup
        if(!profiling_started.load(std::memory_order_relaxed) &&
           num_delivered >= total_profiling_start) {
            profiling_start_time = std::chrono::steady_clock::now();
            profiling_started.store(true, std::memory_order_relaxed);
        }
        // Check for completion
        if(num_delivered == total_num_messages) {
            done = true;
        }
    };

    Mode mode = Mode::ORDERED;
    if(delivery_mode) {
        mode = Mode::UNORDERED;
    }

    auto membership_function = PartialSendersAllocator(num_nodes, senders_mode, mode);

    //Wrap the membership function in a SubgroupInfo
    SubgroupInfo one_raw_group(membership_function);

    // join the group
    Group<RawObject> group(UserMessageCallbacks{stability_callback},
                           one_raw_group, {}, std::vector<view_upcall_t>{},
                           &raw_object_factory);

    cout << "Finished constructing/joining Group" << endl;
    auto members_order = group.get_members();
    uint32_t node_rank = group.get_my_rank();
    my_id = members_order[node_rank];

    long long unsigned int max_payload_size = getConfUInt64(derecho::Conf::SUBGROUP_DEFAULT_MAX_PAYLOAD_SIZE);
    // Use msg_size from config if specified, otherwise use max_payload_size
    long long unsigned int msg_size = hasCustomizedConfKey(SYNTONY_TEST_MSG_SIZE)
        ? getConfUInt64(SYNTONY_TEST_MSG_SIZE) : max_payload_size;

    if(msg_size > max_payload_size) {
        cout << "Error: msg_size (" << msg_size << ") exceeds max_payload_size (" << max_payload_size << ")" << endl;
        group.leave();
        return -1;
    }

    // this function sends all the messages
    auto send_all = [&]() {
        Replicated<RawObject>& raw_subgroup = group.get_subgroup<RawObject>();
        for(uint i = 0; i < num_messages; ++i) {
            // Application-level flow control
            if(max_outstanding_reqs > 0) {
                while(outstanding.load(std::memory_order_relaxed) >= max_outstanding_reqs) {
                    // Spin until a delivery callback frees a slot
                }
            }
            outstanding.fetch_add(1, std::memory_order_relaxed);
            raw_subgroup.send(msg_size, [&](uint8_t* buf) {
                if(msg_size >= sizeof(RequestHeader)) {
                    RequestHeader hdr;
                    hdr.src = my_id;
                    auto now = std::chrono::steady_clock::now();
                    hdr.timestamp_ns = static_cast<uint64_t>(
                        std::chrono::duration_cast<std::chrono::nanoseconds>(
                            now.time_since_epoch()).count());
                    std::memcpy(buf, &hdr, sizeof(hdr));
                }
            });
        }
    };

    // start timer
    auto start_time = std::chrono::steady_clock::now();
    // send all messages or skip if not a sender
    if(senders_mode == PartialSendMode::ALL_SENDERS) {
        send_all();
    } else if(senders_mode == PartialSendMode::HALF_SENDERS) {
        if(node_rank > (num_nodes - 1) / 2) {
            send_all();
        }
    } else {
        if(node_rank == num_nodes - 1) {
            send_all();
        }
    }
    // wait for the test to finish
    while(!done) {
    }
    // end timer
    auto end_time = std::chrono::steady_clock::now();
    // Use profiling_start_time for bandwidth if warmup was configured
    auto effective_start = profiling_started.load(std::memory_order_relaxed)
        ? profiling_start_time : start_time;
    long long int nanoseconds_elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - effective_start).count();
    uint32_t profiled_per_sender = num_messages - profiling_start;
    // Total profiled messages replicated across all senders
    uint64_t total_profiled;
    if(senders_mode == PartialSendMode::ALL_SENDERS) {
        total_profiled = static_cast<uint64_t>(profiled_per_sender) * num_nodes;
    } else if(senders_mode == PartialSendMode::HALF_SENDERS) {
        total_profiled = static_cast<uint64_t>(profiled_per_sender) * (num_nodes / 2);
    } else {
        total_profiled = profiled_per_sender;
    }

    // Dump per-message latencies for locally sent messages
    uint32_t final_lat_cnt = latency_cnt.load(std::memory_order_relaxed);
    if(final_lat_cnt > 0) {
        cout << "Latency samples (" << final_lat_cnt << " entries):";
        __uint128_t lat_sum = 0;
        for(uint32_t i = 0; i < final_lat_cnt; i++) {
            cout << " " << latencies[i];
            lat_sum += latencies[i];
        }
        cout << endl;
        uint64_t lat_avg = static_cast<uint64_t>(lat_sum / final_lat_cnt);
        cout << "Average latency: " << lat_avg << " ns" << endl;
    } else {
        cout << "No latency samples recorded (non-sender)" << endl;
    }

    // calculate throughput
    double bw = (msg_size * total_profiled + 0.0) / nanoseconds_elapsed;
    double throughput = (total_profiled * 1e9) / nanoseconds_elapsed;
    // aggregate bandwidth from all nodes
    double avg_bw = aggregate_bandwidth(members_order, members_order[node_rank], bw);
    // log the result
    unsigned int window_size = getConfUInt32(derecho::Conf::SUBGROUP_DEFAULT_WINDOW_SIZE);
    cout << "=== Performance Results ===" << endl;
    cout << "num_nodes=" << num_nodes
         << " sender_selector=" << num_senders_selector
         << " msg_size=" << msg_size
         << " window_size=" << window_size
         << " max_outstanding_reqs=" << max_outstanding_reqs
         << " num_messages=" << num_messages
         << " delivery_mode=" << delivery_mode
         << " bandwidth=" << avg_bw << "GB/s"
         << " throughput=" << throughput << "msg/s"
         << " total_profiled=" << total_profiled
         << " time=" << nanoseconds_elapsed << "ns" << endl;

    group.barrier_sync();
    group.leave();
}
