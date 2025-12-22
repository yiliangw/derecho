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

    // variable 'done' tracks the end of the test
    std::atomic<bool> done = false;
    // callback into the application code at each message delivery
    auto stability_callback = [&done,
                               total_num_messages,
                               num_delivered = 0u](uint32_t subgroup,
                                                   uint32_t sender_id,
                                                   long long int index,
                                                   std::optional<std::pair<uint8_t*, long long int>> data,
                                                   persistent::version_t ver) mutable {
        // Count the total number of messages delivered
        ++num_delivered;
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
            // the lambda function writes the message contents into the provided memory buffer
            // in this case, we do not touch the memory region
            raw_subgroup.send(msg_size, [](uint8_t* buf) {});
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
    long long int nanoseconds_elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
    // calculate bandwidth measured locally
    double bw;
    if(senders_mode == PartialSendMode::ALL_SENDERS) {
        bw = (msg_size * num_messages * num_nodes + 0.0) / nanoseconds_elapsed;
    } else if(senders_mode == PartialSendMode::HALF_SENDERS) {
        bw = (msg_size * num_messages * (num_nodes / 2) + 0.0) / nanoseconds_elapsed;
    } else {
        bw = (msg_size * num_messages + 0.0) / nanoseconds_elapsed;
    }
    // aggregate bandwidth from all nodes
    double avg_bw = aggregate_bandwidth(members_order, members_order[node_rank], bw);
    // log the result at the leader node
    if(node_rank == 0) {
        unsigned int window_size = getConfUInt32(derecho::Conf::SUBGROUP_DEFAULT_WINDOW_SIZE);
        rls_default_info("=== Performance Results ===");
        rls_default_info("num_nodes={} sender_selector={} msg_size={} window_size={} num_messages={} delivery_mode={} bandwidth={:.6f}GB/s time={}ns",
                         num_nodes, num_senders_selector, msg_size, window_size, num_messages, delivery_mode, avg_bw, nanoseconds_elapsed);
    }

    group.barrier_sync();
    group.leave();
}
