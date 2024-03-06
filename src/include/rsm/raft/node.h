#pragma once

#include <unistd.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdarg>
#include <ctime>
#include <filesystem>
#include <memory>
#include <mutex>
#include <thread>

#include "block/manager.h"
#include "common/logger.h"
#include "common/util.h"

#include "librpc/client.h"
#include "librpc/server.h"
#include "rsm/raft/log.h"
#include "rsm/raft/protocol.h"
#include "rsm/state_machine.h"
#include "utils/thread_pool.h"

namespace chfs {

    class Timer {
    public:
        Timer(int random, int base) : random(random), base(base) { interval = generator.rand(0, random) + base; };

        void reset() {
            interval = generator.rand(0, random) + base;
            start_time = std::chrono::steady_clock::now();
            receive_heartbeat = false;
        }

        auto get_interval() const { return std::chrono::milliseconds(interval); }

        void start() {
            start_time = std::chrono::steady_clock::now();
            state = true;
        }

        void stop() { state = false; }

        void receive() { receive_heartbeat = true; }

        bool check_receive() { return receive_heartbeat.load(); }

        bool timeout() {
            if (!state) {
                return false;
            }
            auto curr_time = std::chrono::steady_clock::now();
            if (const auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                        curr_time - start_time).count();
                    duration > interval) {
                reset();
                return true;
            }
            return false;
        }

    private:
        int random;
        int base;
        int interval;
        std::chrono::steady_clock::time_point start_time;
        RandomNumberGenerator generator{};
        std::atomic<bool> receive_heartbeat{false};
        std::atomic<bool> state{false};
    };

    struct PersistData {
        int term;
        int votedFor;
        int blockNum;
    };
    enum class RaftRole {
        Follower, Candidate, Leader
    };
    struct RaftNodeConfig {
        int node_id;
        uint16_t port;
        std::string ip_address;
    };

    template<typename StateMachine, typename Command>
    class RaftNode {
    public:
        RaftNode(int node_id, std::vector<RaftNodeConfig> node_configs);

        ~RaftNode();

        /* interfaces for test */
        void set_network(std::map<int, bool> &network_availability);

        void set_reliable(bool flag);

        int get_list_state_log_num();

        int rpc_count();

        std::vector<u8> get_snapshot_direct();

    private:
        /*
         * Start the raft node.
         * Please make sure all of the rpc request handlers have been registered before this method.
         */
        auto start() -> int;

        /*
         * Stop the raft node.
         */
        auto stop() -> int;

        /* Returns whether this node is the leader, you should also return the current term. */
        auto is_leader() -> std::tuple<bool, int>;

        /* Checks whether the node is stopped */
        auto is_stopped() -> bool;

        /*
         * Send a new command to the raft nodes.
         * The returned tuple of the method contains three values:
         * 1. bool:  True if this raft node is the leader that successfully appends the log,
         *      false If this node is not the leader.
         * 2. int: Current term.
         * 3. int: Log index.
         */
        auto new_command(std::vector<u8> cmd_data, int cmd_size) -> std::tuple<bool, int, int>;

        /* Save a snapshot of the state machine and compact the log. */
        auto save_snapshot() -> bool;

        /* Get a snapshot of the state machine */
        auto get_snapshot() -> std::vector<u8>;

        /* Internal RPC handlers */
        auto request_vote(RequestVoteArgs arg) -> RequestVoteReply;

        auto append_entries(RpcAppendEntriesArgs arg) -> AppendEntriesReply;

        auto install_snapshot(InstallSnapshotArgs arg) -> InstallSnapshotReply;

        /* RPC helpers */
        void send_request_vote(int target, RequestVoteArgs arg);

        void handle_request_vote_reply(int target, RequestVoteArgs arg, RequestVoteReply reply);

        void send_append_entries(int target, AppendEntriesArgs<Command> arg);

        void handle_append_entries_reply(int target, AppendEntriesArgs<Command> arg, AppendEntriesReply reply);

        void send_install_snapshot(int target, InstallSnapshotArgs arg);

        void handle_install_snapshot_reply(int target, InstallSnapshotArgs arg, InstallSnapshotReply reply);

        /* background workers */
        void run_background_ping();

        void run_background_election();

        void run_background_commit();

        void run_background_apply();

        /* Data structures */
        bool network_stat; /* for test */

        std::mutex mtx;         /* A big lock to protect the whole data structure. */
        std::mutex clients_mtx; /* A lock to protect RpcClient pointers */
        std::unique_ptr<ThreadPool> thread_pool;
        std::unique_ptr<RaftLog<Command>> log_storage; /* To persist the raft log. */
        std::unique_ptr<StateMachine> state;           /*  The state machine that applies the raft log, e.g. a kv store. */

        std::unique_ptr<RpcServer> rpc_server;                     /* RPC server to recieve and handle the RPC requests. */
        std::map<int, std::unique_ptr<RpcClient>> rpc_clients_map; /* RPC clients of all raft nodes including this node. */
        std::vector<RaftNodeConfig> node_configs;                  /* Configuration for all nodes */
        int my_id;                                                 /* The index of this node in rpc_clients, start from 0. */

        std::atomic_bool stopped;

        RaftRole role;
        int current_term;
        int leader_id;

        std::unique_ptr<std::thread> background_election;
        std::unique_ptr<std::thread> background_ping;
        std::unique_ptr<std::thread> background_commit;
        std::unique_ptr<std::thread> background_apply;

        /* Lab3: Your code here */

        std::shared_ptr<BlockManager> bm;

        int votedFor;
        std::atomic<int> grantedVote;
        Timer voteTimer;
        int lastIncludeIndex{0};
        int commitIndex;
        std::map<int, int> nextIndex;
        std::map<int, int> matchIndex;

        void persist();
        void recover();
    };

    template<typename StateMachine, typename Command>
    RaftNode<StateMachine, Command>::RaftNode(int node_id, std::vector<RaftNodeConfig> configs)
            : network_stat(true),
              node_configs(configs),
              my_id(node_id),
              stopped(true),
              role(RaftRole::Follower),
              current_term(0),
              leader_id(-1),
              votedFor(-1),
              grantedVote(0),
              voteTimer(50, 500),
              commitIndex(0) {
        auto my_config = node_configs[my_id];

        /* launch RPC server */
        rpc_server = std::make_unique<RpcServer>(my_config.ip_address, my_config.port);

        /* Register the RPCs. */
        rpc_server->bind(RAFT_RPC_START_NODE, [this]() { return this->start(); });
        rpc_server->bind(RAFT_RPC_STOP_NODE, [this]() { return this->stop(); });
        rpc_server->bind(RAFT_RPC_CHECK_LEADER, [this]() { return this->is_leader(); });
        rpc_server->bind(RAFT_RPC_IS_STOPPED, [this]() { return this->is_stopped(); });
        rpc_server->bind(RAFT_RPC_NEW_COMMEND,
                         [this](std::vector<u8> data, int cmd_size) { return this->new_command(data, cmd_size); });
        rpc_server->bind(RAFT_RPC_SAVE_SNAPSHOT, [this]() { return this->save_snapshot(); });
        rpc_server->bind(RAFT_RPC_GET_SNAPSHOT, [this]() { return this->get_snapshot(); });

        rpc_server->bind(RAFT_RPC_REQUEST_VOTE, [this](RequestVoteArgs arg) { return this->request_vote(arg); });
        rpc_server->bind(RAFT_RPC_APPEND_ENTRY, [this](RpcAppendEntriesArgs arg) { return this->append_entries(arg); });
        rpc_server->bind(RAFT_RPC_INSTALL_SNAPSHOT,
                         [this](InstallSnapshotArgs arg) { return this->install_snapshot(arg); });

        /* Lab3: Your code here */
        thread_pool = std::make_unique<ThreadPool>(16);
        auto log_filename = fmt::format("/tmp/raft_log/{}.log", my_id);
        bm = std::make_shared<BlockManager>(log_filename);
        log_storage = std::make_unique<RaftLog<Command>>(bm);
        recover();
        for (const auto &node: node_configs) {
            if (node.node_id == my_id) {
                continue;
            }
            matchIndex[node.node_id] = 0;
            nextIndex[node.node_id] = log_storage->getSize();
        }
        state = std::make_unique<StateMachine>();
        rpc_server->run(true, configs.size());
    }

    template<typename StateMachine, typename Command>
    RaftNode<StateMachine, Command>::~RaftNode() {
        stop();

        thread_pool.reset();
        rpc_server.reset();
        state.reset();
        log_storage.reset();
    }

/******************************************************************

                        RPC Interfaces

*******************************************************************/

    template<typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::start() -> int {
        /* Lab3: Your code here */
        for (const auto &node: node_configs) {
            rpc_clients_map[node.node_id] = std::make_unique<RpcClient>(node.ip_address, node.port, true);
        }
        stopped = false;
        role = RaftRole::Follower;
        voteTimer.start();
        background_election = std::make_unique<std::thread>(&RaftNode::run_background_election, this);
        background_ping = std::make_unique<std::thread>(&RaftNode::run_background_ping, this);
        background_commit = std::make_unique<std::thread>(&RaftNode::run_background_commit, this);
        background_apply = std::make_unique<std::thread>(&RaftNode::run_background_apply, this);
        return 0;
    }

    template<typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::stop() -> int {
        /* Lab3: Your code here */
        stopped = true;
        background_election->join();
        background_ping->join();
        background_commit->join();
        background_apply->join();
        return 0;
    }

    template<typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::is_leader() -> std::tuple<bool, int> {
        /* Lab3: Your code here */
        return std::make_tuple(role == RaftRole::Leader, current_term);
    }

    template<typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::is_stopped() -> bool {
        return stopped.load();
    }

    template<typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::new_command(std::vector<u8> cmd_data, int cmd_size)
    -> std::tuple<bool, int, int> {
        /* Lab3: Your code here */
        std::scoped_lock<std::mutex> lock(mtx);
        if (role != RaftRole::Leader) {
            return {false, current_term, log_storage->getSize() - 1};
        }
        Entry<Command> entry;
        entry.term = current_term;
        Command cmd;
        cmd.deserialize(cmd_data, cmd_size);
        entry.command = cmd;
        log_storage->appendEntry(entry);
        return std::make_tuple(true, current_term, log_storage->getSize() - 1);
    }

    template<typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::save_snapshot() -> bool {
        /* Lab3: Your code here */
//        return true;
        lastIncludeIndex = log_storage->getSize() - 1; // Set it to the last log entry index
        return true;


    }

    template<typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::get_snapshot() -> std::vector<u8> {
//        return std::vector<u8>{};
        auto snapshot_data = log_storage->readSnapshot();
        return snapshot_data;
    }

/******************************************************************

                         Internal RPC Related

*******************************************************************/

    template<typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::request_vote(RequestVoteArgs args) -> RequestVoteReply {
        /* Lab3: Your code here */
        std::scoped_lock<std::mutex> lock(mtx);
        RequestVoteReply res;
        if (args.term < current_term) {
            res.term = current_term;
            res.voteGranted = false;
            return res;
        }
        if (votedFor == args.candidateId) {
            res.term = current_term;
            res.voteGranted = true;
            return res;
        }
        auto lastEntryIndex = log_storage->getSize() - 1;
        auto lastEntryTerm = log_storage->getLogEntry(lastEntryIndex).term;

        if (lastEntryTerm > args.lastLogTerm ||
            (lastEntryTerm == args.lastLogTerm && lastEntryIndex > args.lastLogIndex)) {
            res.term = current_term;
            res.voteGranted = false;
            return res;
        }
        if (votedFor == my_id) {
            grantedVote--;
        }
        role = RaftRole::Follower;
        leader_id = -1;
        grantedVote = 0;
        voteTimer.start();
        persist();

        votedFor = args.candidateId;
        res.term = current_term;
        res.voteGranted = true;
        return res;
    }

    template<typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::handle_request_vote_reply(int target, const RequestVoteArgs arg,
                                                                    const RequestVoteReply reply) {
        /* Lab3: Your code here */
        if (reply.term > current_term) {
            current_term = reply.term;
        }
        if (reply.voteGranted && role == RaftRole::Candidate) {
            grantedVote++;
            if (grantedVote > node_configs.size() / 2) {
                role = RaftRole::Leader;
                leader_id = my_id;
                grantedVote = 0;
                votedFor = -1;
                voteTimer.stop();
                persist();
            }
        }
    }

    template<typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::append_entries(RpcAppendEntriesArgs rpc_arg) -> AppendEntriesReply {
        /* Lab3: Your code here */
        std::scoped_lock<std::mutex> lock(mtx);
        auto arg = transform_rpc_append_entries_args<Command>(rpc_arg);
        AppendEntriesReply res;
        if (arg.term < current_term) {
            res.term = current_term;
            res.success = false;
            return res;
        }
        if (arg.heartBeat == 1) {
            if (arg.term >= current_term) {
                votedFor = -1;
                role = RaftRole::Follower;
                current_term = arg.term;
                votedFor = -1;
                grantedVote = 0;
                voteTimer.start();
                persist();
            }
            leader_id = arg.leaderId;
            voteTimer.receive();
            res.term = current_term;
            res.success = true;
            return res;
        }
        if (arg.lastIncludeIndex != 0) {
            lastIncludeIndex = arg.lastIncludeIndex;
            log_storage->deleteAfterI(arg.prevLogIndex + 1);
            for (int i = 0; i < arg.entries.size(); ++i) {
                log_storage->insert(arg.prevLogIndex + i + 1, arg.entries.at(i));
            }
            commitIndex = std::min(log_storage->getSize() - 1, arg.leaderCommit);
            res.term = current_term;
            res.success = true;
            return res;
        }
        if (arg.prevLogIndex != 0 && !(arg.prevLogIndex <= log_storage->getSize() - 1 &&
                                       log_storage->getLogEntry(arg.prevLogIndex).term == arg.prevLogTerm)) {
            res.term = current_term;
            res.success = false;
            return res;
        }
        log_storage->deleteAfterI(arg.prevLogIndex + 1);
        for (int i = 0; i < arg.entries.size(); ++i) {
            log_storage->insert(arg.prevLogIndex + i + 1, arg.entries[i]);
        }
        if (log_storage->getSize() - 1 > arg.leaderCommit) {
            commitIndex = arg.leaderCommit;
        } else {
            commitIndex = log_storage->getSize() - 1;
        }
        res.term = current_term;
        res.success = true;
        return res;
    }

    template<typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::handle_append_entries_reply(int target, const AppendEntriesArgs<Command> arg,
                                                                      const AppendEntriesReply reply) {
        /* Lab3: Your code here */
        if (role != RaftRole::Leader) {
            return;
        }
        if (!reply.success) {
            if (reply.term > current_term) {
                role = RaftRole::Follower;
                voteTimer.start();
                current_term = reply.term;
                leader_id = target;
                votedFor = -1;
                grantedVote = 0;
                persist();
                return;
            }
            nextIndex[target] = arg.prevLogIndex;
            return;
        }
        if (arg.heartBeat) {
            return;
        }
        auto agree_index = arg.prevLogIndex + arg.entries.size();
        matchIndex[target] = agree_index;
        nextIndex[target] = matchIndex[target] + 1;
        //
        for (int N = commitIndex + 1; N <= agree_index; ++N) {
            int agreeNum = 1;
            for (auto [nodeId, index]: matchIndex) {
                if (index >= N) {
                    agreeNum++;
                }
            }
            if (agreeNum >= node_configs.size() / 2 + 1 && log_storage->getLogEntry(N).term == current_term) {
                commitIndex = N;
            }
        }
    }

    template<typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::install_snapshot(InstallSnapshotArgs args) -> InstallSnapshotReply {
        /* Lab3: Your code here */
//        return InstallSnapshotReply();
        std::lock_guard<std::mutex> lock(mtx);

        InstallSnapshotReply reply;

        // Check if the term in the snapshot is outdated
        if (args.term < current_term) {
            reply.term = current_term;
            return reply;
        }
        // - Check if the last included index matches the expected value
        // - Write the snapshot chunk data to a temporary storage
        // - If 'done' is true, apply the snapshot to the state machine and update node state
        // - Update the current term and other relevant state information

        if (args.lastIncludedIndex != lastIncludeIndex) {
            // Error: The received snapshot chunk is not aligned with the expected index.
            // Handle the error as needed.
            return reply;
        }
        if (args.done) {
            // Apply the snapshot to the state machine (you need to implement this)
            // Update other relevant state information

            // Example:
            // state_machine->applySnapshot(snapshot_buffer);

            lastIncludeIndex = args.lastIncludedIndex;
            commitIndex = std::max(commitIndex, lastIncludeIndex);
            current_term = std::max(current_term, args.term);
            leader_id = -1;
            votedFor = -1;
            grantedVote = 0;
            role = RaftRole::Follower;
            voteTimer.start();
//            persist();
        }

        reply.term = current_term;
        return reply;
    }

    template<typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::handle_install_snapshot_reply(int target, const InstallSnapshotArgs arg,
                                                                        const InstallSnapshotReply reply) {
        /* Lab3: Your code here */
        std::lock_guard<std::mutex> lock(mtx);

        // Check if the term in the reply is outdated
        if (reply.term < current_term) {
            // The reply term is outdated, ignore the reply
            return;
        }

        // Check if the reply indicates success or failure
        if (reply.term > current_term || (reply.term == current_term)) {
            // The reply is from a more recent term or it indicates success
            // Update the nextIndex and matchIndex for the target node

            if (reply.term > current_term) {
                // If the reply is from a more recent term, update the current term
                current_term = reply.term;
                leader_id = -1;
                votedFor = -1;
                grantedVote = 0;
                role = RaftRole::Follower;
                voteTimer.start();
            }

            // Update nextIndex and matchIndex based on the success of the snapshot installation

            nextIndex[target] = arg.lastIncludedIndex + 1;
            matchIndex[target] = arg.lastIncludedIndex;

        }
    }

    template<typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::send_request_vote(int target_id, RequestVoteArgs arg) {
        std::unique_lock<std::mutex> clients_lock(clients_mtx);
        if (rpc_clients_map[target_id] == nullptr ||
            rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
            return;
        }
        auto res = rpc_clients_map[target_id]->call(RAFT_RPC_REQUEST_VOTE, arg);
        clients_lock.unlock();
        if (res.is_ok()) {
            handle_request_vote_reply(target_id, arg, res.unwrap()->template as<RequestVoteReply>());
        } else {
            // RPC fails
        }
    }

    template<typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::send_append_entries(int target_id, AppendEntriesArgs<Command> arg) {
        std::unique_lock<std::mutex> clients_lock(clients_mtx);
        if (rpc_clients_map[target_id] == nullptr ||
            rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
            return;
        }

        RpcAppendEntriesArgs rpc_arg = transform_append_entries_args(arg);
        auto res = rpc_clients_map[target_id]->call(RAFT_RPC_APPEND_ENTRY, rpc_arg);
        clients_lock.unlock();
        if (res.is_ok()) {
            handle_append_entries_reply(target_id, arg, res.unwrap()->template as<AppendEntriesReply>());
        } else {
            // RPC fails
        }
    }

    template<typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::send_install_snapshot(int target_id, InstallSnapshotArgs arg) {
        std::unique_lock<std::mutex> clients_lock(clients_mtx);
        if (rpc_clients_map[target_id] == nullptr ||
            rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
            return;
        }

        auto res = rpc_clients_map[target_id]->call(RAFT_RPC_INSTALL_SNAPSHOT, arg);
        clients_lock.unlock();
        if (res.is_ok()) {
            handle_install_snapshot_reply(target_id, arg, res.unwrap()->template as<InstallSnapshotReply>());
        } else {
            // RPC fails
        }
    }

/******************************************************************

                        Background Workers

*******************************************************************/

    template<typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::run_background_election() {
        // Periodly check the liveness of the leader.

        // Work for followers and candidates.

        /* Uncomment following code when you finish */
        while (true) {
            {
                if (is_stopped()) {
                    return;
                }
                /* Lab3: Your code here */
                std::this_thread::sleep_for(voteTimer.get_interval());
                if (role == RaftRole::Leader||!rpc_clients_map[my_id]) {
                    continue;
                }
                auto receive = voteTimer.check_receive();
                auto timeout = voteTimer.timeout();
                if (role == RaftRole::Follower) {
                    if (timeout && !receive) {
                        //become candidate
                        role = RaftRole::Candidate;
                        votedFor = my_id;
                        grantedVote = 1;
                        voteTimer.start();
                        persist();
                    }
                }
                if (!timeout || receive) {
                    continue;
                }
                current_term++;
                votedFor = my_id;
                grantedVote = 1;
                if (role != RaftRole::Candidate) {
                    continue;
                }
                RequestVoteArgs res;
                res.term = current_term;
                res.candidateId = my_id;
                res.lastLogTerm = log_storage->getLogEntry(log_storage->getSize() - 1).term;
                res.lastLogIndex = log_storage->getSize() - 1;
                for (const auto &node: node_configs) {
                    if (node.node_id == my_id) {
                        continue;
                    }
                    thread_pool->enqueue(&RaftNode::send_request_vote, this, node.node_id, res);
                }
            }
        }
    }

    template<typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::run_background_commit() {
        // Periodly send logs to the follower.

        // Only work for the leader.

        /* Uncomment following code when you finish */
        while (true) {
            {
                if (is_stopped()) {
                    return;
                }
                /* Lab3: Your code here */
                std::this_thread::sleep_for(std::chrono::milliseconds{300});
                if (role != RaftRole::Leader) {
                    continue;
                }
                if (!rpc_clients_map[my_id]) {
                    role = RaftRole::Follower;
                    leader_id = -1;
                    votedFor = -1;
                    grantedVote = 0;
                    voteTimer.start();
                    persist();
                    continue;
                }
                for (const auto &node: node_configs) {
                    if (node.node_id == my_id) {
                        continue;
                    }
                    if (!rpc_clients_map[node.node_id]) {
                        continue;
                    }
                    auto next_idx = nextIndex[node.node_id];
                    auto prev_idx = next_idx - 1;
                    if (prev_idx > log_storage->getSize() - 1) {
                        continue;
                    }
                    auto logEntry = log_storage->getLogEntry(prev_idx);
                    auto pTerm = logEntry.term;
                    auto entries = log_storage->getAfterI(prev_idx);
                    AppendEntriesArgs<Command> res;
                    res.term = current_term;
                    res.leaderId = my_id;
                    res.prevLogIndex = prev_idx;
                    res.prevLogTerm = pTerm;
                    res.leaderCommit = commitIndex;
                    res.heartBeat = false;
                    res.lastIncludeIndex = lastIncludeIndex;
                    res.entries = std::vector<Entry<Command>>(entries.begin(), entries.end());
                    thread_pool->enqueue(&RaftNode::send_append_entries, this, node.node_id, res);
                }
            }
        }
    }

    template<typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::run_background_apply() {
        // Periodly apply committed logs the state machine

        // Work for all the nodes.

        /* Uncomment following code when you finish */
        while (true) {
            {
                if (is_stopped()) {
                    return;
                }
                /* Lab3: Your code here */
                std::this_thread::sleep_for(std::chrono::milliseconds{300});
                persist();
                for (int i = state->store.size(); i <= commitIndex; ++i) {
                    auto entry = log_storage->getLogEntry(i);
                    state->apply_log(entry.command);
                }
                state->num_append_logs = 0;
                persist();
            }
        }
    }

    template<typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::run_background_ping() {
        // Periodly send empty append_entries RPC to the followers.

        // Only work for the leader.

        /* Uncomment following code when you finish */
        while (true) {
            {
                if (is_stopped()) {
                    return;
                }
                /* Lab3: Your code here */
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                if (role != RaftRole::Leader) {
                    continue;
                }
                if (!rpc_clients_map[my_id]) {
                    role = RaftRole::Follower;
                    leader_id = -1;
                    votedFor = -1;
                    grantedVote = 0;
                    voteTimer.start();
                    persist();
                    continue;
                }
                for (const auto &node: node_configs) {
                    if (node.node_id == my_id) {
                        continue;
                    }
                    AppendEntriesArgs<Command> res;
                    res.term = current_term;
                    res.leaderId = my_id;
                    res.prevLogIndex = -1;
                    res.prevLogTerm = -1;
                    res.leaderCommit = commitIndex;
                    res.heartBeat = true;
                    res.lastIncludeIndex = lastIncludeIndex;
                    res.entries = {};
                    thread_pool->enqueue(&RaftNode::send_append_entries, this, node.node_id, res);
                }
            }
        }
    }

    template<typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::persist() {
        auto block_num = log_storage->Persist();
        PersistData nodePersist;
        nodePersist.term = current_term;
        nodePersist.votedFor = votedFor;
        nodePersist.blockNum = block_num;
        std::vector<u8> buffer(bm->block_size());
        *(PersistData *) buffer.data() = nodePersist;
        bm->write_block(0, buffer.data());

    }

    template<typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::recover() {
        std::vector<u8> buffer(bm->block_size());
        bm->read_block(0, buffer.data());
        auto data = *(PersistData *) (buffer.data());
        current_term = data.term;
        votedFor = data.votedFor;
        if (data.blockNum == 0) {
            log_storage->appendEntry({0, 0});
        } else {
            log_storage->Recover(data.blockNum);
        }
    }

/******************************************************************

                          Test Functions (must not edit)

*******************************************************************/

    template<typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::set_network(std::map<int, bool> &network_availability) {
        std::unique_lock<std::mutex> clients_lock(clients_mtx);

        /* turn off network */
        if (!network_availability[my_id]) {
            for (auto &&client: rpc_clients_map) {
                if (client.second != nullptr) client.second.reset();
            }
            return;
        }

        for (auto node_network: network_availability) {
            int node_id = node_network.first;
            bool node_status = node_network.second;
            if (node_status && rpc_clients_map[node_id] == nullptr) {
                RaftNodeConfig target_config;
                for (auto config: node_configs) {
                    if (config.node_id == node_id) target_config = config;
                }
                rpc_clients_map[node_id] = std::make_unique<RpcClient>(target_config.ip_address, target_config.port,
                                                                       true);
            }
            if (!node_status && rpc_clients_map[node_id] != nullptr) {
                rpc_clients_map[node_id].reset();
            }
        }
    }

    template<typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::set_reliable(bool flag) {
        std::unique_lock<std::mutex> clients_lock(clients_mtx);
        for (auto &&client: rpc_clients_map) {
            if (client.second) {
                client.second->set_reliable(flag);
            }
        }
    }

    template<typename StateMachine, typename Command>
    int RaftNode<StateMachine, Command>::get_list_state_log_num() {
        /* only applied to ListStateMachine*/
        std::unique_lock<std::mutex> lock(mtx);

        return state->num_append_logs;
    }

    template<typename StateMachine, typename Command>
    int RaftNode<StateMachine, Command>::rpc_count() {
        int sum = 0;
        std::unique_lock<std::mutex> clients_lock(clients_mtx);

        for (auto &&client: rpc_clients_map) {
            if (client.second) {
                sum += client.second->count();
            }
        }

        return sum;
    }

    template<typename StateMachine, typename Command>
    std::vector<u8> RaftNode<StateMachine, Command>::get_snapshot_direct() {
        if (is_stopped()) {
            return std::vector<u8>{};
        }

        std::unique_lock<std::mutex> lock(mtx);

        return state->snapshot();
    }

}  // namespace chfs