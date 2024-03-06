#pragma once

#include "rpc/msgpack.hpp"
#include "rsm/raft/log.h"

namespace chfs {

    const std::string RAFT_RPC_START_NODE = "start node";
    const std::string RAFT_RPC_STOP_NODE = "stop node";
    const std::string RAFT_RPC_NEW_COMMEND = "new commend";
    const std::string RAFT_RPC_CHECK_LEADER = "check leader";
    const std::string RAFT_RPC_IS_STOPPED = "check stopped";
    const std::string RAFT_RPC_SAVE_SNAPSHOT = "save snapshot";
    const std::string RAFT_RPC_GET_SNAPSHOT = "get snapshot";

    const std::string RAFT_RPC_REQUEST_VOTE = "request vote";
    const std::string RAFT_RPC_APPEND_ENTRY = "append entries";
    const std::string RAFT_RPC_INSTALL_SNAPSHOT = "install snapshot";

    struct RequestVoteArgs {
        /* Lab3: Your code here */
        int term;//candidate’s term
        int candidateId;// candidate requesting vote
        int lastLogIndex;// index of candidate’s last log entry (§5.4)
        int lastLogTerm;// term of candidate’s last log entry
        MSGPACK_DEFINE(term,candidateId,lastLogIndex,lastLogTerm);
    };

    struct RequestVoteReply {
        /* Lab3: Your code here */
        int term;//currentTerm, for candidate to update itself
        bool voteGranted;//true means candidate received vote
        MSGPACK_DEFINE(term,voteGranted);
    };

    template <typename Command>
    struct AppendEntriesArgs {
        /* Lab3: Your code here */
        int term;
        int leaderId;
        int prevLogIndex;
        int prevLogTerm;
        int leaderCommit;
        bool heartBeat;
        int lastIncludeIndex;
        std::vector<Entry<Command>> entries;
    };

    struct RpcAppendEntriesArgs {
        /* Lab3: Your code here */
        std::string data;
        MSGPACK_DEFINE(data)
    };

    template <typename Command>
    RpcAppendEntriesArgs transform_append_entries_args(const AppendEntriesArgs<Command> &arg) {
        /* Lab3: Your code here */
        std::stringstream ss;
        ss << arg.term << ' ';
        ss << arg.leaderId << ' ';
        ss << arg.prevLogIndex << ' ';
        ss << arg.prevLogTerm << ' ';
        ss << arg.leaderCommit << ' ';
        ss << arg.heartBeat << ' ';
        ss << arg.lastIncludeIndex << ' ';
        ss << arg.entries.size() << ' ';
        for (const Entry<Command> &entry : arg.entries) {
            ss << entry.term << ' '<<entry.command.value<<' ';
        }
        return RpcAppendEntriesArgs{ss.str()};
    }

    template <typename Command>
    AppendEntriesArgs<Command> transform_rpc_append_entries_args(const RpcAppendEntriesArgs &rpc_arg) {
        /* Lab3: Your code here */
        int term;
        int leaderId;
        int prevLogIndex;
        int prevLogTerm;
        int leaderCommit;
        bool heartBeat;
        int lastIncludeIndex;
        int entrySize;
        int tmp_term;
        int value;
        std::vector<Entry<Command>> entries;
        auto str = rpc_arg.data;
        std::stringstream ss(str);
        ss >> term >> leaderId >> prevLogIndex >> prevLogTerm >> leaderCommit >> heartBeat >> lastIncludeIndex >>
           entrySize;
        for (int i = 0; i < entrySize; ++i) {
            ss >> tmp_term >> value;
            Entry<Command>entry;
            entry.term = tmp_term;
            entry.command.value = value;
            entries.emplace_back(entry);
        }
        return {term, leaderId, prevLogIndex, prevLogTerm, leaderCommit, heartBeat, lastIncludeIndex, entries};
    }

    struct AppendEntriesReply {
        /* Lab3: Your code here */
        int term;
        bool success;
        MSGPACK_DEFINE(term, success)
    };

    struct InstallSnapshotArgs {
        /* Lab3: Your code here */
        int term;// leader’s term
        int leaderId;// so follower can redirect clients
        int lastIncludedIndex;// the snapshot replaces all entries up through and including this index
        int lastIncludedTerm;// term of lastIncludedIndex
        int offset;// byte offset where chunk is positioned in the snapshot file
        std::vector<u8> data;// raw bytes of the snapshot chunk, starting at offset
        bool done;// true if this is the last chunk
        MSGPACK_DEFINE(term, leaderId, lastIncludedIndex, lastIncludedTerm, offset, data, done);
    };

    struct InstallSnapshotReply {
        /* Lab3: Your code here */
        int term;
        MSGPACK_DEFINE(term);
    };

} /* namespace chfs */