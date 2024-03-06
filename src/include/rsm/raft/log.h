#pragma once

#include <cstring>
#include <mutex>
#include <sstream>
#include <utility>
#include <vector>
#include "block/manager.h"
#include "common/macros.h"

namespace chfs {

/**
 * RaftLog uses a BlockManager to manage the data..
 */
    template<typename Command>
    struct Entry {
        int term;
        Command command;
    };

    template<typename Command>
    class RaftLog {
    public:
        explicit RaftLog(std::shared_ptr<BlockManager> bm);

        ~RaftLog();

        /* Lab3: Your code here */
        auto getSize() -> int {
            std::scoped_lock<std::mutex> lock(mtx);
            return logData.size();
        }

        void appendEntry(Entry<Command> entry) {
            std::scoped_lock<std::mutex> lock(mtx);
            logData.push_back(entry);
        }

        void insert(int index, Entry<Command> entry) {
            std::scoped_lock<std::mutex> lock(mtx);
            if (index < 0) {
                return;
            }
            if (index >= logData.size()) {
                std::vector<Entry<Command>> new_data{index - logData.size() + 1};
                logData.insert(logData.end(), new_data.begin(), new_data.end());
            }
            logData[index] = entry;
        }

//        auto Back() -> Entry<Command> {
//            std::scoped_lock<std::mutex> lock(mtx);
//            return logData.back();
//        }

        Entry<Command> getLogEntry(int index) {
            std::scoped_lock<std::mutex> lock(mtx);
            if (index >= logData.size()) {
                for (int i = 0; i < index - logData.size() + 1; ++i) {
                    logData.emplace_back(Entry<Command>{0, 0});
                }
            }
            return logData[index];
        }

        auto deleteAfterI(int index) -> void {
            std::scoped_lock<std::mutex> lock(mtx);
            if (index >= logData.size()) {
                return;
            }
            logData.erase(logData.begin() + index, logData.end());
        }

        std::vector<Entry<Command>> getAfterI(int index) {
            std::scoped_lock<std::mutex> lock(mtx);
            if (logData.size() <= index) {
                return std::vector<Entry<Command>>{};
            }
            return {logData.begin() + index + 1, logData.end()};
        }

        void Recover(int block_num) {
            std::scoped_lock<std::mutex> lock(mtx);
            logData.clear();
            std::vector<u8> buffer(bm_->block_size());
            bool first = true;
            for (int i = 1; i <= block_num; ++i) {
                bm_->read_block(i, buffer.data());
                int len = bm_->block_size() / sizeof(Entry<Command>);
                for (int j = 0; j < len; ++j) {
                    Entry<Command> entry = *((Entry<Command> *) buffer.data() + j);
                    if (entry.term == 0 && entry.command.value == 0) {
                        if (!first) {
                            break;
                        }
                        first = false;
                    }
                    logData.emplace_back(entry);
                }
            }
        }
        int Persist() {
            std::lock_guard<std::mutex> lock(mtx);
            int entryCntPerBlock = bm_->block_size() / sizeof(Entry<Command>);
            auto size = logData.size();
            int bIdx = 1;
            int sum = 0;
            std::vector<u8> buffer(bm_->block_size());
            for (int i = 0; i < size; ++i) {
                auto index = i % entryCntPerBlock;
                auto entry = logData[i];
                *((Entry<Command> *) buffer.data() + index) = entry;
                if (index == entryCntPerBlock - 1) {
                    bm_->write_block(bIdx, buffer.data());
                    ++bIdx;
                    // Reuse the buffer instead of clearing and resizing
                    buffer = std::vector<u8>(bm_->block_size());
                }
                ++sum;
            }
            // Write the remaining entries in the buffer
            if (!buffer.empty()) {
                bm_->write_block(bIdx, buffer.data());
            }
            return bIdx;
        }
        std::vector<u8> readSnapshot(){
            logData.clear();
            logData.insert(logData.begin(), logData.begin(), logData.end());
            return std::vector<u8>{};
        }

    private:
        std::shared_ptr<BlockManager> bm_;
        std::mutex mtx;
        std::vector<Entry<Command>> logData;
        /* Lab3: Your code here */
    };

    template<typename Command>
    RaftLog<Command>::RaftLog(std::shared_ptr<BlockManager> bm) : bm_(bm), mtx{}, logData{} {
        /* Lab3: Your code here */
    }

    template<typename Command>
    RaftLog<Command>::~RaftLog() {
        /* Lab3: Your code here */
    }

/* Lab3: Your code here */

} /* namespace chfs */
