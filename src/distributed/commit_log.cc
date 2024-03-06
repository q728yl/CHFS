#include <algorithm>

#include <chrono>
#include <utility>
#include "common/bitmap.h"
#include "distributed/commit_log.h"
#include "distributed/metadata_server.h"
#include "filesystem/directory_op.h"
#include "metadata/inode.h"

namespace chfs {
/**
 * `CommitLog` part
 */
// {Your code here}
    CommitLog::CommitLog(std::shared_ptr<BlockManager> bm, bool is_checkpoint_enabled)
            : is_checkpoint_enabled_(is_checkpoint_enabled), bm_(bm) {}

    CommitLog::~CommitLog() {}

// {Your code here}
    auto CommitLog::get_log_entry_num() -> usize {
        return txs.size();
    }

// {Your code here}
    auto CommitLog::append_log(txn_id_t txn_id, std::vector<std::shared_ptr<BlockOperation>> ops) -> void {
        //只要一个block存bitmap
        auto logBitmapBlockId = bm_->total_blocks();
        auto buffer = std::vector<u8>(DiskBlockSize);
        bm_->read_block(logBitmapBlockId, buffer.data());
        auto logBitmap = Bitmap(buffer.data(), DiskBlockSize);
        for (auto opr: ops) {
            auto logIndex = logBitmap.find_first_free_w_bound(1024).value();
            logBitmap.set(logIndex);
            auto logBlockId = logBitmapBlockId + 1 + logIndex;
            txs[txn_id].emplace_back(opr->block_id_, logBlockId);
            bm_->write_block(logBlockId, opr->new_block_state_.data());
        }
        bm_->write_block(logBitmapBlockId, buffer.data());
        txnDone[txn_id] = false;
    }

// {Your code here}
    auto CommitLog::commit_log(txn_id_t txn_id) -> void {
        //标记为已完成
        txnDone[txn_id] = true;
    }

// {Your code here}
    auto CommitLog::checkpoint() -> void {
        //写所有已提交的tx的操作
        auto logBitmapBlockId = bm_->total_blocks();
        auto bitmapBuffer = std::vector<u8>(DiskBlockSize);
        bm_->read_block(logBitmapBlockId, bitmapBuffer.data());
        auto logBitmap = Bitmap(bitmapBuffer.data(), DiskBlockSize);
        auto buffer = std::vector<u8>(DiskBlockSize);
        for (auto tx: txs) {
            if (txnDone[tx.first]== true) {
                //完成的擦除
                txs.erase(tx.first);
                for (auto [dataBlock, logBlock]: tx.second) {
                    logBitmap.clear(logBlock - logBitmapBlockId - 1);
                    bm_->read_block(logBlock, buffer.data());
                    bm_->write_block(dataBlock, buffer.data());
                }

            }
        }
        bm_->write_block(logBitmapBlockId, bitmapBuffer.data());
        //删掉
        ops_.clear();
    }

// {Your code here}
    auto CommitLog::recover() -> void {
        auto logBitmapBlockId = bm_->total_blocks();
        auto bitmapBuffer = std::vector<u8>(DiskBlockSize);
        bm_->read_block(logBitmapBlockId, bitmapBuffer.data());
        auto logBitmap = Bitmap(bitmapBuffer.data(), DiskBlockSize);
        auto buffer = std::vector<u8>(DiskBlockSize);
        for (auto tx: txs) {
            txs.erase(tx.first);
            for (auto [dataBlock, logBlock]: tx.second) {
                logBitmap.clear(logBlock - logBitmapBlockId - 1);
                bm_->read_block(logBlock, buffer.data());
                bm_->write_block(dataBlock, buffer.data());
            }
        }
        //改写logbitmap
        bm_->write_block(logBitmapBlockId, bitmapBuffer.data());
    }
};  // namespace chfs