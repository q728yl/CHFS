#include "distributed/dataserver.h"
#include "common/util.h"

namespace chfs {

    auto DataServer::initialize(std::string const &data_path) {
        /**
         * At first check whether the file exists or not.
         * If so, which means the distributed chfs has
         * already been initialized and can be rebuilt from
         * existing data.
         */
        bool is_initialized = is_file_exist(data_path);
        auto version_blocks_cnt = (KDefaultBlockCnt * sizeof(version_t)) / DiskBlockSize;
        auto bm = std::make_shared<BlockManager>(data_path, KDefaultBlockCnt);
        if (is_initialized) {
            block_allocator_ = std::make_shared<BlockAllocator>(bm, version_blocks_cnt, false);
        } else {
            // We need to reserve some blocks for storing the version of each block
            block_allocator_ = std::make_shared<BlockAllocator>(bm, version_blocks_cnt, true);
            for (block_id_t i = 0; i < version_blocks_cnt; i++) {
                bm->zero_block(i);
            }
        }

        // Initialize the RPC server and bind all handlers
        server_->bind("read_data", [this](block_id_t block_id, usize offset,
                                          usize len, version_t version) {
            return this->read_data(block_id, offset, len, version);
        });
        server_->bind("write_data", [this](block_id_t block_id, usize offset,
                                           std::vector<u8> &buffer) {
            return this->write_data(block_id, offset, buffer);
        });
        server_->bind("alloc_block", [this]() { return this->alloc_block(); });
        server_->bind("free_block", [this](block_id_t block_id) {
            return this->free_block(block_id);
        });

        // Launch the rpc server to listen for requests
        server_->run(true, num_worker_threads);
    }

    DataServer::DataServer(u16 port, const std::string &data_path)
            : server_(std::make_unique<RpcServer>(port)) {
        initialize(data_path);
    }

    DataServer::DataServer(std::string const &address, u16 port,
                           const std::string &data_path)
            : server_(std::make_unique<RpcServer>(address, port)) {
        initialize(data_path);
    }

    DataServer::~DataServer() { server_.reset(); }

// {Your code here}
    auto DataServer::read_data(block_id_t block_id, usize offset, usize len,
                               version_t version) -> std::vector<u8> {
        // TODO: Implement this function.
//        UNIMPLEMENTED();
        //version_block是映射data_block的
        auto versionBlockNum = DiskBlockSize / sizeof(version_t);
        auto buffer = std::vector<u8>(DiskBlockSize);
        block_id_t versionBlockBlockId = 0 + block_id / versionBlockNum;
        auto versionBlockOffset = block_id % versionBlockNum;
        this->block_allocator_->bm->read_block(versionBlockBlockId, buffer.data());
        int pian = sizeof(version_t) / sizeof(u8);
        version_t version1 = *(version_t *) (buffer.data() + versionBlockOffset * pian);
        if (version1 != version) {
            //版本不对
            return std::vector<u8>(0);
        }
        buffer.clear();
        //可以读
        block_allocator_->bm->read_block(block_id, buffer.data());
        auto v = std::vector<u8>(len);
        memcpy(v.data(), buffer.data() + offset, len);
        return v;
    }

// {Your code here}
    auto DataServer::write_data(block_id_t block_id, usize offset,
                                std::vector<u8> &buffer) -> bool {
        // TODO: Implement this function.
//        UNIMPLEMENTED();

        auto res = block_allocator_->bm->write_partial_block(block_id, buffer.data(), offset, buffer.size());
        if (res.is_ok()) {
            return true;
        } else {
            return false;
        }
    }

// {Your code here}
    auto DataServer::alloc_block() -> std::pair<block_id_t, version_t> {
        // TODO: Implement this function.
//        UNIMPLEMENTED();
        block_id_t blockId = block_allocator_->allocate().unwrap();
        auto versionBlockNum = DiskBlockSize / sizeof(version_t);
        auto buffer = std::vector<u8>(DiskBlockSize);
        block_id_t versionBlockBlockId = 0 + blockId / versionBlockNum;
        auto versionBlockOffset = blockId % versionBlockNum;
        this->block_allocator_->bm->read_block(versionBlockBlockId, buffer.data());
        int pian = sizeof(version_t) / sizeof(u8);
        version_t version = *(version_t *) (buffer.data() + versionBlockOffset * pian);
        version += 1;
        memcpy(buffer.data() + versionBlockOffset * pian, (version_t *) &(version), sizeof(version_t));
        block_allocator_->bm->write_block(versionBlockBlockId, buffer.data());
        return std::pair<block_id_t, version_t>{blockId, version};
    }

// {Your code here}
    auto DataServer::free_block(block_id_t block_id) -> bool {
        // TODO: Implement this function.
//        UNIMPLEMENTED();
        if(block_allocator_->deallocate(block_id).is_err()){
            return false;
        }
        auto versionBlockNum = DiskBlockSize / sizeof(version_t);
        auto buffer = std::vector<u8>(DiskBlockSize);
        block_id_t versionBlockBlockId = 0 + block_id / versionBlockNum;
        auto versionBlockOffset = block_id % versionBlockNum;
        this->block_allocator_->bm->read_block(versionBlockBlockId, buffer.data());
        int pian = sizeof(version_t) / sizeof(u8);
        version_t version = *(version_t *) (buffer.data() + versionBlockOffset * pian);
        version += 1;
        memcpy(buffer.data() + versionBlockOffset * pian, (version_t *) &(version), sizeof(version_t));
        block_allocator_->bm->write_block(versionBlockBlockId, buffer.data());

        return true;
    }
} // namespace chfs