#include "distributed/metadata_server.h"
#include "common/util.h"
#include "filesystem/directory_op.h"
#include <fstream>

namespace chfs {

    inline auto MetadataServer::bind_handlers() {
        server_->bind("mknode",
                      [this](u8 type, inode_id_t parent, std::string const &name) {
                          return this->mknode(type, parent, name);
                      });
        server_->bind("unlink", [this](inode_id_t parent, std::string const &name) {
            return this->unlink(parent, name);
        });
        server_->bind("lookup", [this](inode_id_t parent, std::string const &name) {
            return this->lookup(parent, name);
        });
        server_->bind("get_block_map",
                      [this](inode_id_t id) { return this->get_block_map(id); });
        server_->bind("alloc_block",
                      [this](inode_id_t id) { return this->allocate_block(id); });
        server_->bind("free_block",
                      [this](inode_id_t id, block_id_t block, mac_id_t machine_id) {
                          return this->free_block(id, block, machine_id);
                      });
        server_->bind("readdir", [this](inode_id_t id) { return this->readdir(id); });
        server_->bind("get_type_attr",
                      [this](inode_id_t id) { return this->get_type_attr(id); });
    }

    inline auto MetadataServer::init_fs(const std::string &data_path) {
        /**
         * Check whether the metadata exists or not.
         * If exists, we wouldn't create one from scratch.
         */
        bool is_initialed = is_file_exist(data_path);

        auto block_manager = std::shared_ptr<BlockManager>(nullptr);
        if (is_log_enabled_) {
            block_manager =
                    std::make_shared<BlockManager>(data_path, KDefaultBlockCnt, true);
        } else {
            block_manager = std::make_shared<BlockManager>(data_path, KDefaultBlockCnt);
        }

        CHFS_ASSERT(block_manager != nullptr, "Cannot create block manager.");

        if (is_initialed) {
            auto origin_res = FileOperation::create_from_raw(block_manager);
            std::cout << "Restarting..." << std::endl;
            if (origin_res.is_err()) {
                std::cerr << "Original FS is bad, please remove files manually."
                          << std::endl;
                exit(1);
            }

            operation_ = origin_res.unwrap();
        } else {
            operation_ = std::make_shared<FileOperation>(block_manager,
                                                         DistributedMaxInodeSupported);
            std::cout << "We should init one new FS..." << std::endl;
            /**
             * If the filesystem on metadata server is not initialized, create
             * a root directory.
             */
            auto init_res = operation_->alloc_inode(InodeType::Directory);
            if (init_res.is_err()) {
                std::cerr << "Cannot allocate inode for root directory." << std::endl;
                exit(1);
            }

            CHFS_ASSERT(init_res.unwrap() == 1, "Bad initialization on root dir.");
        }

        running = false;
        num_data_servers =0; // Default no data server. Need to call `reg_server` to add.

        if (is_log_enabled_) {
            if (may_failed_)
                operation_->block_manager_->set_may_fail(true);
            commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                                     is_checkpoint_enabled_);
        }

        bind_handlers();

        /**
         * The metadata server wouldn't start immediately after construction.
         * It should be launched after all the data servers are registered.
         */
    }

    MetadataServer::MetadataServer(u16 port, const std::string &data_path,
                                   bool is_log_enabled, bool is_checkpoint_enabled,
                                   bool may_failed)
            : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
              is_checkpoint_enabled_(is_checkpoint_enabled) {
        server_ = std::make_unique<RpcServer>(port);
        init_fs(data_path);
        if (is_log_enabled_) {
            commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                                     is_checkpoint_enabled);
        }
    }

    MetadataServer::MetadataServer(std::string const &address, u16 port,
                                   const std::string &data_path,
                                   bool is_log_enabled, bool is_checkpoint_enabled,
                                   bool may_failed)
            : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
              is_checkpoint_enabled_(is_checkpoint_enabled) {
        server_ = std::make_unique<RpcServer>(address, port);
        init_fs(data_path);
        if (is_log_enabled_) {
            commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                                     is_checkpoint_enabled);
        }
    }

// {Your code here}
    /**
     * A RPC handler for client. It create a regular file or directory on metadata
     * server.
     *
     * @param type: Create a dir or regular file
     * @param parent: The parent directory of the node to be created.
     * @param name: The name of the node to be created.
     *
     * Using `const std::string&` rather than `const char *`. see
     * https://github.com/rpclib/rpclib/issues/280.
     *
     * Also note that we don't use an enum for `type` since it needs extra
     * support by `msgpack` for serialization.
     */
    auto MetadataServer::mknode(u8 type, inode_id_t parent, const std::string &name)
    -> inode_id_t {
        // TODO: Implement this function.
//  UNIMPLEMENTED();
        std::scoped_lock<std::shared_mutex> lock(mutexLock);
        auto inodeId_wrapper = operation_->mk_helper(parent, name.c_str(), InodeType(type));
        if (commit_log != nullptr) {
            //使用log写记录
            commit_log->append_log(CommitLog::currTxnId,CommitLog::ops_);
            commit_log->commit_log(CommitLog::currTxnId);
            commit_log->checkpoint();
            CommitLog::currTxnId+=1;
            if(CommitLog::txnFail){
                CommitLog::txnFail = false;
                return KInvalidInodeID;
            }
        }
        //实际操作
        if (inodeId_wrapper.is_err()) {
            return KInvalidInodeID;
        } else {
//            std::cout<<"MetadataServer::mknode里的inode为："<<inodeId_wrapper.unwrap()<<std::endl;
            return inodeId_wrapper.unwrap();
        }
    }

// {Your code here}
    auto MetadataServer::unlink(inode_id_t parent, const std::string &name)
    -> bool {
        // TODO: Implement this function.
//        UNIMPLEMENTED();
        std::scoped_lock<std::shared_mutex> lock(mutexLock);
        if (commit_log != nullptr) {
            //使用log写记录
            commit_log->append_log(CommitLog::currTxnId,CommitLog::ops_);
            commit_log->commit_log(CommitLog::currTxnId);
            commit_log->checkpoint();
            CommitLog::currTxnId+=1;
            if(CommitLog::txnFail){
                CommitLog::txnFail = false;
                return KInvalidInodeID;
            }
        }
        auto res = operation_->unlink(parent, name.c_str());

        if (res.is_err())
            return false;
        return true;
    }

// {Your code here}
    auto MetadataServer::lookup(inode_id_t parent, const std::string &name)
    -> inode_id_t {
        // TODO: Implement this function.
//        UNIMPLEMENTED();
        std::scoped_lock<std::shared_mutex> lock(mutexLock);
        auto res = operation_->lookup(parent, name.c_str());
        if (commit_log != nullptr) {
            //使用log写记录
//            commit_log->append_log(,)
        }
        if (res.is_err())
            return KInvalidInodeID;
        return res.unwrap();
    }

// {Your code here}
    /**
     * A RPC handler for client. It returns client a list about the position of
     * each block in a file.
     *
     * @param id: The inode id of the file.
     */
    auto MetadataServer::get_block_map(inode_id_t id) -> std::vector<BlockInfo> {
        // TODO: Implement this function.
//        UNIMPLEMENTED();
        std::vector<u8> buffer;

        {
            std::scoped_lock<std::shared_mutex> lock(mutexLock);
            auto blockContent = operation_->read_file(id);
            if (blockContent.is_err()) {
                return {};
            }
            buffer = blockContent.unwrap();
        }
//        auto blockContent = operation_->read_file(id);

        u32 num = buffer.size() * sizeof(u8) / sizeof(BlockInfo);
        std::vector<BlockInfo> res;
        for (u32 i = 0; i < num; i++) {
            BlockInfo info = *((BlockInfo *) buffer.data() + i);
            if (std::get<0>(info) != KInvalidBlockID) {
                res.push_back(info);
            }
        }
        return res;
    }

// {Your code here}
    auto MetadataServer::allocate_block(inode_id_t id) -> BlockInfo {
        // TODO: Implement this function.
//        UNIMPLEMENTED();

        auto macId = generator.rand(1, num_data_servers);
        auto callResponse = ChfsResult<std::shared_ptr<RpcResponse>>{std::make_shared<RpcResponse>()};
        {
            std::scoped_lock<std::shared_mutex> lock(serverMtx[macId-1]);
            callResponse = clients_[macId]->call("alloc_block");
        }
        auto res = callResponse.unwrap()->as<std::pair<block_id_t, version_t>>();
        BlockInfo info = {res.first, macId, res.second};

        //写回metadata
        {
            std::scoped_lock<std::shared_mutex> lock(mutexLock);
            auto metadataBlockInfo = operation_->read_file(id).unwrap();
            u32 cur = 0;
            u32 blockInfoNum = metadataBlockInfo.size() / (sizeof(BlockInfo) / sizeof(u8));
            for (; cur < blockInfoNum; cur++) {
                BlockInfo oldInfo = *((BlockInfo *) metadataBlockInfo.data() + cur);
                if (std::get<0>(oldInfo) == KInvalidBlockID && std::get<1>(oldInfo) == 0 && std::get<2>(oldInfo) == 0) {
                    //找到了一个空闲位置
                    *((BlockInfo *) metadataBlockInfo.data() + cur) = info;
                    break;
                }
            }
            //没空位
            if (cur == blockInfoNum) {
                std::vector<u8> buffer(sizeof(BlockInfo));
                *(BlockInfo *) (buffer.data()) = info;
                metadataBlockInfo.insert(metadataBlockInfo.end(), buffer.begin(), buffer.end());
            }
            operation_->write_file(id, metadataBlockInfo);
            return info;
        }
    }

// {Your code here}
    auto MetadataServer::free_block(inode_id_t id, block_id_t block_id,
                                    mac_id_t machine_id) -> bool {
        // TODO: Implement this function.
//        UNIMPLEMENTED();
        auto callResponse = ChfsResult<std::shared_ptr<RpcResponse>>{std::make_shared<RpcResponse>()};
        {
            std::scoped_lock<std::shared_mutex> lock(serverMtx[machine_id - 1]);
            callResponse = clients_[machine_id]->call("free_block", block_id);
        }
        auto res = callResponse.unwrap()->as<bool>();
        if (!res) {
            //释放失败
            return false;
        }
        //写回metadata
        {
            std::scoped_lock<std::shared_mutex> lock(mutexLock);
            auto metadataBlockInfo = operation_->read_file(id).unwrap();
            u32 blockInfoNum = metadataBlockInfo.size() / (sizeof(BlockInfo) / sizeof(u8));
            for (u32 i = 0; i < blockInfoNum; i++) {
                BlockInfo info = *((BlockInfo *) metadataBlockInfo.data() + i);
                if (std::get<0>(info) == block_id && std::get<1>(info) == machine_id) {
                    *((BlockInfo *) metadataBlockInfo.data() + i) = BlockInfo(0, 0, 0);
                    break;
                }
            }
            operation_->write_file(id, metadataBlockInfo);
            return true;
        }
    }

// {Your code here}
    auto MetadataServer::readdir(inode_id_t node)
    -> std::vector<std::pair<std::string, inode_id_t>> {
        // TODO: Implement this function.
//        UNIMPLEMENTED();
        std::scoped_lock<std::shared_mutex> lock(mutexLock);
        std::vector<std::pair<std::string, inode_id_t>> res;
        std::list<DirectoryEntry> DList;
        read_directory(operation_.get(), node, DList);
        for (auto &i: DList) {
            res.emplace_back(i.name, i.id);
        }
        return res;
    }

// {Your code here}
    /**
      * A RPC handler for client. It returns the type and attribute of a file
      *
      * @param id: The inode id of the file
      *
      * @return: a tuple of <size, atime, mtime, ctime, type>
      */
    auto MetadataServer::get_type_attr(inode_id_t id)
    -> std::tuple<u64, u64, u64, u64, u8> {
        // TODO: Implement this function.
//        UNIMPLEMENTED();
//        std::tuple<u64, u64, u64, u64, u8> ans;
        std::scoped_lock<std::shared_mutex> lock(mutexLock);
        auto res = operation_->get_type_attr(id).unwrap();
        auto type = res.first;
        auto fileAttr = res.second;
        auto atime = fileAttr.atime;
        auto mtime = fileAttr.mtime;
        auto ctime = fileAttr.ctime;
        auto fSize = fileAttr.size;

        return {fSize, atime, mtime, ctime, (u8) type};
    }

    auto MetadataServer::reg_server(const std::string &address, u16 port,
                                    bool reliable) -> bool {
        num_data_servers += 1;
        auto cli = std::make_shared<RpcClient>(address, port, reliable);
        clients_.insert(std::make_pair(num_data_servers, cli));

        return true;
    }

    auto MetadataServer::run() -> bool {
        if (running)
            return false;
        serverMtx = std::vector<std::shared_mutex>(num_data_servers);
        // Currently we only support async start
        server_->run(true, num_worker_threads);
        running = true;
        return true;
    }

} // namespace chfs