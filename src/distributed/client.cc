#include "distributed/client.h"
#include "common/macros.h"
#include "common/util.h"
#include "distributed/metadata_server.h"

namespace chfs {

    ChfsClient::ChfsClient() : num_data_servers(0) {}

    auto ChfsClient::reg_server(ServerType type, const std::string &address,
                                u16 port, bool reliable) -> ChfsNullResult {
        switch (type) {
            case ServerType::DATA_SERVER:
                num_data_servers += 1;
                data_servers_.insert({num_data_servers, std::make_shared<RpcClient>(
                        address, port, reliable)});
                break;
            case ServerType::METADATA_SERVER:
                metadata_server_ = std::make_shared<RpcClient>(address, port, reliable);
                break;
            default:
                std::cerr << "Unknown Type" << std::endl;
                exit(1);
        }

        return KNullOk;
    }

// {Your code here}
    /**
      * Some Filesystem operations for client.
      *
      * @param type: The type of the file to be created.
      * @param parent: The parent directory of the node to be created.
      * @param name: The name of the node to be created.
      *
      * @return: The inode number of the new created node.
      */
    auto ChfsClient::mknode(FileType type, inode_id_t parent,
                            const std::string &name) -> ChfsResult<inode_id_t> {
        // TODO: Implement this function.
//  UNIMPLEMENTED();
        auto response = metadata_server_->call("mknode", (u8) type, parent, name);
        if (response.is_err()) {
            return ChfsResult<inode_id_t>{response.unwrap_error()};
        }
        auto inodeId = response.unwrap()->as<inode_id_t>();
        if (inodeId == KInvalidInodeID) {
            return ChfsResult<inode_id_t>{ErrorType::AlreadyExist};
        }
        //     std::cout<<"ChfsClient::mknode里的inode为："<<inodeId<<std::endl;
        return ChfsResult<inode_id_t>(inodeId);
    }

// {Your code here}
    auto ChfsClient::unlink(inode_id_t parent, std::string const &name)
    -> ChfsNullResult {
        // TODO: Implement this function.
//        UNIMPLEMENTED();
        auto response = metadata_server_->call("unlink", parent, name);
        if (response.is_err()) {
            return ChfsNullResult{response.unwrap_error()};
        }
        auto res = response.unwrap()->as<bool>();
        if (!res) {
            return ChfsNullResult{response.unwrap_error()};
        }
        return KNullOk;
    }

// {Your code here}
    auto ChfsClient::lookup(inode_id_t parent, const std::string &name)
    -> ChfsResult<inode_id_t> {
        // TODO: Implement this function.
//        UNIMPLEMENTED();
        auto response = metadata_server_->call("lookup", parent, name);
        if (response.is_err()) {
            return ChfsResult<inode_id_t>{response.unwrap_error()};
        }
        auto res = response.unwrap()->as<inode_id_t>();

        return ChfsResult<inode_id_t>(res);
    }

// {Your code here}
    auto ChfsClient::readdir(inode_id_t id)
    -> ChfsResult<std::vector<std::pair<std::string, inode_id_t>>> {
        // TODO: Implement this function.
//        UNIMPLEMENTED();
        auto response = metadata_server_->call("readdir", id);
        if (response.is_err()) {
            return ChfsResult<std::vector<std::pair<std::string, inode_id_t>>>{response.unwrap_error()};
        }
        auto res = response.unwrap()->as<std::vector<std::pair<std::string, inode_id_t>>>();
        return ChfsResult<std::vector<std::pair<std::string, inode_id_t>>>({res});
    }

// {Your code here}
    auto ChfsClient::get_type_attr(inode_id_t id)
    -> ChfsResult<std::pair<InodeType, FileAttr>> {
        // TODO: Implement this function.
//        UNIMPLEMENTED();
        auto response = metadata_server_->call("get_type_attr", id);
        if (response.is_err()) {
            return ChfsResult<std::pair<InodeType, FileAttr>>{response.unwrap_error()};
        }
        auto res = response.unwrap()->as<std::tuple<u64, u64, u64, u64, u8>>();
        auto inodeType = (InodeType) std::get<4>(res);
        if (inodeType == InodeType::Directory || inodeType == InodeType::Unknown) {
            return ChfsResult<std::pair<InodeType, FileAttr>>({(InodeType) std::get<4>(res),
                                                               FileAttr{std::get<1>(res), std::get<2>(res),
                                                                        std::get<3>(res), std::get<0>(res)}});
        } else if (inodeType == InodeType::FILE) {
            //需要计算的
            auto blockMap = metadata_server_->call("get_block_map", id).unwrap()->as<std::vector<BlockInfo>>();
            auto realSize = DiskBlockSize * blockMap.size();
            return ChfsResult<std::pair<InodeType, FileAttr>>({(InodeType) std::get<4>(res),
                                                               FileAttr{std::get<1>(res), std::get<2>(res),
                                                                        std::get<3>(res), realSize}});

        }
        return ChfsResult<std::pair<InodeType, FileAttr>>({(InodeType) std::get<4>(res),
                                                           FileAttr{std::get<1>(res), std::get<2>(res),
                                                                    std::get<3>(res), std::get<0>(res)}});

    }

/**
 * Read and Write operations are more complicated.
 */
// {Your code here}
    auto ChfsClient::read_file(inode_id_t id, usize offset, usize size)
    -> ChfsResult<std::vector<u8>> {
        // TODO: Implement this function.
//        UNIMPLEMENTED();
        auto response = metadata_server_->call("get_block_map", id);
        if (response.is_err()) {
            return ChfsResult<std::vector<u8>>{response.unwrap_error()};
        }
        auto blockMaps = response.unwrap()->as<std::vector<BlockInfo>>();

        auto res = std::vector<u8>(size);
        usize hasReadSize = 0;

        for (auto [blockId, macId, version]: blockMaps) {
            if (offset > DiskBlockSize) {
                offset -= DiskBlockSize;
                continue;
            }

            usize readBlockSize = std::min(size, DiskBlockSize - offset);
            auto dataResponse = data_servers_[macId]->call("read_data", blockId, offset, readBlockSize, version);

            if (dataResponse.is_err()) {
                return ChfsResult<std::vector<u8>>{dataResponse.unwrap_error()};
            }
            auto data = dataResponse.unwrap()->as<std::vector<u8>>();
            std::move(data.begin(), data.end(), res.begin() + hasReadSize);
            size -= readBlockSize;
            hasReadSize += readBlockSize;
            offset = 0;
            if (size == 0) {
                break;
            }
        }

        return ChfsResult<std::vector<u8>>{res};
//        return ChfsResult<std::vector<u8>>({});
    }

// {Your code here}
    auto ChfsClient::write_file(inode_id_t id, usize offset, std::vector<u8> data)
    -> ChfsNullResult {
        // TODO: Implement this function.
//        UNIMPLEMENTED();
        auto response = metadata_server_->call("get_block_map", id);
        if (response.is_err()) {
            return ChfsNullResult{response.unwrap_error()};
        }
        auto blockMaps = response.unwrap()->as<std::vector<BlockInfo>>();
//        std::cout<<"size :"<<blockMaps.size()<<std::endl;

        usize writtenLen = 0;
        for (auto blockMap: blockMaps) {
            if (writtenLen>=data.size()) {
                return KNullOk;
            }
            if (offset > DiskBlockSize) {
                offset -= DiskBlockSize;
                continue;
            }
            //在当前块写入
            block_id_t blockId = std::get<0>(blockMap);
            mac_id_t macId = std::get<1>(blockMap);
            usize writeBlockSize = ((data.size()-writtenLen) > (DiskBlockSize - offset)) ? (DiskBlockSize - offset) : (data.size()-writtenLen);
            auto neededToBeWrittenData = std::vector<u8>(data.begin()+writtenLen, data.begin() +writtenLen+ writeBlockSize);
            auto dataResponse = data_servers_[macId]->call("write_data", blockId, offset, neededToBeWrittenData);

            if (dataResponse.is_err()) {
                return ChfsNullResult{dataResponse.unwrap_error()};
            }
            writtenLen+=writeBlockSize;
            offset = 0;
        }

        while (writtenLen<data.size()) {
            auto alloc_call = metadata_server_->call("alloc_block", id);
            if (alloc_call.is_err()) {
                return ChfsNullResult{alloc_call.unwrap_error()};
            }
            if (offset > DiskBlockSize) {
                offset -= DiskBlockSize;
                continue;
            }
            auto [blockId, macId, version] = alloc_call.unwrap()->as<BlockInfo>();

            usize writeBlockSizeRemain = ((data.size()-writtenLen) > (DiskBlockSize - offset)) ? (DiskBlockSize - offset) : (data.size()-writtenLen);
            auto neededToBeWrittenDataRemain = std::vector<u8>(data.begin()+writtenLen, data.begin()+writtenLen + writeBlockSizeRemain);
            auto dataResponse2 = data_servers_[macId]->call("write_data", blockId, offset, neededToBeWrittenDataRemain);

            if (dataResponse2.is_err()) {
                return ChfsNullResult{dataResponse2.unwrap_error()};
            }
            writtenLen+=writeBlockSizeRemain;
            offset = 0;
        }

        return KNullOk;
//        return KNullOk;
    }

// {Your code here}
    auto ChfsClient::free_file_block(inode_id_t id, block_id_t block_id,
                                     mac_id_t mac_id) -> ChfsNullResult {
        // TODO: Implement this function.
//        UNIMPLEMENTED();

        auto response = metadata_server_->call("free_block", id, block_id, mac_id);
        if (response.is_err()) {
            return ChfsNullResult{response.unwrap_error()};
        }
        return KNullOk;
    }

} // namespace chfs