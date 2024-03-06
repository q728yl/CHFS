#include <ctime>

#include "filesystem/operations.h"

namespace chfs {

// {Your code here}
    /**
      * Create a inode with a given type
      * It will allocate a block for the created inode
      *
      * @param type the type of the inode
      * @return the id of the inode
      */
    auto FileOperation::alloc_inode(InodeType type) -> ChfsResult<inode_id_t> {
        inode_id_t inode_id = static_cast<inode_id_t>(0);
        auto inode_res = ChfsResult<inode_id_t>(inode_id);

        // TODO:
        // 1. Allocate a block for the inode.
        // 2. Allocate an inode.
        // 3. Initialize the inode block
        //    and write the block back to block manager.
//  UNIMPLEMENTED();
        block_id_t blockId = this->block_allocator_->allocate().unwrap();//找到一个空快
        inode_res = this->inode_manager_->allocate_inode(type, blockId);
        //创造inode并写入
        Inode inode = Inode(type, this->block_manager_->block_size());
        this->block_manager_->write_partial_block(blockId, reinterpret_cast<const u8 *>(&(inode)), 0, sizeof(inode));

        return inode_res;
    }

    auto FileOperation::getattr(inode_id_t id) -> ChfsResult<FileAttr> {
        return this->inode_manager_->get_attr(id);
    }

    auto FileOperation::get_type_attr(inode_id_t id)
    -> ChfsResult<std::pair<InodeType, FileAttr>> {
        return this->inode_manager_->get_type_attr(id);
    }

    auto FileOperation::gettype(inode_id_t id) -> ChfsResult<InodeType> {
        return this->inode_manager_->get_type(id);
    }

    auto calculate_block_sz(u64 file_sz, u64 block_sz) -> u64 {
        return (file_sz % block_sz) ? (file_sz / block_sz + 1) : (file_sz / block_sz);
    }

    auto FileOperation::write_file_w_off(inode_id_t id, const char *data, u64 sz,
                                         u64 offset) -> ChfsResult<u64> {
        auto read_res = this->read_file(id);
        if (read_res.is_err()) {
            return ChfsResult<u64>(read_res.unwrap_error());
        }

        auto content = read_res.unwrap();
        if (offset + sz > content.size()) {
            content.resize(offset + sz);
        }
        memcpy(content.data() + offset, data, sz);

        auto write_res = this->write_file(id, content);
        if (write_res.is_err()) {
            return ChfsResult<u64>(write_res.unwrap_error());
        }
        return ChfsResult<u64>(sz);
    }
    /**
     * Write the content to the blocks pointed by the inode
     * If the inode's block is insufficient, we will dynamically allocate more
     * blocks
     *
     * @param id the id of the inode
     * @param content the content to write
     */
// {Your code here}
    auto FileOperation::write_file(inode_id_t id, const std::vector<u8> &content)
    -> ChfsNullResult {
        auto error_code = ErrorType::DONE;
        const auto block_size = this->block_manager_->block_size();
        usize old_block_num = 0;
        usize new_block_num = 0;
        u64 original_file_sz = 0;

        // 1. read the inode
        std::vector<u8> inode(block_size);//// 创建一个大小为块大小的向量来存储inode数据
        std::vector<u8> indirect_block(0);
        indirect_block.reserve(block_size);

        auto inode_p = reinterpret_cast<Inode *>(inode.data());
        auto inlined_blocks_num = 0;

        auto inode_res = this->inode_manager_->read_inode(id, inode);
        if (inode_res.is_err()) {
            error_code = inode_res.unwrap_error();
            // I know goto is bad, but we have no choice
            goto err_ret;
        } else {
            inlined_blocks_num = inode_p->get_direct_block_num();
        }

        if (content.size() > inode_p->max_file_sz_supported()) {
            std::cerr << "file size too large: " << content.size() << " vs. "
                      << inode_p->max_file_sz_supported() << std::endl;
            error_code = ErrorType::OUT_OF_RESOURCE;
            goto err_ret;
        }

        // 2. make sure whether we need to allocate more blocks
        original_file_sz = inode_p->get_size();
        old_block_num = calculate_block_sz(original_file_sz, block_size);
        new_block_num = calculate_block_sz(content.size(), block_size);

        if (new_block_num > old_block_num) {
            // If we need to allocate more blocks.
            for (usize idx = old_block_num; idx < new_block_num; ++idx) {

                // TODO: Implement the case of allocating more blocks.
                // 1. Allocate a block.
                // 2. Fill the allocated block id to the inode.
                //    You should pay attention to the case of indirect block.
                //    You may use function `get_or_insert_indirect_block`
                //    in the case of indirect block.
//      UNIMPLEMENTED();
//写入blockId
                if (inode_p->is_direct_block(idx)) {
                    block_id_t blockId = block_allocator_->allocate().unwrap();
                    inode_p->set_block_direct(idx, blockId);

                } else {
                    auto indirectBlockId = inode_p->get_or_insert_indirect_block(this->block_allocator_).unwrap();
                    //得到并写入间接块indirect_blockId
                    auto wei = (idx - inode_p->get_direct_block_num()) * sizeof(block_id_t);
                    block_id_t blockId = block_allocator_->allocate().unwrap();
                    std::vector<u8> buffer(block_size);
                    block_manager_->read_block(indirectBlockId, buffer.data());
                    memcpy(buffer.data() + wei, &blockId,
                           sizeof(block_id_t));
                    inode_p->write_indirect_block(block_manager_, buffer);
                }
            }

        } else {
            // We need to free the extra blocks.
            for (usize idx = new_block_num; idx < old_block_num; ++idx) {
                if (inode_p->is_direct_block(idx)) {

                    // TODO: Free the direct extra block.
//        UNIMPLEMENTED();
                    block_allocator_->deallocate(inode_p->operator[](idx));
                    std::cout<<"KInvalidBlockID: "<<KInvalidBlockID<<std::endl;
                    inode_p->set_block_direct(idx, KInvalidBlockID);//shifang

                } else {

                    // TODO: Free the indirect extra block.
//        UNIMPLEMENTED();
                    auto indirectId = inode_p->get_indirect_block_id();
//          if (indirectId.is_err()) {
//              error_code = indirectId.unwrap_error();  // 如果获取失败，将错误码设置为间接块操作错误类型
//              goto err_ret;
//          }
                    std::vector<u8> buffer(block_size);
                    block_manager_->read_block(indirectId, buffer.data());  // 读取间接块数据
                    block_id_t clearBlockId = 0;
                    auto wei = (idx - inode_p->get_direct_block_num()) * sizeof(block_id_t);
                    memcpy(&clearBlockId, buffer.data() + wei, sizeof(block_id_t));
                    block_allocator_->deallocate(clearBlockId);
                    memcpy(buffer.data() + wei, &KInvalidBlockID, sizeof(block_id_t));
                    block_manager_->write_block(indirectId, buffer.data());
                }
            }

            // If there are no more indirect blocks.
            if (old_block_num > inlined_blocks_num &&
                new_block_num <= inlined_blocks_num && true) {

                auto res =
                        this->block_allocator_->deallocate(inode_p->get_indirect_block_id());
                if (res.is_err()) {
                    error_code = res.unwrap_error();
                    goto err_ret;
                }
                indirect_block.clear();
                inode_p->invalid_indirect_block_id();
            }
        }

        // 3. write the contents
        inode_p->inner_attr.size = content.size();
        inode_p->inner_attr.mtime = time(0);

        {
            auto block_idx = 0;
            u64 write_sz = 0;

            while (write_sz < content.size()) {
                auto sz = ((content.size() - write_sz) > block_size)
                          ? block_size
                          : (content.size() - write_sz);
                std::vector<u8> buffer(block_size);
                memcpy(buffer.data(), content.data() + write_sz, sz);
                block_id_t ActualBlockId = 0;
                if (inode_p->is_direct_block(block_idx)) {

                    // TODO: Implement getting block id of current direct block.
//                    //UNIMPLEMENTED();
                    ActualBlockId = inode_p->operator[](block_idx);

                } else {

                    // TODO: Implement getting block id of current indirect block.
//                    UNIMPLEMENTED();
                    block_id_t blockId = inode_p->get_indirect_block_id();
                    std::vector<u8> indirectBuffer(block_size);
                    block_manager_->read_block(blockId, indirectBuffer.data());
                    auto wei = (block_idx - inode_p->get_direct_block_num()) * sizeof(block_id_t);
                    memcpy(&ActualBlockId,
                           indirectBuffer.data() + wei,
                           sizeof(block_id_t));

                }

                // TODO: Write to current block.
//                UNIMPLEMENTED();
                block_manager_->write_block(ActualBlockId, buffer.data());

                write_sz += sz;
                block_idx += 1;
            }
        }

        // finally, update the inode
        {
            inode_p->inner_attr.set_all_time(time(0));

            auto write_res =
                    this->block_manager_->write_block(inode_res.unwrap(), inode.data());
            if (write_res.is_err()) {
                error_code = write_res.unwrap_error();
                goto err_ret;
            }
            if (indirect_block.size() != 0) {
                write_res =
                        inode_p->write_indirect_block(this->block_manager_, indirect_block);
                if (write_res.is_err()) {
                    error_code = write_res.unwrap_error();
                    goto err_ret;
                }
            }
        }

        return KNullOk;

        err_ret:
        // std::cerr << "write file return error: " << (int)error_code << std::endl;
        return ChfsNullResult(error_code);
    }

// {Your code here}
    auto FileOperation::read_file(inode_id_t id) -> ChfsResult<std::vector<u8>> {
        auto error_code = ErrorType::DONE;
        std::vector<u8> content;

        const auto block_size = this->block_manager_->block_size();

        // 1. read the inode
        std::vector<u8> inode(block_size);
        std::vector<u8> indirect_block(0);
        indirect_block.reserve(block_size);

        auto inode_p = reinterpret_cast<Inode *>(inode.data());
        u64 file_sz = 0;
        u64 read_sz = 0;

        auto inode_res = this->inode_manager_->read_inode(id, inode);
        if (inode_res.is_err()) {
            error_code = inode_res.unwrap_error();
            // I know goto is bad, but we have no choice
            goto err_ret;
        }

        file_sz = inode_p->get_size();
        content.reserve(file_sz);

        // Now read the file
        while (read_sz < file_sz) {
            auto sz = ((inode_p->get_size() - read_sz) > block_size)
                      ? block_size
                      : (inode_p->get_size() - read_sz);
            std::vector<u8> buffer(block_size);

            // Get current block id.
            block_id_t  ActualBlockId = 0;
            if (inode_p->is_direct_block(read_sz / block_size)) {
                // TODO: Implement the case of direct block.
//                UNIMPLEMENTED();
                auto bId = read_sz / block_size;
                ActualBlockId = inode_p->operator[](bId);
            } else {
                // TODO: Implement the case of indirect block.
//                UNIMPLEMENTED();
                auto bId = read_sz / block_size;
                block_id_t blockId = inode_p->get_indirect_block_id();
                std::vector<u8> indirectBuffer(block_size);
                block_manager_->read_block(blockId, indirectBuffer.data());
                auto wei = (bId - inode_p->get_direct_block_num()) * sizeof(block_id_t);
                memcpy(&ActualBlockId,
                       indirectBuffer.data() + wei,
                       sizeof(block_id_t));
            }

            // TODO: Read from current block and store to `content`.
//            UNIMPLEMENTED();
            block_manager_->read_block(ActualBlockId, buffer.data());
            //每次一块的数据写入content，拼接而成
            content.insert(content.end(), buffer.begin(), buffer.begin() + sz);

            read_sz += sz;
        }

        return ChfsResult<std::vector<u8>>(std::move(content));

        err_ret:
        return ChfsResult<std::vector<u8>>(error_code);
    }

    auto FileOperation::read_file_w_off(inode_id_t id, u64 sz, u64 offset)
    -> ChfsResult<std::vector<u8>> {
        auto res = read_file(id);
        if (res.is_err()) {
            return res;
        }

        auto content = res.unwrap();
        return ChfsResult<std::vector<u8>>(
                std::vector<u8>(content.begin() + offset, content.begin() + offset + sz));
    }

    auto FileOperation::resize(inode_id_t id, u64 sz) -> ChfsResult<FileAttr> {
        auto attr_res = this->getattr(id);
        if (attr_res.is_err()) {
            return ChfsResult<FileAttr>(attr_res.unwrap_error());
        }

        auto attr = attr_res.unwrap();
        auto file_content = this->read_file(id);
        if (file_content.is_err()) {
            return ChfsResult<FileAttr>(file_content.unwrap_error());
        }

        auto content = file_content.unwrap();

        if (content.size() != sz) {
            content.resize(sz);

            auto write_res = this->write_file(id, content);
            if (write_res.is_err()) {
                return ChfsResult<FileAttr>(write_res.unwrap_error());
            }
        }

        attr.size = sz;
        return ChfsResult<FileAttr>(attr);
    }

} // namespace chfs
