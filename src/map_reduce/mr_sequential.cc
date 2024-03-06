#include <string>
#include <utility>
#include <vector>
#include <algorithm>

#include "map_reduce/protocol.h"

namespace mapReduce {
    SequentialMapReduce::SequentialMapReduce(std::shared_ptr<chfs::ChfsClient> client,
                                             const std::vector<std::string> &files_, std::string resultFile) {
        chfs_client = std::move(client);
        files = files_;
        outPutFile = resultFile;
        // Your code goes here (optional)
    }

    void SequentialMapReduce::doWork() {
        std::unordered_map<std::string, std::vector<std::string>> intermediate;

        // 遍历每个文件并处理
        for (const auto &file : files) {
            // 从分布式文件系统中获取文件内容
            auto resLookup = chfs_client->lookup(1, file);
            auto inodeId = resLookup.unwrap();
            auto [inodeType, fileAttr] = chfs_client->get_type_attr(inodeId).unwrap();
            auto fileContent = chfs_client->read_file(inodeId,0,fileAttr.size).unwrap();
            auto fileContentStr = std::string(fileContent.begin(), fileContent.end());
            // 使用Map函数处理文件内容
            auto keyVals = Map(fileContentStr);
            // 将Map函数的输出添加到中间结果中
            for (const auto &kv : keyVals) {
                intermediate[kv.key].push_back(kv.val);
            }
        }

        // 存储归约后的结果
        auto result_ss = std::stringstream();

        // 使用Reduce函数进行归约并将结果写入字符串流
        for (const auto &[key, values] : intermediate) {
            auto count = Reduce(key, values);
            result_ss << key << ' ' << count << '\n';
        }

        // 将字符串流的内容转换为字节
        auto result_content = result_ss.str();
        auto result = std::vector<uint8_t>(result_content.begin(), result_content.end());

        try {
            // 获取输出文件的inode_id并写入结果
            auto output_file_inode_id = chfs_client->lookup(1, outPutFile).unwrap();
            chfs_client->write_file(output_file_inode_id, 0, result);
        } catch (const std::exception &e) {
            // 错误处理
            std::cerr << "Error writing to output file: " << e.what() << std::endl;
        }

    }

}