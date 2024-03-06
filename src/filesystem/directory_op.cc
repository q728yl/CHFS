#include <algorithm>
#include <sstream>

#include "filesystem/directory_op.h"

namespace chfs {

/**
 * Some helper functions
 */
    auto string_to_inode_id(std::string &data) -> inode_id_t {
        std::stringstream ss(data);
        inode_id_t inode;
        ss >> inode;
        return inode;
    }

    auto inode_id_to_string(inode_id_t id) -> std::string {
        std::stringstream ss;
        ss << id;
        return ss.str();
    }

// {Your code here}
    auto dir_list_to_string(const std::list<DirectoryEntry> &entries)
    -> std::string {
        std::ostringstream oss;
        usize cnt = 0;
        for (const auto &entry: entries) {
            oss << entry.name << ':' << entry.id;
            if (cnt < entries.size() - 1) {
                oss << '/';
            }
            cnt += 1;
        }
        return oss.str();
    }

// {Your code here}
    auto append_to_directory(std::string src, std::string filename, inode_id_t id)
    -> std::string {

        // TODO: Implement this function.
        //       Append the new directory entry to `src`.
        //UNIMPLEMENTED();
        if (!src.empty()) {
            src += '/';
        }
        src += filename + ':' + std::to_string(id);
        return src;
    }

// {Your code here}
/**
 * Parse a directory of content "name0:inode0/name1:inode1/ ..."
 * in the list.
 *
 * # Warn
 * Note that, we assume the file name will not contain '/'
 *
 * @param src: the string to parse
 * @param list: the list to store the parsed content
 */
    void parse_directory(std::string &src, std::list<DirectoryEntry> &list) {

        // TODO: Implement this function.
        //UNIMPLEMENTED();
        list.clear();
        if (src.empty()) {
            return;
        }
        std::istringstream iss(src);
        std::string entry;
        while (std::getline(iss, entry, '/')) {
            std::size_t tmp = entry.find(':');
            if (tmp != std::string::npos) {
                //分割出name Id
                std::string name = entry.substr(0, tmp);
                std::string id_str = entry.substr(tmp + 1);
                inode_id_t id = string_to_inode_id(id_str);
                list.push_back({name, id});
            }
        }

    }

// {Your code here}
    auto rm_from_directory(std::string src, std::string filename) -> std::string {


        std::list<DirectoryEntry> TheList;
        parse_directory(src, TheList);

        TheList.remove_if([&filename](const DirectoryEntry &entry) {
            return entry.name == filename;
        });

        return dir_list_to_string(TheList);
        // TODO: Implement this function.
        //       Remove the directory entry from `src`.
//        UNIMPLEMENTED();


    }

/**
 * { Your implementation here }
 */
    auto read_directory(FileOperation *fs, inode_id_t id,
                        std::list<DirectoryEntry> &list) -> ChfsNullResult {

//        // TODO: Implement this function.
//        UNIMPLEMENTED();
//
//        return KNullOk;
        std::vector<u8> dir = fs->read_file(id).unwrap();
        std::string dirS(dir.begin(), dir.end());
        parse_directory(dirS, list);

        return KNullOk;
    }

// {Your code here}
    auto FileOperation::lookup(inode_id_t id, const char *name)
    -> ChfsResult<inode_id_t> {
        std::list<DirectoryEntry> list;

        // TODO: Implement this function.
//        UNIMPLEMENTED();
        read_directory(this,id, list);

        for (auto &entry : list) {
            if (entry.name == name) {
                return ChfsResult<inode_id_t>(entry.id);
            }
        }

        return ChfsResult<inode_id_t>(ErrorType::NotExist);
    }
    /**
     * Helper function to create directory or file
     *
     * @param parent the id of the parent
     * @param name the name of the directory
     */
// {Your code here}
    auto FileOperation::mk_helper(inode_id_t id, const char *name, InodeType type)
    -> ChfsResult<inode_id_t> {

        // TODO:
        // 1. Check if `name` already exists in the parent.
        //    If already exist, return ErrorType::AlreadyExist.
        // 2. Create the new inode.
        // 3. Append the new entry to the parent directory.
//        UNIMPLEMENTED();
        std::list<DirectoryEntry> list;
        read_directory(this,id, list);
     //   std::cout<<"当前目录下的内容有几条"<<list.size()<<std::endl;

        for (auto &entry : list) {
          //  std::cout<<entry.name<<entry.id<<std::endl;
            if (entry.name == name) {
                return ChfsResult<inode_id_t>(ErrorType::AlreadyExist);
            }
        }
        std::string dirString = dir_list_to_string(list);
        inode_id_t nId = alloc_inode(type).unwrap();
      //  std::cout<<"FileOperation::mk_helper里面分配到的inodeId:"<<nId<<std::endl;
        std::string nString = append_to_directory(dirString,name,nId);
        std::vector<u8> newDirString(nString.size());
        memcpy(newDirString.data(), nString.c_str(), nString.size());
        write_file(id, newDirString);
        return ChfsResult<inode_id_t>(static_cast<inode_id_t>(nId));
    }
    /**
      * Remove the file named @name from directory @parent.
      * Free the file's blocks.
      *
      * @return  If the file doesn't exist, indicate error ENOENT.
      * @return  ENOTEMPTY if the deleted file is a directory
      */
// {Your code here}
    auto FileOperation::unlink(inode_id_t parent, const char *name)
    -> ChfsNullResult {

        // TODO:
        // 1. Remove the file, you can use the function `remove_file`
        // 2. Remove the entry from the directory.
//        UNIMPLEMENTED();
//
        std::list<DirectoryEntry> list;
        read_directory(this,parent, list);

        bool entryFound = false;
        for (const auto &entry : list) {
            if (entry.name == name) {
                entryFound = true;
                break;
            }
        }

        if (!entryFound) {
            return ErrorType(ENOENT);
        }

        ChfsResult<inode_id_t> inodeId = lookup(parent, name);

//        InodeType type = (gettype(inodeId.unwrap()).unwrap());
//        if (type == InodeType::Directory) {
//            return ErrorType(ENOTEMPTY);
//        }

        remove_file(inodeId.unwrap());
        std::string dir_content = rm_from_directory(dir_list_to_string(list), name);
        std::vector<u8> s (dir_content.size());
        memcpy(s.data(),dir_content.c_str(),dir_content.size());
        write_file(parent, s);

        return KNullOk;
//        return KNullOk;
    }

} // namespace chfs
