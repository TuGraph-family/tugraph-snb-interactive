#include "lgraph/lgraph.h"
#include "snb_common.h"
#include "snb_constants.h"

extern "C" bool Process(lgraph_api::GraphDB& db, const std::string& request, std::string& response) {
    std::stringstream oss;
    try {
        auto txn = db.CreateReadTxn();
        std::string input = lgraph_api::base64::Decode(request);
        std::stringstream iss(input);
        int64_t forum_id = ReadInt64(iss);
        std::string forum_title = ReadString(iss);
        int64_t creation_date = ReadInt64(iss);
        int64_t person_id = ReadInt64(iss);
        int64_t person_vid;
        {
            auto fd = lgraph_api::FieldData::Int64(person_id);
            auto iit = txn.GetVertexIndexIterator(PERSON, PERSON_ID, fd, fd);
            person_vid = iit.GetVid();
        }
        int16_t num_items;
        std::vector<int64_t> tag_vids;
        num_items = ReadInt16(iss);
        for (int i = 0; i < num_items; i++) {
            int64_t tag_id = ReadInt64(iss);
            auto fd = lgraph_api::FieldData::Int64(tag_id);
            auto iit = txn.GetVertexIndexIterator(TAG, TAG_ID, fd, fd);
            int64_t tag_vid = iit.GetVid();
            tag_vids.emplace_back(tag_vid);
        }
        bool committed = false;
        try {
            txn.Abort();
            txn = db.CreateWriteTxn();
            int64_t forum_vid =
                txn.AddVertex(FORUM, {FORUM_ID, FORUM_TITLE, FORUM_CREATIONDATE, FORUM_MODERATOR},
                              {lgraph_api::FieldData::Int64(forum_id), lgraph_api::FieldData::String(forum_title),
                               lgraph_api::FieldData::Int64(creation_date), lgraph_api::FieldData::Int64(person_vid)});
            txn.AddEdge(forum_vid, person_vid, HASMODERATOR, {}, {});
            for (auto& tag_vid : tag_vids) {
                txn.AddEdge(forum_vid, tag_vid, FORUMHASTAG, {}, {});
            }
            txn.Commit();
            committed = true;
        } catch (std::exception& e) {
            std::cout << "interactive_update_4 exception: " << e.what() << std::endl;
        }
        if (committed) {
            WriteInt16(oss, 0);
        } else {
            std::cout << "interactive_update_4 failed" << std::endl;
            WriteInt16(oss, 1);
        }
    } catch (std::exception& e) {
        std::cout << "interactive_update_4 exception: " << e.what() << std::endl;
        std::cout << "interactive_update_4 failed" << std::endl;
        WriteInt16(oss, 1);
    }
    response = oss.str();
    return true;
}
