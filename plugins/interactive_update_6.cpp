#include "lgraph/lgraph.h"
#include "snb_common.h"
#include "snb_constants.h"

extern "C" bool Process(lgraph_api::GraphDB& db, const std::string& request, std::string& response) {
    std::stringstream oss;
    try {
        auto txn = db.CreateReadTxn();
        std::string input = lgraph_api::base64::Decode(request);
        std::stringstream iss(input);
        int64_t post_id = ReadInt64(iss);
        std::string image_file = ReadString(iss);
        int64_t creation_date = ReadInt64(iss);
        std::string location_ip = ReadString(iss);
        std::string browser_used = ReadString(iss);
        std::string language = ReadString(iss);
        std::string content = ReadString(iss);
        int32_t length = ReadInt32(iss);
        int64_t person_id = ReadInt64(iss);
        int64_t person_vid;
        {
            auto fd = lgraph_api::FieldData::Int64(person_id);
            auto iit = txn.GetVertexIndexIterator(PERSON, PERSON_ID, fd, fd);
            person_vid = iit.GetVid();
        }
        int64_t forum_id = ReadInt64(iss);
        int64_t forum_vid;
        {
            auto fd = lgraph_api::FieldData::Int64(forum_id);
            auto iit = txn.GetVertexIndexIterator(FORUM, FORUM_ID, fd, fd);
            forum_vid = iit.GetVid();
        }
        int64_t country_id = ReadInt64(iss);
        int64_t place_vid;
        {
            auto fd = lgraph_api::FieldData::Int64(country_id);
            auto iit = txn.GetVertexIndexIterator(PLACE, PLACE_ID, fd, fd);
            place_vid = iit.GetVid();
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
            int64_t post_vid =
                txn.AddVertex(POST,
                              {POST_ID, POST_CREATIONDATE, POST_LOCATIONIP, POST_BROWSERUSED, POST_LANGUAGE,
                               POST_LENGTH, POST_CREATOR, POST_CONTAINER, POST_PLACE},
                              {lgraph_api::FieldData::Int64(post_id), lgraph_api::FieldData::Int64(creation_date),
                               lgraph_api::FieldData::String(location_ip), lgraph_api::FieldData::String(browser_used),
                               lgraph_api::FieldData::String(language), lgraph_api::FieldData::Int32(length),
                               lgraph_api::FieldData::Int64(person_vid), lgraph_api::FieldData::Int64(forum_vid),
                               lgraph_api::FieldData::Int64(place_vid)});
            auto post = txn.GetVertexIterator(post_vid);
            if (!image_file.empty()) {
                post.SetField(POST_IMAGEFILE, lgraph_api::FieldData::String(image_file));
            } else {
                post.SetField(POST_CONTENT, lgraph_api::FieldData::String(content));
            }
            txn.AddEdge(post_vid, person_vid, POSTHASCREATOR, {POSTHASCREATOR_CREATIONDATE},
                        {lgraph_api::FieldData::Int64(creation_date)});
            txn.AddEdge(post_vid, place_vid, POSTISLOCATEDIN, {POSTISLOCATEDIN_CREATIONDATE},
                        {lgraph_api::FieldData::Int64(creation_date)});
            txn.AddEdge(forum_vid, post_vid, CONTAINEROF, {}, {});
            for (auto& tag_vid : tag_vids) {
                txn.AddEdge(post_vid, tag_vid, POSTHASTAG, {}, {});
            }
            auto eit = txn.GetOutEdgeIterator(forum_vid, person_vid, HASMEMBER);
            if (eit.IsValid()) {
                eit.SetField(HASMEMBER_NUMPOSTS, lgraph_api::FieldData::Int32(eit[HASMEMBER_NUMPOSTS].integer() + 1));
            }
            auto person = txn.GetVertexIterator(person_vid);
            person.SetField(PERSON_CREATIONDATE, person[PERSON_CREATIONDATE]);
            txn.Commit();
            committed = true;
        } catch (std::exception& e) {
            std::cout << "interactive_update_6 exception: " << e.what() << std::endl;
        }
        if (committed) {
            WriteInt16(oss, 0);
        } else {
            std::cout << "interactive_update_6 failed" << std::endl;
            WriteInt16(oss, 1);
        }
    } catch (std::exception& e) {
        std::cout << "interactive_update_6 exception: " << e.what() << std::endl;
        std::cout << "interactive_update_6 failed" << std::endl;
        WriteInt16(oss, 1);
    }
    response = oss.str();
    return true;
}
