#include "lgraph/lgraph.h"
#include "snb_common.h"
#include "snb_constants.h"

extern "C" bool Process(lgraph_api::GraphDB& db, const std::string& request, std::string& response) {
    std::stringstream oss;
    try {
        auto txn = db.CreateReadTxn();
        std::string input = lgraph_api::base64::Decode(request);
        std::stringstream iss(input);
        int64_t comment_id = ReadInt64(iss);
        int64_t creation_date = ReadInt64(iss);
        std::string location_ip = ReadString(iss);
        std::string browser_used = ReadString(iss);
        std::string content = ReadString(iss);
        int32_t length = ReadInt32(iss);
        int64_t person_id = ReadInt64(iss);
        int64_t person_vid;
        {
            auto fd = lgraph_api::FieldData::Int64(person_id);
            auto iit = txn.GetVertexIndexIterator(PERSON, PERSON_ID, fd, fd);
            person_vid = iit.GetVid();
        }
        int64_t country_id = ReadInt64(iss);
        int64_t place_vid;
        {
            auto fd = lgraph_api::FieldData::Int64(country_id);
            auto iit = txn.GetVertexIndexIterator(PLACE, PLACE_ID, fd, fd);
            place_vid = iit.GetVid();
        }
        int64_t post_id = ReadInt64(iss);
        int64_t post_vid = -1;
        if (post_id != -1) {
            auto fd = lgraph_api::FieldData::Int64(post_id);
            auto iit = txn.GetVertexIndexIterator(POST, POST_ID, fd, fd);
            post_vid = iit.GetVid();
        }
        int64_t original_comment_id = ReadInt64(iss);
        int64_t original_comment_vid = -1;
        if (original_comment_id != -1) {
            auto fd = lgraph_api::FieldData::Int64(original_comment_id);
            auto iit = txn.GetVertexIndexIterator(COMMENT, COMMENT_ID, fd, fd);
            original_comment_vid = iit.GetVid();
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
            int64_t comment_vid =
                txn.AddVertex(COMMENT,
                              {COMMENT_ID, COMMENT_CREATIONDATE, COMMENT_LOCATIONIP, COMMENT_BROWSERUSED,
                               COMMENT_CONTENT, COMMENT_LENGTH, COMMENT_CREATOR, COMMENT_PLACE},
                              {lgraph_api::FieldData::Int64(comment_id), lgraph_api::FieldData::Int64(creation_date),
                               lgraph_api::FieldData::String(location_ip), lgraph_api::FieldData::String(browser_used),
                               lgraph_api::FieldData::String(content), lgraph_api::FieldData::Int32(length),
                               lgraph_api::FieldData::Int64(person_vid), lgraph_api::FieldData::Int64(place_vid)});
            auto comment = txn.GetVertexIterator(comment_vid);
            int64_t friend_vid;
            if (post_vid != -1) {
                comment.SetField(COMMENT_REPLYOFPOST, lgraph_api::FieldData::Int64(post_vid));
                txn.AddEdge(comment_vid, post_vid, REPLYOF, {REPLYOF_CREATIONDATE},
                            {lgraph_api::FieldData::Int64(creation_date)});
                auto post = txn.GetVertexIterator(post_vid);
                friend_vid = post[POST_CREATOR].integer();
            } else {
                comment.SetField(COMMENT_REPLYOFCOMMENT, lgraph_api::FieldData::Int64(original_comment_vid));
                txn.AddEdge(comment_vid, original_comment_vid, REPLYOF, {REPLYOF_CREATIONDATE},
                            {lgraph_api::FieldData::Int64(creation_date)});
                auto original_comment = txn.GetVertexIterator(original_comment_vid);
                friend_vid = original_comment[COMMENT_CREATOR].integer();
            }
            txn.AddEdge(comment_vid, person_vid, COMMENTHASCREATOR, {COMMENTHASCREATOR_CREATIONDATE},
                        {lgraph_api::FieldData::Int64(creation_date)});
            txn.AddEdge(comment_vid, place_vid, COMMENTISLOCATEDIN, {COMMENTISLOCATEDIN_CREATIONDATE},
                        {lgraph_api::FieldData::Int64(creation_date)});
            for (auto& tag_vid : tag_vids) {
                txn.AddEdge(comment_vid, tag_vid, COMMENTHASTAG, {}, {});
            }
            auto eit = txn.GetOutEdgeIterator(lgraph_api::EdgeUid(person_vid, friend_vid, KNOWS, 0, 0));
            bool ok = false;
            if (eit.IsValid()) {
                ok = true;
            } else {
                eit.Goto(lgraph_api::EdgeUid(friend_vid, person_vid, KNOWS, 0, 0));
                ok = eit.IsValid();
            }
            if (ok) {
                eit.SetField(KNOWS_WEIGHT,
                             lgraph_api::FieldData::Double(eit[KNOWS_WEIGHT].real() + ((post_vid != -1) ? 1.0 : 0.5)));
            }
            auto person = txn.GetVertexIterator(person_vid);
            person.SetField(PERSON_CREATIONDATE, person[PERSON_CREATIONDATE]);
            txn.Commit();
            committed = true;
        } catch (std::exception& e) {
            std::cout << "interactive_update_7 exception: " << e.what() << std::endl;
        }
        if (committed) {
            WriteInt16(oss, 0);
        } else {
            std::cout << "interactive_update_7 failed" << std::endl;
            WriteInt16(oss, 1);
        }
    } catch (std::exception& e) {
        std::cout << "interactive_update_7 exception: " << e.what() << std::endl;
        std::cout << "interactive_update_7 failed" << std::endl;
        WriteInt16(oss, 1);
    }
    response = oss.str();
    return true;
}
