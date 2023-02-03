#include "lgraph/lgraph.h"
#include "snb_common.h"
#include "snb_constants.h"

extern "C" bool Process(lgraph_api::GraphDB& db, const std::string& request, std::string& response) {
    std::stringstream oss;
    try {
        auto txn = db.CreateReadTxn();
        std::string input = lgraph_api::base64::Decode(request);
        std::stringstream iss(input);
        int64_t person_id = ReadInt64(iss);
        int64_t person_vid;
        {
            auto fd = lgraph_api::FieldData::Int64(person_id);
            auto iit = txn.GetVertexIndexIterator(PERSON, PERSON_ID, fd, fd);
            person_vid = iit.GetVid();
        }
        int64_t friend_id = ReadInt64(iss);
        int64_t friend_vid;
        {
            auto fd = lgraph_api::FieldData::Int64(friend_id);
            auto iit = txn.GetVertexIndexIterator(PERSON, PERSON_ID, fd, fd);
            friend_vid = iit.GetVid();
        }
        int64_t creation_date = ReadInt64(iss);

        bool committed = false;
        size_t num_attempts = 0;
        while (num_attempts < 3) {
            num_attempts++;
            try {
                txn.Abort();
                txn = db.CreateWriteTxn(num_attempts > 2 ? false : true);
                double weight = 0.0;
                for (auto person_comments = lgraph_api::LabeledInEdgeIterator(txn, person_vid, COMMENTHASCREATOR);
                     person_comments.IsValid(); person_comments.Next()) {
                    auto comment = txn.GetVertexIterator(person_comments.GetSrc());
                    auto fd = comment[COMMENT_REPLYOFPOST];
                    if (!fd.is_null()) {
                        int64_t post_vid = fd.integer();
                        auto& post = comment;
                        post.Goto(post_vid);
                        if (friend_vid == post[POST_CREATOR].integer()) weight += 1.0;
                    } else {
                        int64_t comment_vid = comment[COMMENT_REPLYOFCOMMENT].integer();
                        comment.Goto(comment_vid);
                        if (friend_vid == comment[COMMENT_CREATOR].integer()) weight += 0.5;
                    }
                }
                std::swap(person_vid, friend_vid);
                for (auto person_comments = lgraph_api::LabeledInEdgeIterator(txn, person_vid, COMMENTHASCREATOR);
                     person_comments.IsValid(); person_comments.Next()) {
                    auto comment = txn.GetVertexIterator(person_comments.GetSrc());
                    auto fd = comment[COMMENT_REPLYOFPOST];
                    if (!fd.is_null()) {
                        int64_t post_vid = fd.integer();
                        auto& post = comment;
                        post.Goto(post_vid);
                        if (friend_vid == post[POST_CREATOR].integer()) weight += 1.0;
                    } else {
                        int64_t comment_vid = comment[COMMENT_REPLYOFCOMMENT].integer();
                        comment.Goto(comment_vid);
                        if (friend_vid == comment[COMMENT_CREATOR].integer()) weight += 0.5;
                    }
                }
                std::swap(person_vid, friend_vid);
                txn.AddEdge(person_vid, friend_vid, KNOWS, {KNOWS_CREATIONDATE, KNOWS_WEIGHT},
                            {lgraph_api::FieldData::Int64(creation_date), lgraph_api::FieldData::Double(weight)});
                auto person = txn.GetVertexIterator(person_vid);
                person.SetField(PERSON_CREATIONDATE, person[PERSON_CREATIONDATE]);
                auto person_friend = txn.GetVertexIterator(friend_vid);
                person_friend.SetField(PERSON_CREATIONDATE, person_friend[PERSON_CREATIONDATE]);
                txn.Commit();
                committed = true;
                break;
            } catch (std::exception& e) {
                std::string s(e.what());
                if (s.find("CONFLICTS") == std::string::npos)
                    std::cout << "interactive_update_8 exception: " << e.what() << std::endl;
            }
        }
        if (committed) {
            WriteInt16(oss, 0);
        } else {
            std::cout << "interactive_update_8 failed" << std::endl;
            WriteInt16(oss, 1);
        }
    } catch (std::exception& e) {
        std::cout << "interactive_update_8 exception: " << e.what() << std::endl;
        std::cout << "interactive_update_8 failed" << std::endl;
        WriteInt16(oss, 1);
    }
    response = oss.str();
    return true;
}
