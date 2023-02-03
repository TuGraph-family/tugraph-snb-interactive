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
        int64_t forum_id = ReadInt64(iss);
        int64_t forum_vid;
        {
            auto fd = lgraph_api::FieldData::Int64(forum_id);
            auto iit = txn.GetVertexIndexIterator(FORUM, FORUM_ID, fd, fd);
            forum_vid = iit.GetVid();
        }
        int64_t join_date = ReadInt64(iss);
        bool committed = false;
        size_t num_attempts = 0;
        while (num_attempts < 3) {
            num_attempts++;
            try {
                txn.Abort();
                txn = db.CreateWriteTxn(num_attempts > 2 ? false : true);
                int32_t num_posts = 0;
                auto person = txn.GetVertexIterator(person_vid);
                for (auto person_posts = lgraph_api::LabeledInEdgeIterator(person, POSTHASCREATOR);
                     person_posts.IsValid(); person_posts.Next()) {
                    auto post = txn.GetVertexIterator(person_posts.GetSrc());
                    int64_t container_vid = post[POST_CONTAINER].integer();
                    if (container_vid == forum_vid) num_posts++;
                }
                auto forum = txn.GetVertexIterator(forum_vid);
                txn.AddEdge(forum_vid, person_vid, HASMEMBER,
                            {HASMEMBER_JOINDATE, HASMEMBER_NUMPOSTS, HASMEMBER_FORUMID},
                            {lgraph_api::FieldData::Int64(join_date), lgraph_api::FieldData::Int32(num_posts),
                             lgraph_api::FieldData::Int64(forum[FORUM_ID].integer())});
                person.Goto(person_vid);
                person.SetField(PERSON_CREATIONDATE, person[PERSON_CREATIONDATE]);
                txn.Commit();
                committed = true;
                break;
            } catch (std::exception& e) {
                std::string s(e.what());
                if (s.find("CONFLICTS") == std::string::npos)
                    std::cout << "interactive_update_5 exception: " << e.what() << std::endl;
            }
        }
        if (committed) {
            WriteInt16(oss, 0);
        } else {
            std::cout << "interactive_update_5 failed" << std::endl;
            WriteInt16(oss, 1);
        }
    } catch (std::exception& e) {
        std::cout << "interactive_update_5 exception: " << e.what() << std::endl;
        std::cout << "interactive_update_5 failed" << std::endl;
        WriteInt16(oss, 1);
    }
    response = oss.str();
    return true;
}
