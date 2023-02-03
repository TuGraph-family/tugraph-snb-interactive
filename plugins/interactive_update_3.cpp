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
        int64_t comment_id = ReadInt64(iss);
        int64_t comment_vid;
        {
            auto fd = lgraph_api::FieldData::Int64(comment_id);
            auto iit = txn.GetVertexIndexIterator(COMMENT, COMMENT_ID, fd, fd);
            comment_vid = iit.GetVid();
        }
        int64_t creation_date = ReadInt64(iss);
        bool committed = false;
        size_t num_attempts = 0;
        while (num_attempts < 3) {
            num_attempts++;
            try {
                txn.Abort();
                txn = db.CreateWriteTxn(num_attempts > 2 ? false : true);
                txn.AddEdge(person_vid, comment_vid, LIKES, {LIKES_CREATIONDATE},
                            {lgraph_api::FieldData::Int64(creation_date)});
                txn.Commit();
                committed = true;
                break;
            } catch (std::exception& e) {
                std::string s(e.what());
                if (s.find("CONFLICTS") == std::string::npos)
                    std::cout << "interactive_update_3 exception: " << e.what() << std::endl;
            }
        }
        if (committed) {
            WriteInt16(oss, 0);
        } else {
            std::cout << "interactive_update_3 failed" << std::endl;
            WriteInt16(oss, 1);
        }
    } catch (std::exception& e) {
        std::cout << "interactive_update_3 exception: " << e.what() << std::endl;
        std::cout << "interactive_update_3 failed" << std::endl;
        WriteInt16(oss, 1);
    }
    response = oss.str();
    return true;
}
