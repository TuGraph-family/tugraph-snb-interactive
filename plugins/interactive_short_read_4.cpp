#include "lgraph/lgraph.h"
#include "snb_common.h"
#include "snb_constants.h"

extern "C" bool Process(lgraph_api::GraphDB& db, const std::string& request, std::string& response) {
    std::string input = lgraph_api::base64::Decode(request);
    std::stringstream iss(input);
    int64_t message_id = ReadInt64(iss);

    auto txn = db.CreateReadTxn();
    std::stringstream oss;

    auto fd = lgraph_api::FieldData::Int64(message_id);
    auto iit = txn.GetVertexIndexIterator(COMMENT, COMMENT_ID, fd, fd);
    if (iit.IsValid()) {
        auto comment = txn.GetVertexIterator(iit.GetVid());
        WriteInt64(oss, comment[COMMENT_CREATIONDATE].integer());
        WriteString(oss, comment[COMMENT_CONTENT].string());
    } else {
        auto post = txn.GetVertexByUniqueIndex(POST, POST_ID, fd);
        WriteInt64(oss, post[POST_CREATIONDATE].integer());
        auto content = post[POST_CONTENT];
        if (!content.is_null()) {
            WriteString(oss, content.string());
        } else {
            WriteString(oss, post[POST_IMAGEFILE].string());
        }
    }

    response = oss.str();
    return true;
}
