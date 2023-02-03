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
    int64_t person_vid;
    if (iit.IsValid()) {
        auto comment = txn.GetVertexIterator(iit.GetVid());
        person_vid = comment[COMMENT_CREATOR].integer();
    } else {
        auto post = txn.GetVertexByUniqueIndex(POST, POST_ID, fd);
        person_vid = post[POST_CREATOR].integer();
    }
    auto person = txn.GetVertexIterator(person_vid);
    WriteInt64(oss, person[PERSON_ID].integer());
    WriteString(oss, person[PERSON_FIRSTNAME].string());
    WriteString(oss, person[PERSON_LASTNAME].string());

    response = oss.str();
    return true;
}
