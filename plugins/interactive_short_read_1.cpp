#include "lgraph/lgraph.h"
#include "snb_common.h"
#include "snb_constants.h"

extern "C" bool Process(lgraph_api::GraphDB& db, const std::string& request, std::string& response) {
    std::string input = lgraph_api::base64::Decode(request);
    std::stringstream iss(input);
    int64_t person_id = ReadInt64(iss);

    auto txn = db.CreateReadTxn();
    std::stringstream oss;

    auto person = txn.GetVertexByUniqueIndex(PERSON, PERSON_ID, lgraph_api::FieldData::Int64(person_id));
    WriteString(oss, person[PERSON_FIRSTNAME].string());
    WriteString(oss, person[PERSON_LASTNAME].string());
    WriteInt64(oss, person[PERSON_BIRTHDAY].integer());
    WriteString(oss, person[PERSON_LOCATIONIP].string());
    WriteString(oss, person[PERSON_BROWSERUSED].string());
    auto place = txn.GetVertexIterator(person[PERSON_PLACE].integer());
    WriteInt64(oss, place[PLACE_ID].integer());
    WriteString(oss, person[PERSON_GENDER].string());
    WriteInt64(oss, person[PERSON_CREATIONDATE].integer());

    response = oss.str();
    return true;
}
