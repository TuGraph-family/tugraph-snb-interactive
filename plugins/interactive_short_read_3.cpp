#include <algorithm>
#include <tuple>
#include <vector>

#include "lgraph/lgraph.h"
#include "lgraph/lgraph_traversal.h"
#include "snb_common.h"
#include "snb_constants.h"

using namespace lgraph_api;

constexpr int worker_num = 8;

extern "C" bool Process(lgraph_api::GraphDB& db, const std::string& request, std::string& response) {
    std::string input = lgraph_api::base64::Decode(request);
    std::stringstream iss(input);
    int64_t person_id = ReadInt64(iss);

    auto txn = db.CreateReadTxn();
    auto person = txn.GetVertexByUniqueIndex(PERSON, PERSON_ID, lgraph_api::FieldData::Int64(person_id));
    std::vector<int64_t> friends;
    std::vector<int64_t> dates;
    for (auto person_friends = lgraph_api::LabeledOutEdgeIterator(person, KNOWS); person_friends.IsValid();
         person_friends.Next()) {
        friends.emplace_back(person_friends.GetDst());
        dates.emplace_back(person_friends[KNOWS_CREATIONDATE].integer());
    }
    for (auto person_friends = lgraph_api::LabeledInEdgeIterator(person, KNOWS); person_friends.IsValid();
         person_friends.Next()) {
        friends.emplace_back(person_friends.GetSrc());
        dates.emplace_back(person_friends[KNOWS_CREATIONDATE].integer());
    }
    using result_type = std::tuple<int64_t, int64_t, std::string, std::string>;
    static std::vector<Worker> workers(worker_num);
    auto candidates =
        ForEachVertex<result_type>(db, txn, workers, friends, [&](Transaction& t, VertexIterator& vit, size_t idx) {
            return std::make_tuple(0 - dates[idx], vit[PERSON_ID].integer(), vit[PERSON_FIRSTNAME].string(),
                                   vit[PERSON_LASTNAME].string());
        }, 6);
    std::sort(candidates.begin(), candidates.end());
    std::stringstream oss;
    WriteInt16(oss, candidates.size());
    for (auto& tup : candidates) {
        WriteInt64(oss, std::get<1>(tup));
        WriteString(oss, std::get<2>(tup));
        WriteString(oss, std::get<3>(tup));
        WriteInt64(oss, 0 - std::get<0>(tup));
    }
    response = oss.str();
    return true;
}
