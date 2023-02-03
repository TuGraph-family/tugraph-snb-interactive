#include <set>
#include <tuple>

#include "lgraph/lgraph.h"
#include "snb_common.h"
#include "snb_constants.h"
#include "tsl/hopscotch_map.h"
#include "tsl/hopscotch_set.h"

extern "C" bool Process(lgraph_api::GraphDB& db, const std::string& request, std::string& response) {
    constexpr size_t limit_results = 10;
    std::string input = lgraph_api::base64::Decode(request);
    std::stringstream iss(input);
    int64_t person_id = ReadInt64(iss);
    std::string country_name = ReadString(iss);
    int32_t year = ReadInt32(iss);

    auto txn = db.CreateReadTxn();
    tsl::hopscotch_set<int64_t> visited;
    auto person = txn.GetVertexByUniqueIndex(PERSON, PERSON_ID, lgraph_api::FieldData::Int64(person_id));
    auto person_out_friends = lgraph_api::LabeledOutEdgeIterator(person, KNOWS);
    auto person_in_friends = lgraph_api::LabeledInEdgeIterator(person, KNOWS);
    std::vector<int64_t> curr_frontier({person.GetId()});
    for (int hop = 0; hop < 2; hop++) {
        std::vector<int64_t> next_frontier;
        for (auto vid : curr_frontier) {
            person.Goto(vid);
            for (person_out_friends.Reset(person, KNOWS); person_out_friends.IsValid(); person_out_friends.Next()) {
                int64_t friend_vid = person_out_friends.GetDst();
                if (visited.find(friend_vid) == visited.end()) {
                    visited.emplace(friend_vid);
                    next_frontier.emplace_back(friend_vid);
                }
            }
            for (person_in_friends.Reset(person, KNOWS); person_in_friends.IsValid(); person_in_friends.Next()) {
                int64_t friend_vid = person_in_friends.GetSrc();
                if (visited.find(friend_vid) == visited.end()) {
                    visited.emplace(friend_vid);
                    next_frontier.emplace_back(friend_vid);
                }
            }
        }
        if (hop == 1) break;
        std::sort(next_frontier.begin(), next_frontier.end());
        curr_frontier.swap(next_frontier);
    }
    visited.erase(person.GetId());
    auto country = txn.GetVertexIterator();
    {
        auto fd = lgraph_api::FieldData::String(country_name);
        auto iit = txn.GetVertexIndexIterator(PLACE, PLACE_NAME, fd, fd);
        while (iit.IsValid()) {
            country.Goto(iit.GetVid());
            if (country[PLACE_TYPE].string() == "country") break;
            iit.Next();
        }
    }
    tsl::hopscotch_map<int64_t, std::string> organisation_info;
    std::vector<std::tuple<int32_t, int64_t, std::string, std::string, std::string>> result;
    for (auto country_organisations = lgraph_api::LabeledInEdgeIterator(country, ORGANISATIONISLOCATEDIN);
         country_organisations.IsValid(); country_organisations.Next()) {
        auto company_vid = country_organisations.GetSrc();
        auto company = txn.GetVertexIterator(company_vid);
        for (auto person_work_exps = lgraph_api::LabeledInEdgeIterator(company, WORKAT); person_work_exps.IsValid();
             person_work_exps.Next()) {
            auto person_vid0 = person_work_exps.GetSrc();
            int32_t work_from_year = person_work_exps[WORKAT_WORKFROM].integer();
            if (visited.contains(person_vid0) && work_from_year < year) {
                if (!organisation_info.contains(company_vid)) {
                    organisation_info.emplace(company_vid, company[ORGANISATION_NAME].string());
                }
                auto person0 = txn.GetVertexIterator(person_vid0);
                auto person_id0 = person0[PERSON_ID].AsInt64();
                auto person_first_name = person0[PERSON_FIRSTNAME].string();
                auto person_last_name = person0[PERSON_LASTNAME].string();
                result.emplace_back(0 - work_from_year, 0 - person_id0, organisation_info[company_vid],
                                    person_first_name, person_last_name);
            }
        }
    }
    std::sort(result.begin(), result.end(),
              std::greater<std::tuple<int32_t, int64_t, std::string, std::string, std::string>>());
    int result_size = result.size() > limit_results ? limit_results : result.size();
    std::stringstream oss;
    WriteInt16(oss, result_size);
    for (int i = 0; i < result_size; i++) {
        auto tup = result[i];
        WriteInt64(oss, 0 - std::get<1>(tup));
        WriteString(oss, std::get<3>(tup));
        WriteString(oss, std::get<4>(tup));
        WriteString(oss, std::get<2>(tup));
        WriteInt32(oss, 0 - std::get<0>(tup));
    }
    response = oss.str();
    return true;
}
