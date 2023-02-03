#include <set>
#include <tuple>

#include "lgraph/lgraph.h"
#include "snb_common.h"
#include "snb_constants.h"
#include "tsl/hopscotch_set.h"

extern "C" bool Process(lgraph_api::GraphDB& db, const std::string& request, std::string& response) {
    constexpr size_t limit_results = 20;
    std::string input = lgraph_api::base64::Decode(request);
    std::stringstream iss(input);
    int64_t person_id = ReadInt64(iss);
    std::string first_name = ReadString(iss);

    auto txn = db.CreateReadTxn();
    std::set<std::tuple<int, std::string, int64_t, int64_t> > candidates;
    auto fd = lgraph_api::FieldData::Int64(person_id);
    auto iit = txn.GetVertexIndexIterator(PERSON, PERSON_ID, fd, fd);
    int64_t start_vid = iit.GetVid();
    std::vector<int64_t> curr_frontier({start_vid});
    tsl::hopscotch_set<int64_t> visited({start_vid});
    auto person = txn.GetVertexIterator();

    for (int distance = 0; distance <= 3; distance++) {
        std::vector<int64_t> next_frontier;
        for (auto vid : curr_frontier) {
            person.Goto(vid);
            if (person.GetId() == start_vid || person[PERSON_FIRSTNAME].string() != first_name) continue;
            std::string last_name = person[PERSON_LASTNAME].string();
            int64_t person_id = person[PERSON_ID].integer();
            auto tup = std::make_tuple(distance, last_name, person_id, vid);
            if (candidates.size() >= limit_results) {
                auto& candidate = *candidates.rbegin();
                if (tup > candidate) continue;
            }
            candidates.emplace(tup);
            if (candidates.size() > limit_results) {
                candidates.erase(--candidates.end());
            }
        }
        if (candidates.size() >= limit_results || distance == 3) break;
        for (auto vid : curr_frontier) {
            person.Goto(vid);
            for (auto person_friends = lgraph_api::LabeledOutEdgeIterator(person, KNOWS); person_friends.IsValid();
                 person_friends.Next()) {
                int64_t friend_vid = person_friends.GetDst();
                if (visited.find(friend_vid) == visited.end()) {
                    visited.emplace(friend_vid);
                    next_frontier.emplace_back(friend_vid);
                }
            }
            for (auto person_friends = lgraph_api::LabeledInEdgeIterator(person, KNOWS); person_friends.IsValid();
                 person_friends.Next()) {
                int64_t friend_vid = person_friends.GetSrc();
                if (visited.find(friend_vid) == visited.end()) {
                    visited.emplace(friend_vid);
                    next_frontier.emplace_back(friend_vid);
                }
            }
        }
        std::sort(next_frontier.begin(), next_frontier.end());
        curr_frontier.swap(next_frontier);
    }
    // output result
    std::stringstream oss;
    WriteInt16(oss, candidates.size());
    auto place = txn.GetVertexIterator();
    auto organisation = txn.GetVertexIterator();
    for (auto it = candidates.begin(); it != candidates.end(); it++) {
        auto& tup = *it;
        int64_t vid = std::get<3>(tup);
        person.Goto(vid);
        WriteInt64(oss, std::get<2>(tup));
        WriteString(oss, std::get<1>(tup));
        WriteInt32(oss, std::get<0>(tup));
        WriteInt64(oss, person[PERSON_BIRTHDAY].integer());
        WriteInt64(oss, person[PERSON_CREATIONDATE].integer());
        WriteString(oss, person[PERSON_GENDER].string());
        WriteString(oss, person[PERSON_BROWSERUSED].string());
        WriteString(oss, person[PERSON_LOCATIONIP].string());
        WriteString(oss, person[PERSON_EMAIL].string());
        WriteString(oss, person[PERSON_SPEAKS].string());
        place.Goto(person[PERSON_PLACE].integer());
        WriteString(oss, place[PLACE_NAME].string());
        std::vector<std::tuple<std::string, int32_t, std::string> > list_exp;
        for (auto person_study_at = lgraph_api::LabeledOutEdgeIterator(person, STUDYAT); person_study_at.IsValid();
             person_study_at.Next()) {
            organisation.Goto(person_study_at.GetDst());
            place.Goto(organisation[ORGANISATION_PLACE].integer());
            list_exp.emplace_back(organisation[ORGANISATION_NAME].string(),
                                  person_study_at[STUDYAT_CLASSYEAR].integer(), place[PLACE_NAME].string());
        }
        WriteInt16(oss, list_exp.size());
        for (auto& tup : list_exp) {
            WriteString(oss, std::get<0>(tup));
            WriteInt32(oss, std::get<1>(tup));
            WriteString(oss, std::get<2>(tup));
        }
        list_exp.clear();
        for (auto person_work_at = lgraph_api::LabeledOutEdgeIterator(person, WORKAT); person_work_at.IsValid();
             person_work_at.Next()) {
            organisation.Goto(person_work_at.GetDst());
            place.Goto(organisation[ORGANISATION_PLACE].integer());
            list_exp.emplace_back(organisation[ORGANISATION_NAME].string(), person_work_at[WORKAT_WORKFROM].integer(),
                                  place[PLACE_NAME].string());
        }
        WriteInt16(oss, list_exp.size());
        for (auto& tup : list_exp) {
            WriteString(oss, std::get<0>(tup));
            WriteInt32(oss, std::get<1>(tup));
            WriteString(oss, std::get<2>(tup));
        }
    }
    response = oss.str();
    return true;
}
