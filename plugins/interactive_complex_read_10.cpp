#include <set>
#include <tuple>

#include "lgraph/lgraph.h"
#include "lgraph/lgraph_traversal.h"
#include "snb_common.h"
#include "snb_constants.h"
#include "tsl/hopscotch_set.h"

using namespace lgraph_api;

constexpr int worker_num = 4;

extern "C" bool Process(lgraph_api::GraphDB &db, const std::string &request, std::string &response) {
    constexpr size_t limit_results = 10;
    std::string input = base64::Decode(request);
    std::stringstream iss(input);
    int64_t person_id = ReadInt64(iss);
    int32_t month = ReadInt32(iss);
    int32_t next_month = month % 12 + 1;

    auto txn = db.CreateReadTxn();
    auto person = txn.GetVertexByUniqueIndex(PERSON, PERSON_ID, FieldData::Int64(person_id));
    tsl::hopscotch_set<int64_t> interested_tags;
    for (auto person_tags = LabeledOutEdgeIterator(person, HASINTEREST); person_tags.IsValid(); person_tags.Next()) {
        interested_tags.emplace(person_tags.GetDst());
    }
    int64_t start_vid = person.GetId();
    tsl::hopscotch_set<int64_t> visited({start_vid});
    std::vector<int64_t> curr_frontier({start_vid});
    auto person_out_friends = LabeledOutEdgeIterator(person, KNOWS);
    auto person_in_friends = LabeledInEdgeIterator(person, KNOWS);
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
        std::sort(next_frontier.begin(), next_frontier.end());
        curr_frontier.swap(next_frontier);
    }
    auto &two_hop_friends = curr_frontier;
    std::sort(two_hop_friends.begin(), two_hop_friends.end());
    using result_type = std::set<std::tuple<int32_t, int64_t, int64_t>>;
    static std::vector<Worker> workers(worker_num);
    auto candidates = ForEachVertex<result_type>(
        db, txn, workers, two_hop_friends,
        [&](Transaction &t, VertexIterator &vit, result_type &local) {
            auto msg_it = t.GetVertexIterator();
            auto month_day = GetMonthDay(vit[PERSON_BIRTHDAY].integer());
            bool ok = (month_day.first == month && month_day.second >= 21) ||
                      (month_day.first == next_month && month_day.second < 22);
            if (!ok) return;
            int32_t score = 0;
            for (auto person_msgs = LabeledInEdgeIterator(vit, POSTHASCREATOR); person_msgs.IsValid();
                 person_msgs.Next()) {
                msg_it.Goto(person_msgs.GetSrc());
                ok = false;
                for (auto post_ts = LabeledOutEdgeIterator(msg_it, POSTHASTAG); post_ts.IsValid(); post_ts.Next()) {
                    if (interested_tags.find(post_ts.GetDst()) != interested_tags.end()) {
                        ok = true;
                        break;
                    }
                }
                score += (ok ? +1 : -1);
            }
            int64_t person_id = -1;
            if (local.size() >= limit_results) {
                auto &cand = *local.rbegin();
                if (0 - score > std::get<0>(cand)) return;
                if (0 - score == std::get<0>(cand)) {
                    person_id = vit[PERSON_ID].integer();
                    if (person_id > std::get<1>(cand)) return;
                }
            }
            if (person_id == -1) person_id = vit[PERSON_ID].integer();
            local.emplace(0 - score, person_id, vit.GetId());
            if (local.size() > limit_results) {
                local.erase(--local.end());
            }
        },
        [&](const result_type &local, result_type &res) {
            for (auto &r : local) res.emplace(r);
        }, 10);
    // output results
    std::stringstream oss;
    int res_size = std::min(candidates.size(), limit_results);
    int res_count = 0;
    WriteInt16(oss, res_size);
    for (auto &tup : candidates) {
        WriteInt64(oss, std::get<1>(tup));
        person.Goto(std::get<2>(tup));
        WriteString(oss, person[PERSON_FIRSTNAME].string());
        WriteString(oss, person[PERSON_LASTNAME].string());
        WriteInt32(oss, 0 - std::get<0>(tup));
        WriteString(oss, person[PERSON_GENDER].string());
        auto place = txn.GetVertexIterator(person[PERSON_PLACE].integer());
        WriteString(oss, place[PLACE_NAME].string());
        if (++res_count == res_size) break;
    }
    response = oss.str();
    return true;
}
