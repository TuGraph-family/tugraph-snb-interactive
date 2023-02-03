#include <set>
#include <tuple>

#include "lgraph/lgraph.h"
#include "lgraph/lgraph_traversal.h"
#include "snb_common.h"
#include "snb_constants.h"
#include "tsl/hopscotch_map.h"
#include "tsl/hopscotch_set.h"

using namespace lgraph_api;

constexpr int worker_num = 4;

void ProcessPersonPosts(lgraph_api::VertexIterator& person,
                        std::pair<std::set<int64_t>, std::vector<std::tuple<int64_t, int32_t>>>& post_counts,
                        const int64_t min_date) {
    for (auto person_forums = LabeledInEdgeIterator(person, HASMEMBER, min_date); person_forums.IsValid();
         person_forums.Next()) {
        auto forum_id = person_forums[HASMEMBER_FORUMID].integer();
        auto num_posts = person_forums[HASMEMBER_NUMPOSTS].integer();
        if (num_posts == 0) {
            auto &tail = post_counts.first;
            if (tail.size() < 20/*limit*/ || *tail.rbegin() > forum_id) {
                tail.emplace(forum_id);
                if (tail.size() > 20) tail.erase(--tail.end());
            }
        } else {
            post_counts.second.emplace_back(forum_id, num_posts);
        }
    }
}

extern "C" bool Process(GraphDB& db, const std::string& request, std::string& response) {
    constexpr size_t limit_results = 20;
    std::string input = lgraph_api::base64::Decode(request);
    std::stringstream iss(input);
    int64_t person_id = ReadInt64(iss);
    int64_t min_date = ReadInt64(iss);

    auto txn = db.CreateReadTxn();
    int64_t start_vid;
    {
        auto fd = FieldData::Int64(person_id);
        auto iit = txn.GetVertexIndexIterator(PERSON, PERSON_ID, fd, fd);
        start_vid = iit.GetVid();
    }
    tsl::hopscotch_set<int64_t> visited({start_vid});
    std::vector<int64_t> curr_frontier({start_vid});
    auto person = txn.GetVertexIterator();
    size_t stat_num_0 = 0;
    size_t stat_num_1 = 0;
    std::vector<int64_t> friends;
    for (int hop = 0; hop <= 2; hop++) {
        std::vector<int64_t> next_frontier;
        for (auto vid : curr_frontier) {
            person.Goto(vid);
            if (hop < 2) {
                for (auto person_friends = LabeledOutEdgeIterator(person, KNOWS); person_friends.IsValid();
                     person_friends.Next()) {
                    int64_t friend_vid = person_friends.GetDst();
                    if (visited.find(friend_vid) == visited.end()) {
                        visited.emplace(friend_vid);
                        next_frontier.emplace_back(friend_vid);
                    }
                }
                for (auto person_friends = LabeledInEdgeIterator(person, KNOWS); person_friends.IsValid();
                     person_friends.Next()) {
                    int64_t friend_vid = person_friends.GetSrc();
                    if (visited.find(friend_vid) == visited.end()) {
                        visited.emplace(friend_vid);
                        next_frontier.emplace_back(friend_vid);
                    }
                }
            }
            if (hop == 0) continue;
            friends.emplace_back(vid);
        }
        std::sort(next_frontier.begin(), next_frontier.end());
        curr_frontier.swap(next_frontier);
    }
    using result_type = std::pair<std::set<int64_t>, std::vector<std::tuple<int64_t, int32_t>>>;
    static std::vector<Worker> workers(worker_num);
    auto post_counts = ForEachVertex<result_type>(
        db, txn, workers, friends,
        [&](Transaction& t, VertexIterator& vit, result_type& local) {
            ProcessPersonPosts(vit, local, min_date);
        },
        [&](const result_type& local, result_type& res) {
            res.first.insert(local.first.begin(), local.first.end());
            res.second.insert(res.second.end(), local.second.begin(), local.second.end());
        });
    // select candidates
    std::set<std::tuple<int32_t, int64_t>> candidates;
    if (!post_counts.second.empty()) {
        std::sort(post_counts.second.begin(), post_counts.second.end());
        int64_t last_forum = std::get<0>(post_counts.second[0]);
        int32_t post_count = std::get<1>(post_counts.second[0]);
        for (size_t i = 1; i < post_counts.second.size(); i++) {
            int64_t curr_forum = std::get<0>(post_counts.second[i]);
            if (curr_forum == last_forum) {
                post_count += std::get<1>(post_counts.second[i]);
                continue;
            }
            int64_t forum_id = last_forum;
            auto tup = std::make_tuple(0 - post_count, forum_id);
            if (candidates.size() < limit_results || *candidates.rbegin() > tup) {
                candidates.emplace(tup);
                if (candidates.size() > limit_results){
                    candidates.erase(--candidates.end());
                }
            }
            last_forum = curr_forum;
            post_count = std::get<1>(post_counts.second[i]);
        }
        int64_t forum_id = last_forum;
        auto tup = std::make_tuple(0 - post_count, forum_id);
        if (candidates.size() < limit_results || *candidates.rbegin() > tup) {
            candidates.emplace(tup);
            if (candidates.size() > limit_results) {
                candidates.erase(--candidates.end());
            }
        }
    }

    // output results
    std::stringstream oss;
    int res_size = std::min(limit_results, candidates.size() + post_counts.first.size());
    WriteInt16(oss, res_size);
    for (auto& tup : candidates) {
        auto fit = txn.GetVertexByUniqueIndex(FORUM, FORUM_ID, lgraph_api::FieldData::Int64(std::get<1>(tup)));
        WriteString(oss, fit[FORUM_TITLE].string());
        WriteInt32(oss, 0 - std::get<0>(tup));
    }
    int res_count = candidates.size();
    if (res_count != res_size){
        tsl::hopscotch_set<int64_t> candidates_forum_ids;
        for (auto tuple_item:candidates) {
            candidates_forum_ids.emplace(std::get<1>(tuple_item));
        }
        for (auto it = post_counts.first.begin(); it != post_counts.first.end(); it++) {
            if (res_count == res_size) break;
            if (candidates_forum_ids.find(*it)!=candidates_forum_ids.end()) continue;
            auto fit = txn.GetVertexByUniqueIndex(FORUM, FORUM_ID, lgraph_api::FieldData::Int64(*it));
            WriteString(oss, fit[FORUM_TITLE].string());
            WriteInt32(oss, 0);
            res_count++;
        }
    }
    response = oss.str();
    return true;
}
