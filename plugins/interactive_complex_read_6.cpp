#include <set>

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
    std::string tag_name = ReadString(iss);

    auto txn = db.CreateReadTxn();
    auto person = txn.GetVertexByUniqueIndex(PERSON, PERSON_ID, lgraph_api::FieldData::Int64(person_id));
    int64_t start_vid = person.GetId();
    tsl::hopscotch_set<int64_t> visited({start_vid});
    std::vector<int64_t> curr_frontier({start_vid});
    for (int hop = 0; hop < 2; hop++) {
        std::vector<int64_t> next_frontier;
        for (auto vid : curr_frontier) {
            person.Goto(vid);
            for (auto person_friends = lgraph_api::LabeledOutEdgeIterator(person, KNOWS); person_friends.IsValid();
                 person_friends.Next()) {
                int friend_vid = person_friends.GetDst();
                if (visited.find(friend_vid) == visited.end()) {
                    visited.emplace(friend_vid);
                    if (hop < 1) next_frontier.emplace_back(friend_vid);
                }
            }
            for (auto person_friends = lgraph_api::LabeledInEdgeIterator(person, KNOWS); person_friends.IsValid();
                 person_friends.Next()) {
                int friend_vid = person_friends.GetSrc();
                if (visited.find(friend_vid) == visited.end()) {
                    visited.emplace(friend_vid);
                    if (hop < 1) next_frontier.emplace_back(friend_vid);
                }
            }
        }
        std::sort(next_frontier.begin(), next_frontier.end());
        curr_frontier.swap(next_frontier);
    }
    visited.erase(start_vid);
    auto tag = txn.GetVertexByUniqueIndex(TAG, TAG_NAME, lgraph_api::FieldData::String(tag_name));
    int64_t start_tag_vid = tag.GetId();
    auto& post = person;
    tsl::hopscotch_map<int64_t, int32_t> post_counts;
    for (auto tag_posts = lgraph_api::LabeledInEdgeIterator(tag, POSTHASTAG); tag_posts.IsValid(); tag_posts.Next()) {
        post.Goto(tag_posts.GetSrc());
        int64_t creator = post[POST_CREATOR].integer();
        if (visited.find(creator) == visited.end()) continue;
        for (auto post_tags = lgraph_api::LabeledOutEdgeIterator(post, POSTHASTAG); post_tags.IsValid();
             post_tags.Next()) {
            int64_t tag_vid = post_tags.GetDst();
            if (tag_vid == start_tag_vid) continue;
            auto it = post_counts.find(tag_vid);
            if (it != post_counts.end()) {
                it.value() += 1;
            } else {
                post_counts.emplace(tag_vid, 1);
            }
        }
    }

    std::set<std::pair<int32_t, std::string> > candidates;
    for (auto it = post_counts.begin(); it != post_counts.end(); it++) {
        int64_t tag_vid = it->first;
        std::string tag_name;
        int32_t post_count = it->second;
        if (candidates.size() >= limit_results) {
            auto& candidate = *candidates.rbegin();
            if (0 - post_count > candidate.first) continue;
            if (0 - post_count == candidate.first) {
                tag.Goto(tag_vid);
                tag_name = tag[TAG_NAME].string();
                if (tag_name > candidate.second) continue;
            }
        }
        if (tag_name.empty()) {
            tag.Goto(tag_vid);
            tag_name = tag[TAG_NAME].string();
        }
        candidates.emplace(0 - post_count, tag_name);
        if (candidates.size() > limit_results) {
            candidates.erase(--candidates.end());
        }
    }
    // output results
    std::stringstream oss;
    WriteInt16(oss, candidates.size());
    for (auto& tup : candidates) {
        WriteString(oss, tup.second);
        WriteInt32(oss, 0 - tup.first);
    }
    response = oss.str();
    return true;
}
