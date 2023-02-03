#include <set>

#include "lgraph/lgraph.h"
#include "snb_common.h"
#include "snb_constants.h"
#include "tsl/hopscotch_map.h"

void ProcessPersonPosts(lgraph_api::VertexIterator& person, lgraph_api::VertexIterator& message,
                        tsl::hopscotch_map<int64_t, std::pair<int64_t, int32_t> >& tag_stats,
                        const int64_t start_date, const int64_t end_date) {
    for (auto person_posts = lgraph_api::LabeledInEdgeIterator(person, POSTHASCREATOR); person_posts.IsValid();
         person_posts.Next()) {
        int64_t creation_date = person_posts[POSTHASCREATOR_CREATIONDATE].integer();
        if (creation_date > end_date) continue;
        message.Goto(person_posts.GetSrc());
        for (auto message_tags = lgraph_api::LabeledOutEdgeIterator(message, POSTHASTAG); message_tags.IsValid();
             message_tags.Next()) {
            int64_t tag_vid = message_tags.GetDst();
            auto it = tag_stats.find(tag_vid);
            if (it == tag_stats.end()) {
                tag_stats.emplace(tag_vid, std::make_pair(creation_date, 1));
            } else {
                it.value().first = std::min(it->second.first, creation_date);
                it.value().second++;
            }
        }
    }
}

extern "C" bool Process(lgraph_api::GraphDB& db, const std::string& request, std::string& response) {
    constexpr size_t limit_results = 10;

    std::string input = lgraph_api::base64::Decode(request);
    std::stringstream iss(input);
    int64_t person_id = ReadInt64(iss);
    int64_t start_date = ReadInt64(iss);
    int64_t duration_days = ReadInt32(iss);
    int64_t end_date = start_date + duration_days * 24 * 3600 * 1000;

    auto txn = db.CreateReadTxn();
    auto person = txn.GetVertexByUniqueIndex(PERSON, PERSON_ID, lgraph_api::FieldData::Int64(person_id));
    auto person_friend = txn.GetVertexIterator();
    auto message = txn.GetVertexIterator();
    tsl::hopscotch_map<int64_t, std::pair<int64_t, int32_t> > tag_stats;
    for (auto person_friends = lgraph_api::LabeledOutEdgeIterator(person, KNOWS); person_friends.IsValid();
         person_friends.Next()) {
        person_friend.Goto(person_friends.GetDst());
        ProcessPersonPosts(person_friend, message, tag_stats, start_date, end_date);
    }
    for (auto person_friends = lgraph_api::LabeledInEdgeIterator(person, KNOWS); person_friends.IsValid();
         person_friends.Next()) {
        person_friend.Goto(person_friends.GetSrc());
        ProcessPersonPosts(person_friend, message, tag_stats, start_date, end_date);
    }

    std::set<std::pair<int32_t, std::string> > candidates;
    auto tag = txn.GetVertexIterator();
    for (auto it = tag_stats.begin(); it != tag_stats.end(); it++) {
        if (it->second.first < start_date) continue;
        int64_t tag_vid = it->first;
        auto post_count = it->second.second;
        std::string tag_name;
        if (candidates.size() >= limit_results) {
            auto& candidate = *candidates.rbegin();
            if (0 - post_count > std::get<0>(candidate)) continue;
            if (0 - post_count == std::get<0>(candidate)) {
                tag.Goto(tag_vid);
                tag_name = tag[TAG_NAME].string();
                if (tag_name > std::get<1>(candidate)) continue;
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
        WriteString(oss, std::get<1>(tup));
        WriteInt32(oss, 0 - std::get<0>(tup));
    }
    response = oss.str();
    return true;
}
