#include <map>
#include <tuple>

#include "lgraph/lgraph.h"
#include "snb_common.h"
#include "snb_constants.h"
#include "tsl/hopscotch_map.h"
#include "tsl/hopscotch_set.h"

inline int64_t FetchPersonId(int64_t person_vid, lgraph_api::VertexIterator& person,
                             tsl::hopscotch_map<int64_t, int64_t>& person_id_map) {
    auto it = person_id_map.find(person_vid);
    if (it != person_id_map.end()) return it->second;
    person.Goto(person_vid);
    int64_t person_id = person[PERSON_ID].integer();
    person_id_map[person_vid] = person_id;
    return person_id;
}

void ProcessMessageLikes(
    lgraph_api::VertexIterator& message, int64_t message_id, int64_t message_creation_date,
    const std::string& message_content, lgraph_api::VertexIterator& person,
    tsl::hopscotch_map<int64_t, int64_t>& person_id_map,
    tsl::hopscotch_map<int64_t, std::pair<int64_t, int64_t> >& candidates_index,
    std::map<std::pair<int64_t, int64_t>, std::tuple<int64_t, int64_t, std::string, int32_t> >& candidates,
    const size_t limit_results) {
    for (auto message_likes = lgraph_api::LabeledInEdgeIterator(message, LIKES); message_likes.IsValid();
         message_likes.Next()) {
        int64_t like_creation_date = message_likes[LIKES_CREATIONDATE].integer();
        int64_t person_vid = message_likes.GetSrc();
        auto it = candidates_index.find(person_vid);
        if (it != candidates_index.end()) {
            auto& key = it.value();
            if (like_creation_date < 0 - key.first) continue;
            if (like_creation_date == 0 - key.first) {
                auto cit = candidates.find(key);
                int64_t old_message_id = std::get<1>(cit->second);
                if (message_id > old_message_id) continue;
            }
            candidates.erase(key);
            key.first = 0 - like_creation_date;
            candidates.emplace(key, std::make_tuple(person_vid, message_id, message_content,
                                                    (like_creation_date - message_creation_date) / 1000 / 60));
        } else {
            int64_t person_id = FetchPersonId(person_vid, person, person_id_map);
            auto key = std::make_pair(0 - like_creation_date, person_id);
            if (candidates.size() >= limit_results && candidates.lower_bound(key) == candidates.end()) continue;
            candidates.emplace(key, std::make_tuple(person_vid, message_id, message_content,
                                                    (like_creation_date - message_creation_date) / 1000 / 60));
            candidates_index.emplace(person_vid, key);
            if (candidates.size() > limit_results) {
                auto it = --candidates.end();
                candidates_index.erase(candidates_index.find(std::get<0>(it->second)));
                candidates.erase(it);
            }
        }
    }
}

extern "C" bool Process(lgraph_api::GraphDB& db, const std::string& request, std::string& response) {
    constexpr size_t limit_results = 20;

    std::string input = lgraph_api::base64::Decode(request);
    std::stringstream iss(input);
    int64_t person_id = ReadInt64(iss);

    auto txn = db.CreateReadTxn();
    std::stringstream oss;

    tsl::hopscotch_set<int64_t> friends;
    tsl::hopscotch_map<int64_t, int64_t> person_id_map;
    auto person = txn.GetVertexByUniqueIndex(PERSON, PERSON_ID, lgraph_api::FieldData::Int64(person_id));
    auto person_friend = txn.GetVertexIterator();
    for (auto person_friends = lgraph_api::LabeledOutEdgeIterator(person, KNOWS); person_friends.IsValid();
         person_friends.Next()) {
        int64_t friend_vid = person_friends.GetDst();
        friends.emplace(friend_vid);
        person_friend.Goto(friend_vid);
        person_id_map[friend_vid] = person_friend[PERSON_ID].integer();
    }
    for (auto person_friends = lgraph_api::LabeledInEdgeIterator(person, KNOWS); person_friends.IsValid();
         person_friends.Next()) {
        int64_t friend_vid = person_friends.GetSrc();
        friends.emplace(friend_vid);
        person_friend.Goto(friend_vid);
        person_id_map[friend_vid] = person_friend[PERSON_ID].integer();
    }

    tsl::hopscotch_map<int64_t, std::pair<int64_t, int64_t> > candidates_index;
    std::map<std::pair<int64_t, int64_t>, std::tuple<int64_t, int64_t, std::string, int32_t> > candidates;
    auto message = txn.GetVertexIterator();
    auto liker = txn.GetVertexIterator();
    for (auto person_posts = lgraph_api::LabeledInEdgeIterator(person, POSTHASCREATOR); person_posts.IsValid();
         person_posts.Next()) {
        int64_t message_vid = person_posts.GetSrc();
        message.Goto(message_vid);
        int64_t message_id;
        int64_t message_creation_date;
        std::string message_content;
        message_id = message[POST_ID].integer();
        message_creation_date = message[POST_CREATIONDATE].integer();
        auto fd = message[POST_CONTENT];
        if (!fd.is_null()) {
            message_content = fd.string();
        } else {
            message_content = message[POST_IMAGEFILE].string();
        }
        ProcessMessageLikes(message, message_id, message_creation_date, message_content, liker, person_id_map,
                            candidates_index, candidates, limit_results);
    }
    for (auto person_comments = lgraph_api::LabeledInEdgeIterator(person, COMMENTHASCREATOR); person_comments.IsValid();
         person_comments.Next()) {
        int64_t message_vid = person_comments.GetSrc();
        message.Goto(message_vid);
        int64_t message_id;
        int64_t message_creation_date;
        std::string message_content;
        message_id = message[COMMENT_ID].integer();
        message_creation_date = message[COMMENT_CREATIONDATE].integer();
        message_content = message[COMMENT_CONTENT].string();
        ProcessMessageLikes(message, message_id, message_creation_date, message_content, liker, person_id_map,
                            candidates_index, candidates, limit_results);
    }

    WriteInt16(oss, candidates.size());
    for (auto it = candidates.begin(); it != candidates.end(); it++) {
        WriteInt64(oss, it->first.second);
        int64_t person_vid = std::get<0>(it->second);
        person.Goto(person_vid);
        WriteString(oss, person[PERSON_FIRSTNAME].string());
        WriteString(oss, person[PERSON_LASTNAME].string());
        WriteInt64(oss, 0 - it->first.first);
        WriteInt64(oss, std::get<1>(it->second));
        WriteString(oss, std::get<2>(it->second));
        WriteInt32(oss, std::get<3>(it->second));
        WriteBool(oss, friends.find(person_vid) == friends.end());
    }

    response = oss.str();
    return true;
}
