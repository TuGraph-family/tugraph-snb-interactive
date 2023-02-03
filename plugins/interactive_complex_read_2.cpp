#include <set>
#include <tuple>

#include "lgraph/lgraph.h"
#include "snb_common.h"
#include "snb_constants.h"

void ProcessFriendMessages(
    lgraph_api::VertexIterator& person_friend, lgraph_api::VertexIterator& message,
    std::set<std::tuple<int64_t, int64_t, int64_t, std::string, std::string, std::string> >& candidates,
    const int64_t max_date, const size_t limit_results) {
    for (auto friend_posts = lgraph_api::LabeledInEdgeIterator(person_friend, POSTHASCREATOR); friend_posts.IsValid();
         friend_posts.Next()) {
        int64_t creation_date = friend_posts[POSTHASCREATOR_CREATIONDATE].integer();
        if (creation_date > max_date) continue;
        int64_t message_vid = -1;
        int64_t message_id;
        if (candidates.size() >= limit_results) {
            auto& candidate = *candidates.rbegin();
            if (0 - creation_date > std::get<0>(candidate)) continue;
            if (0 - creation_date == std::get<0>(candidate)) {
                message_vid = friend_posts.GetSrc();
                message.Goto(message_vid);
                message_id = message[POST_ID].integer();
                if (message_id > std::get<1>(candidate)) continue;
            }
        }
        if (message_vid == -1) {
            message_vid = friend_posts.GetSrc();
            message.Goto(message_vid);
            message_id = message[POST_ID].integer();
        }
        std::string message_content;
        auto content = message[POST_CONTENT];
        if (!content.is_null()) {
            message_content = content.string();
        } else {
            message_content = message[POST_IMAGEFILE].string();
        }
        candidates.emplace(0 - creation_date, message_id, person_friend[PERSON_ID].integer(),
                           person_friend[PERSON_FIRSTNAME].string(), person_friend[PERSON_LASTNAME].string(),
                           message_content);
        if (candidates.size() > limit_results) {
            candidates.erase(--candidates.end());
        }
    }
    for (auto friend_comments = lgraph_api::LabeledInEdgeIterator(person_friend, COMMENTHASCREATOR);
         friend_comments.IsValid(); friend_comments.Next()) {
        int64_t creation_date = friend_comments[COMMENTHASCREATOR_CREATIONDATE].integer();
        if (creation_date > max_date) continue;
        int64_t message_vid = -1;
        int64_t message_id;
        if (candidates.size() >= limit_results) {
            auto& candidate = *candidates.rbegin();
            if (0 - creation_date > std::get<0>(candidate)) continue;
            if (0 - creation_date == std::get<0>(candidate)) {
                message_vid = friend_comments.GetSrc();
                message.Goto(message_vid);
                message_id = message[COMMENT_ID].integer();
                if (message_id > std::get<1>(candidate)) continue;
            }
        }
        if (message_vid == -1) {
            message_vid = friend_comments.GetSrc();
            message.Goto(message_vid);
            message_id = message[COMMENT_ID].integer();
        }
        std::string message_content;
        message_content = message[COMMENT_CONTENT].string();
        candidates.emplace(0 - creation_date, message_id, person_friend[PERSON_ID].integer(),
                           person_friend[PERSON_FIRSTNAME].string(), person_friend[PERSON_LASTNAME].string(),
                           message_content);
        if (candidates.size() > limit_results) {
            candidates.erase(--candidates.end());
        }
    }
}

extern "C" bool Process(lgraph_api::GraphDB& db, const std::string& request, std::string& response) {
    constexpr size_t limit_results = 20;

    std::string input = lgraph_api::base64::Decode(request);
    std::stringstream iss(input);
    int64_t person_id = ReadInt64(iss);
    int64_t max_date = ReadInt64(iss);

    auto txn = db.CreateReadTxn();
    auto person = txn.GetVertexByUniqueIndex(PERSON, PERSON_ID, lgraph_api::FieldData::Int64(person_id));
    auto person_friend = txn.GetVertexIterator();
    auto message = txn.GetVertexIterator();
    std::set<std::tuple<int64_t, int64_t, int64_t, std::string, std::string, std::string> > candidates;
    for (auto person_friends = lgraph_api::LabeledOutEdgeIterator(person, KNOWS); person_friends.IsValid();
         person_friends.Next()) {
        person_friend.Goto(person_friends.GetDst());
        ProcessFriendMessages(person_friend, message, candidates, max_date, limit_results);
    }
    for (auto person_friends = lgraph_api::LabeledInEdgeIterator(person, KNOWS); person_friends.IsValid();
         person_friends.Next()) {
        person_friend.Goto(person_friends.GetSrc());
        ProcessFriendMessages(person_friend, message, candidates, max_date, limit_results);
    }
    // output results
    std::stringstream oss;
    WriteInt16(oss, candidates.size());
    for (auto& tup : candidates) {
        WriteInt64(oss, std::get<2>(tup));
        WriteString(oss, std::get<3>(tup));
        WriteString(oss, std::get<4>(tup));
        WriteInt64(oss, std::get<1>(tup));
        WriteString(oss, std::get<5>(tup));
        WriteInt64(oss, 0 - std::get<0>(tup));
    }
    response = oss.str();
    return true;
}
