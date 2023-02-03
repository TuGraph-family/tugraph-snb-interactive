#include <algorithm>
#include <tuple>
#include <vector>

#include "lgraph/lgraph.h"
#include "snb_common.h"
#include "snb_constants.h"
#include "tsl/hopscotch_set.h"

extern "C" bool Process(lgraph_api::GraphDB& db, const std::string& request, std::string& response) {
    std::string input = lgraph_api::base64::Decode(request);
    std::stringstream iss(input);
    int64_t message_id = ReadInt64(iss);

    auto txn = db.CreateReadTxn();
    std::stringstream oss;

    auto fd = lgraph_api::FieldData::Int64(message_id);
    auto iit = txn.GetVertexIndexIterator(COMMENT, COMMENT_ID, fd, fd);
    bool message_is_post;
    int64_t message_vid;
    if (iit.IsValid()) {
        message_is_post = false;
        message_vid = iit.GetVid();
    } else {
        message_is_post = true;
        auto iit = txn.GetVertexIndexIterator(POST, POST_ID, fd, fd);
        message_vid = iit.GetVid();
    }
    auto message = txn.GetVertexIterator(message_vid);
    auto message_creator = !message_is_post ? txn.GetVertexIterator(message[COMMENT_CREATOR].integer())
                                            : txn.GetVertexIterator(message[POST_CREATOR].integer());
    tsl::hopscotch_set<int64_t> message_creator_friends;
    for (auto person_friends = lgraph_api::LabeledOutEdgeIterator(message_creator, KNOWS); person_friends.IsValid();
         person_friends.Next()) {
        message_creator_friends.emplace(person_friends.GetDst());
    }
    for (auto person_friends = lgraph_api::LabeledInEdgeIterator(message_creator, KNOWS); person_friends.IsValid();
         person_friends.Next()) {
        message_creator_friends.emplace(person_friends.GetSrc());
    }
    std::vector<std::tuple<int64_t, int64_t, int64_t, std::string, std::string, std::string, bool> > comments;
    for (auto replies = lgraph_api::LabeledInEdgeIterator(message, REPLYOF); replies.IsValid(); replies.Next()) {
        auto comment = txn.GetVertexIterator(replies.GetSrc());
        int64_t comment_author_vid = comment[COMMENT_CREATOR].integer();
        auto comment_author = txn.GetVertexIterator(comment_author_vid);
        bool knows = message_creator_friends.find(comment_author_vid) != message_creator_friends.end();
        comments.emplace_back(0 - comment[COMMENT_CREATIONDATE].integer(), comment_author[PERSON_ID].integer(),
                              comment[COMMENT_ID].integer(), comment[COMMENT_CONTENT].string(),
                              comment_author[PERSON_FIRSTNAME].string(), comment_author[PERSON_LASTNAME].string(),
                              knows);
    }
    std::sort(comments.begin(), comments.end());
    WriteInt16(oss, comments.size());
    for (auto& tup : comments) {
        WriteInt64(oss, std::get<2>(tup));
        WriteString(oss, std::get<3>(tup));
        WriteInt64(oss, 0 - std::get<0>(tup));
        WriteInt64(oss, std::get<1>(tup));
        WriteString(oss, std::get<4>(tup));
        WriteString(oss, std::get<5>(tup));
        WriteBool(oss, std::get<6>(tup));
    }

    response = oss.str();
    return true;
}
