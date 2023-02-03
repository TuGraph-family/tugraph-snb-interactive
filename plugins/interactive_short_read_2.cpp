#include <set>

#include "lgraph/lgraph.h"
#include "snb_common.h"
#include "snb_constants.h"

extern "C" bool Process(lgraph_api::GraphDB& db, const std::string& request, std::string& response) {
    constexpr size_t limit_messages = 10;

    std::string input = lgraph_api::base64::Decode(request);
    std::stringstream iss(input);
    int64_t person_id = ReadInt64(iss);

    auto txn = db.CreateReadTxn();
    std::stringstream oss;

    // TODO: check whether there are cases when creationDates are the same while messageIds are different
    auto person = txn.GetVertexByUniqueIndex(PERSON, PERSON_ID, lgraph_api::FieldData::Int64(person_id));
    std::set<std::pair<int64_t, int64_t> > candidates;
    for (auto person_posts = lgraph_api::LabeledInEdgeIterator(person, POSTHASCREATOR); person_posts.IsValid();
         person_posts.Next()) {
        int64_t creation_date = person_posts[POSTHASCREATOR_CREATIONDATE].integer();
        if (candidates.size() < limit_messages || creation_date > candidates.begin()->first) {
            candidates.emplace(creation_date, person_posts.GetSrc());
            if (candidates.size() > limit_messages) {
                candidates.erase(candidates.begin());
            }
        }
    }
    for (auto person_comments = lgraph_api::LabeledInEdgeIterator(person, COMMENTHASCREATOR); person_comments.IsValid();
         person_comments.Next()) {
        int64_t creation_date = person_comments[POSTHASCREATOR_CREATIONDATE].integer();
        if (candidates.size() < limit_messages || creation_date > candidates.begin()->first) {
            candidates.emplace(creation_date, person_comments.GetSrc());
            if (candidates.size() > limit_messages) {
                candidates.erase(candidates.begin());
            }
        }
    }
    WriteInt16(oss, candidates.size());
    for (auto it = candidates.rbegin(); it != candidates.rend(); it++) {
        auto message = txn.GetVertexIterator(it->second);
        if (message.GetLabelId() == COMMENT) {
            int64_t message_id = message[COMMENT_ID].integer();
            WriteInt64(oss, message_id);
            WriteString(oss, message[COMMENT_CONTENT].string());
            WriteInt64(oss, it->first);
            while (true) {
                auto reply_of_comment = message[COMMENT_REPLYOFCOMMENT];
                if (!reply_of_comment.is_null()) {
                    message.Goto(reply_of_comment.integer());
                } else {
                    message.Goto(message[COMMENT_REPLYOFPOST].integer());
                    break;
                }
            }
            message_id = message[POST_ID].integer();
            WriteInt64(oss, message_id);
            auto author = txn.GetVertexIterator(message[POST_CREATOR].integer());
            WriteInt64(oss, author[PERSON_ID].integer());
            WriteString(oss, author[PERSON_FIRSTNAME].string());
            WriteString(oss, author[PERSON_LASTNAME].string());
        } else /* POST */ {
            int64_t message_id = message[POST_ID].integer();
            WriteInt64(oss, message_id);
            auto content = message[POST_CONTENT];
            if (!content.is_null()) {
                WriteString(oss, content.string());
            } else {
                WriteString(oss, message[POST_IMAGEFILE].string());
            }
            WriteInt64(oss, it->first);
            WriteInt64(oss, message_id);
            auto author = txn.GetVertexIterator(message[POST_CREATOR].integer());
            WriteInt64(oss, author[PERSON_ID].integer());
            WriteString(oss, author[PERSON_FIRSTNAME].string());
            WriteString(oss, author[PERSON_LASTNAME].string());
        }
    }

    response = oss.str();
    return true;
}
