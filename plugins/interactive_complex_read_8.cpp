#include <set>
#include <tuple>

#include "lgraph/lgraph.h"
#include "lgraph/lgraph_traversal.h"
#include "snb_common.h"
#include "snb_constants.h"

using namespace lgraph_api;

constexpr int worker_num = 4;

void ProcessMessage(lgraph_api::VertexIterator& message,
                    std::set<std::tuple<int64_t, int64_t, std::string, int64_t>>& candidates,
                    const size_t limit_results) {
    for (auto message_replies = lgraph_api::LabeledInEdgeIterator(message, REPLYOF); message_replies.IsValid();
         message_replies.Next()) {
        int64_t creation_date = message_replies[REPLYOF_CREATIONDATE].integer();
        int64_t comment_id = -1;
        auto& comment = message;
        if (candidates.size() >= limit_results) {
            auto& candidate = *candidates.rbegin();
            if (0 - creation_date > std::get<0>(candidate)) continue;
            if (0 - creation_date == std::get<0>(candidate)) {
                comment.Goto(message_replies.GetSrc());
                comment_id = comment[COMMENT_ID].integer();
                if (comment_id > std::get<1>(candidate)) continue;
            }
        }
        if (comment_id == -1) {
            comment.Goto(message_replies.GetSrc());
            comment_id = comment[COMMENT_ID].integer();
        }
        std::string comment_content = comment[COMMENT_CONTENT].string();
        int64_t comment_creator = comment[COMMENT_CREATOR].integer();
        candidates.emplace(0 - creation_date, comment_id, comment_content, comment_creator);
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

    auto txn = db.CreateReadTxn();
    auto person = txn.GetVertexByUniqueIndex(PERSON, PERSON_ID, lgraph_api::FieldData::Int64(person_id));
    std::vector<int64_t> messages;
    for (auto person_posts = lgraph_api::LabeledInEdgeIterator(person, POSTHASCREATOR); person_posts.IsValid();
         person_posts.Next()) {
        messages.push_back(person_posts.GetSrc());
    }
    for (auto person_comments = lgraph_api::LabeledInEdgeIterator(person, COMMENTHASCREATOR); person_comments.IsValid();
         person_comments.Next()) {
        messages.push_back(person_comments.GetSrc());
    }
    using result_type = std::set<std::tuple<int64_t, int64_t, std::string, int64_t>>;
    static std::vector<Worker> workers(worker_num);
    auto candidates = ForEachVertex<result_type>(
        db, txn, workers, messages,
        [&](Transaction& t, VertexIterator& vit, result_type& local) {
            ProcessMessage(vit, local, limit_results);
        },
        [&](const result_type& local, result_type& res) {
            for (auto& r : local) res.emplace(r);
        });
    // output results
    std::stringstream oss;
    int res_size = std::min(candidates.size(), limit_results);
    int res_count = 0;
    WriteInt16(oss, res_size);
    for (auto& tup : candidates) {
        int64_t person_vid = std::get<3>(tup);
        person.Goto(person_vid);
        WriteInt64(oss, person[PERSON_ID].integer());
        WriteString(oss, person[PERSON_FIRSTNAME].string());
        WriteString(oss, person[PERSON_LASTNAME].string());
        WriteInt64(oss, 0 - std::get<0>(tup));
        WriteInt64(oss, std::get<1>(tup));
        WriteString(oss, std::get<2>(tup));
        if (++res_count == res_size) break;
    }
    response = oss.str();
    return true;
}
