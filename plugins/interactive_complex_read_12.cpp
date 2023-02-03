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

void ProcessPersonComments(lgraph_api::VertexIterator& person, lgraph_api::VertexIterator& comment,
                           std::set<std::tuple<int32_t, int64_t, std::vector<int64_t>, int64_t>>& candidates,
                           const tsl::hopscotch_map<int64_t, std::string>& tag_info, const size_t limit_results) {
    int32_t count = 0;
    tsl::hopscotch_set<int64_t> tag_set;
    auto post_tags = lgraph_api::LabeledOutEdgeIterator(comment, POSTHASTAG);
    for (auto person_comments = lgraph_api::LabeledInEdgeIterator(person, COMMENTHASCREATOR); person_comments.IsValid();
         person_comments.Next()) {
        comment.Goto(person_comments.GetSrc());
        auto fd = comment[COMMENT_REPLYOFPOST];
        if (fd.is_null()) continue;
        int64_t post_vid = fd.integer();
        bool ok = false;
        for (post_tags.Reset(post_vid, POSTHASTAG); post_tags.IsValid(); post_tags.Next()) {
            int64_t tag_vid = post_tags.GetDst();
            if (tag_info.find(tag_vid) != tag_info.end()) {
                tag_set.emplace(tag_vid);
                ok = true;
            }
        }
        if (ok) count++;
    }
    if (count == 0) return;
    int64_t person_id = -1;
    if (candidates.size() >= limit_results) {
        auto& candidate = *candidates.rbegin();
        if (0 - count > std::get<0>(candidate)) return;
        if (0 - count == std::get<0>(candidate)) {
            person_id = person[PERSON_ID].integer();
            if (person_id > std::get<1>(candidate)) return;
        }
    }
    if (person_id == -1) person_id = person[PERSON_ID].integer();
    std::vector<int64_t> tag_list;
    for (auto tag_vid : tag_set) {
        tag_list.emplace_back(tag_vid);
    }
    candidates.emplace(0 - count, person_id, tag_list, person.GetId());
    if (candidates.size() > limit_results) {
        candidates.erase(--candidates.end());
    }
}

extern "C" bool Process(lgraph_api::GraphDB& db, const std::string& request, std::string& response) {
    constexpr size_t limit_results = 20;
    std::string input = lgraph_api::base64::Decode(request);
    std::stringstream iss(input);
    int64_t person_id = ReadInt64(iss);
    std::string tagclass_name = ReadString(iss);

    auto txn = db.CreateReadTxn();
    auto tagclass = txn.GetVertexByUniqueIndex(TAGCLASS, TAGCLASS_NAME, lgraph_api::FieldData::String(tagclass_name));
    int64_t start_vid = tagclass.GetId();
    tsl::hopscotch_set<int64_t> visited({start_vid});
    tsl::hopscotch_map<int64_t, std::string> tag_info;
    std::vector<int64_t> curr_frontier({start_vid});
    auto tagclass_subclasses = lgraph_api::LabeledInEdgeIterator(tagclass, ISSUBCLASSOF);
    auto tagclass_tags = lgraph_api::LabeledInEdgeIterator(tagclass, HASTYPE);
    auto tag = txn.GetVertexIterator();
    while (!curr_frontier.empty()) {
        std::vector<int64_t> next_frontier;
        for (auto tagclass_vid : curr_frontier) {
            tagclass.Goto(tagclass_vid);
            for (tagclass_subclasses.Reset(tagclass, ISSUBCLASSOF); tagclass_subclasses.IsValid();
                 tagclass_subclasses.Next()) {
                int64_t tagclass_vid = tagclass_subclasses.GetSrc();
                if (visited.find(tagclass_vid) == visited.end()) {
                    visited.emplace(tagclass_vid);
                    next_frontier.emplace_back(tagclass_vid);
                }
            }
            for (tagclass_tags.Reset(tagclass, HASTYPE); tagclass_tags.IsValid(); tagclass_tags.Next()) {
                int64_t tag_vid = tagclass_tags.GetSrc();
                if (tag_info.find(tag_vid) == tag_info.end()) {
                    tag.Goto(tag_vid);
                    tag_info.emplace(tag_vid, tag[TAG_NAME].string());
                }
            }
        }
        curr_frontier.swap(next_frontier);
    }
    std::vector<int64_t> friends;
    auto person = txn.GetVertexByUniqueIndex(PERSON, PERSON_ID, lgraph_api::FieldData::Int64(person_id));
    for (auto person_friends = lgraph_api::LabeledOutEdgeIterator(person, KNOWS); person_friends.IsValid();
         person_friends.Next()) {
        friends.emplace_back(person_friends.GetDst());
    }
    for (auto person_friends = lgraph_api::LabeledInEdgeIterator(person, KNOWS); person_friends.IsValid();
         person_friends.Next()) {
        friends.emplace_back(person_friends.GetSrc());
    }
    // result type
    using result_type = std::set<std::tuple<int32_t, int64_t, std::vector<int64_t>, int64_t>>;
    static std::vector<Worker> workers(worker_num);
    auto candidates = ForEachVertex<result_type>(
        db, txn, workers, friends,
        [&](Transaction& t, VertexIterator& vit, result_type& local) {
            auto comment = t.GetVertexIterator();
            ProcessPersonComments(vit, comment, local, tag_info, limit_results);
        },
        [&](const result_type& local, result_type& res) {
            for (auto& r : local) res.emplace(r);
        });
    int res_size = std::min(candidates.size(), limit_results);
    int res_count = 0;
    std::stringstream oss;
    WriteInt16(oss, res_size);
    for (auto& tup : candidates) {
        WriteInt64(oss, std::get<1>(tup));
        person.Goto(std::get<3>(tup));
        WriteString(oss, person[PERSON_FIRSTNAME].string());
        WriteString(oss, person[PERSON_LASTNAME].string());
        auto& tag_list = std::get<2>(tup);
        WriteInt16(oss, tag_list.size());
        for (auto tag_vid : tag_list) {
            WriteString(oss, tag_info[tag_vid]);
        }
        WriteInt32(oss, 0 - std::get<0>(tup));
        if (++res_count == res_size) break;
    }
    response = oss.str();
    return true;
}
