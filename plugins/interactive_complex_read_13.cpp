#include "lgraph/lgraph.h"
#include "snb_common.h"
#include "snb_constants.h"
#include "tsl/hopscotch_map.h"

int32_t CalcShortestPathLength(lgraph_api::Transaction& txn, const int64_t start_vid, const int64_t end_vid) {
    if (start_vid == end_vid) {
        return 0;
    }
    tsl::hopscotch_map<int64_t, int64_t> parent({{start_vid, start_vid}});
    tsl::hopscotch_map<int64_t, int64_t> child({{end_vid, end_vid}});
    std::vector<int64_t> forward_q({start_vid});
    std::vector<int64_t> backward_q({end_vid});
    for (int hop = 0; !forward_q.empty() && !backward_q.empty(); hop++) {
        std::vector<int64_t> next_q;
        if (forward_q.size() <= backward_q.size()) {
            for (int64_t person_vid : forward_q) {
                for (auto person_friends = lgraph_api::LabeledOutEdgeIterator(txn, person_vid, KNOWS);
                     person_friends.IsValid(); person_friends.Next()) {
                    int64_t friend_vid = person_friends.GetDst();
                    if (child.find(friend_vid) != child.end()) return hop + 1;
                    auto it = parent.find(friend_vid);
                    if (it == parent.end()) {
                        parent.emplace_hint(it, friend_vid, person_vid);
                        next_q.emplace_back(friend_vid);
                    }
                }
                for (auto person_friends = lgraph_api::LabeledInEdgeIterator(txn, person_vid, KNOWS);
                     person_friends.IsValid(); person_friends.Next()) {
                    int64_t friend_vid = person_friends.GetSrc();
                    if (child.find(friend_vid) != child.end()) return hop + 1;
                    auto it = parent.find(friend_vid);
                    if (it == parent.end()) {
                        parent.emplace_hint(it, friend_vid, person_vid);
                        next_q.emplace_back(friend_vid);
                    }
                }
            }
            std::sort(next_q.begin(), next_q.end());
            forward_q.swap(next_q);
        } else {
            for (int64_t person_vid : backward_q) {
                for (auto person_friends = lgraph_api::LabeledOutEdgeIterator(txn, person_vid, KNOWS);
                     person_friends.IsValid(); person_friends.Next()) {
                    int64_t friend_vid = person_friends.GetDst();
                    if (parent.find(friend_vid) != parent.end()) return hop + 1;
                    auto it = child.find(friend_vid);
                    if (it == child.end()) {
                        child.emplace_hint(it, friend_vid, person_vid);
                        next_q.emplace_back(friend_vid);
                    }
                }
                for (auto person_friends = lgraph_api::LabeledInEdgeIterator(txn, person_vid, KNOWS);
                     person_friends.IsValid(); person_friends.Next()) {
                    int64_t friend_vid = person_friends.GetSrc();
                    if (parent.find(friend_vid) != parent.end()) return hop + 1;
                    auto it = child.find(friend_vid);
                    if (it == child.end()) {
                        child.emplace_hint(it, friend_vid, person_vid);
                        next_q.emplace_back(friend_vid);
                    }
                }
            }
            std::sort(next_q.begin(), next_q.end());
            backward_q.swap(next_q);
        }
    }
    return -1;
}

extern "C" bool Process(lgraph_api::GraphDB& db, const std::string& request, std::string& response) {
    constexpr size_t limit_results = 20;

    std::string input = lgraph_api::base64::Decode(request);
    std::stringstream iss(input);
    int64_t person1_id = ReadInt64(iss);
    int64_t person2_id = ReadInt64(iss);

    auto txn = db.CreateReadTxn();
    int64_t start_vid;
    {
        auto fd = lgraph_api::FieldData::Int64(person1_id);
        auto iit = txn.GetVertexIndexIterator(PERSON, PERSON_ID, fd, fd);
        start_vid = iit.GetVid();
    }
    int64_t end_vid;
    {
        auto fd = lgraph_api::FieldData::Int64(person2_id);
        auto iit = txn.GetVertexIndexIterator(PERSON, PERSON_ID, fd, fd);
        end_vid = iit.GetVid();
    }
    std::stringstream oss;
    WriteInt32(oss, CalcShortestPathLength(txn, start_vid, end_vid));
    response = oss.str();
    return true;
}
