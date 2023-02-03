#include <tuple>

#include "lgraph/lgraph.h"
#include "snb_common.h"
#include "snb_constants.h"
#include "tsl/hopscotch_map.h"

inline int64_t FetchPersonId(int64_t person_vid, lgraph_api::VertexIterator& person,
                             tsl::hopscotch_map<int64_t, int64_t>& person_id_map) {
    auto it = person_id_map.find(person_vid);
    if (it != person_id_map.end()) return it->second;
    person.Goto(person_vid);
    int64_t person_id = person[PERSON_ID].integer();
    person_id_map[person_vid] = person_id;
    return person_id;
}

void EnumeratePartialPaths(lgraph_api::Transaction& txn, const tsl::hopscotch_map<int64_t, int>& hop_info,
                           std::vector<std::pair<double, std::vector<int64_t> > >& paths, const int64_t person_vid,
                           const int depth, const double path_weight, std::vector<int64_t>& path, const bool reverse) {
    path.emplace_back(person_vid);
    if (depth != 0) {
        for (auto person_friends = lgraph_api::LabeledOutEdgeIterator(txn, person_vid, KNOWS); person_friends.IsValid();
             person_friends.Next()) {
            int64_t friend_vid = person_friends.GetDst();
            auto it = hop_info.find(friend_vid);
            if (it != hop_info.end() && it->second == depth - 1) {
                double weight = person_friends[KNOWS_WEIGHT].real();
                EnumeratePartialPaths(txn, hop_info, paths, friend_vid, depth - 1, path_weight + weight, path, reverse);
            }
        }
        for (auto person_friends = lgraph_api::LabeledInEdgeIterator(txn, person_vid, KNOWS); person_friends.IsValid();
             person_friends.Next()) {
            int64_t friend_vid = person_friends.GetSrc();
            auto it = hop_info.find(friend_vid);
            if (it != hop_info.end() && it->second == depth - 1) {
                double weight = person_friends[KNOWS_WEIGHT].real();
                EnumeratePartialPaths(txn, hop_info, paths, friend_vid, depth - 1, path_weight + weight, path, reverse);
            }
        }
    } else {
        if (reverse) {
            paths.emplace_back();
            paths.back().first = path_weight;
            paths.back().second.insert(paths.back().second.end(), path.rbegin(), path.rend());
        } else {
            paths.emplace_back(path_weight, path);
        }
    }
    path.pop_back();
}

void EnumerateAllShortestPaths(lgraph_api::Transaction& txn, const int64_t start_vid, const int64_t end_vid,
                               std::stringstream& oss) {
    if (start_vid == end_vid) {
        auto person = txn.GetVertexIterator(start_vid);
        // num_paths, path_length, [vids], weight
        WriteInt32(oss, 1);
        WriteInt32(oss, 0);
        WriteInt64(oss, person[PERSON_ID].integer());
        WriteDouble(oss, 0.0);
        return;
    }
    tsl::hopscotch_map<int64_t, int> parent({{start_vid, 0}});
    tsl::hopscotch_map<int64_t, int> child({{end_vid, 0}});
    std::vector<int64_t> forward_q({start_vid});
    std::vector<int64_t> backward_q({end_vid});
    int fhop = 0;
    int bhop = 0;
    std::vector<std::tuple<int64_t, int64_t, double> > hits;
    for (int hop = 0; !forward_q.empty() && !backward_q.empty() && hits.empty(); hop++) {
        std::vector<int64_t> next_q;
        if (forward_q.size() <= backward_q.size()) {
            fhop++;
            for (int64_t person_vid : forward_q) {
                for (auto person_friends = lgraph_api::LabeledOutEdgeIterator(txn, person_vid, KNOWS);
                     person_friends.IsValid(); person_friends.Next()) {
                    int64_t friend_vid = person_friends.GetDst();
                    double weight = person_friends[KNOWS_WEIGHT].real();
                    if (child.find(friend_vid) != child.end()) {
                        hits.emplace_back(person_vid, friend_vid, weight);
                    } else {
                        auto it = parent.find(friend_vid);
                        if (it == parent.end()) {
                            parent.emplace_hint(it, friend_vid, fhop);
                            next_q.emplace_back(friend_vid);
                        }
                    }
                }
                for (auto person_friends = lgraph_api::LabeledInEdgeIterator(txn, person_vid, KNOWS);
                     person_friends.IsValid(); person_friends.Next()) {
                    int64_t friend_vid = person_friends.GetSrc();
                    double weight = person_friends[KNOWS_WEIGHT].real();
                    if (child.find(friend_vid) != child.end()) {
                        hits.emplace_back(person_vid, friend_vid, weight);
                    } else {
                        auto it = parent.find(friend_vid);
                        if (it == parent.end()) {
                            parent.emplace_hint(it, friend_vid, fhop);
                            next_q.emplace_back(friend_vid);
                        }
                    }
                }
            }
            std::sort(next_q.begin(), next_q.end());
            forward_q.swap(next_q);
        } else {
            bhop++;
            for (int64_t person_vid : backward_q) {
                for (auto person_friends = lgraph_api::LabeledOutEdgeIterator(txn, person_vid, KNOWS);
                     person_friends.IsValid(); person_friends.Next()) {
                    int64_t friend_vid = person_friends.GetDst();
                    double weight = person_friends[KNOWS_WEIGHT].real();
                    if (parent.find(friend_vid) != parent.end()) {
                        hits.emplace_back(friend_vid, person_vid, weight);
                    } else {
                        auto it = child.find(friend_vid);
                        if (it == child.end()) {
                            child.emplace_hint(it, friend_vid, bhop);
                            next_q.emplace_back(friend_vid);
                        }
                    }
                }
                for (auto person_friends = lgraph_api::LabeledInEdgeIterator(txn, person_vid, KNOWS);
                     person_friends.IsValid(); person_friends.Next()) {
                    int64_t friend_vid = person_friends.GetSrc();
                    double weight = person_friends[KNOWS_WEIGHT].real();
                    if (parent.find(friend_vid) != parent.end()) {
                        hits.emplace_back(friend_vid, person_vid, weight);
                    } else {
                        auto it = child.find(friend_vid);
                        if (it == child.end()) {
                            child.emplace_hint(it, friend_vid, bhop);
                            next_q.emplace_back(friend_vid);
                        }
                    }
                }
            }
            std::sort(next_q.begin(), next_q.end());
            backward_q.swap(next_q);
        }
    }
    if (hits.empty()) {
        WriteInt32(oss, 0);
        WriteInt32(oss, -1);
        return;
    }
    std::vector<std::pair<double, std::vector<int64_t> > > paths;
    for (auto& tup : hits) {
        std::vector<std::pair<double, std::vector<int64_t> > > fpaths;
        std::vector<std::pair<double, std::vector<int64_t> > > bpaths;
        std::vector<int64_t> path;
        int64_t src = std::get<0>(tup);
        int64_t dst = std::get<1>(tup);
        EnumeratePartialPaths(txn, parent, fpaths, src, parent[src], 0.0, path, true);
        EnumeratePartialPaths(txn, child, bpaths, dst, child[dst], 0.0, path, false);
        for (auto& fpath : fpaths) {
            for (auto& bpath : bpaths) {
                paths.emplace_back(fpath.first + std::get<2>(tup) + bpath.first, fpath.second);
                paths.back().second.insert(paths.back().second.end(), bpath.second.begin(), bpath.second.end());
            }
        }
    }
    tsl::hopscotch_map<int64_t, int64_t> person_info;
    auto person = txn.GetVertexIterator();
    for (auto it = paths.rbegin(); it != paths.rend(); it++) {
        for (auto& vid : it->second) {
            int64_t person_id = FetchPersonId(vid, person, person_info);
            vid = person_id;
        }
        it->first = -it->first;
    }
    std::sort(paths.begin(), paths.end());
    WriteInt32(oss, paths.size());
    WriteInt32(oss, fhop + bhop);
    for (auto it = paths.begin(); it != paths.end(); it++) {
        for (auto& person_id : it->second) {
            WriteInt64(oss, person_id);
        }
        WriteDouble(oss, -it->first);
    }
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
    EnumerateAllShortestPaths(txn, start_vid, end_vid, oss);
    response = oss.str();
    return true;
}
