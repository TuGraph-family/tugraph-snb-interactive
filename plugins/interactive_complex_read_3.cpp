#include <set>
#include <tuple>

#include "lgraph/lgraph.h"
#include "snb_common.h"
#include "snb_constants.h"
#include "tsl/hopscotch_map.h"
#include "tsl/hopscotch_set.h"

extern "C" bool Process(lgraph_api::GraphDB& db, const std::string& request, std::string& response) {
    constexpr size_t limit_results = 20;
    std::string input = lgraph_api::base64::Decode(request);
    std::stringstream iss(input);
    int64_t person_id = ReadInt64(iss);
    std::string country_x_name = ReadString(iss);
    std::string country_y_name = ReadString(iss);
    int64_t start_date = ReadInt64(iss);
    int64_t duration_days = ReadInt32(iss);
    int64_t end_date = start_date + duration_days * 24 * 3600 * 1000;

    auto txn = db.CreateReadTxn();
    int64_t start_vid;
    {
        auto fd = lgraph_api::FieldData::Int64(person_id);
        auto iit = txn.GetVertexIndexIterator(PERSON, PERSON_ID, fd, fd);
        start_vid = iit.GetVid();
    }
    auto place = txn.GetVertexIterator();
    std::vector<std::pair<int64_t, int8_t> > message_vids;
    tsl::hopscotch_set<int64_t> xy_person_vids;
    int64_t country_x_vid = -1;
    {
        auto fd = lgraph_api::FieldData::String(country_x_name);
        auto iit = txn.GetVertexIndexIterator(PLACE, PLACE_NAME, fd, fd);
        while (iit.IsValid()) {
            country_x_vid = iit.GetVid();
            place.Goto(country_x_vid);
            if (place[PLACE_TYPE].string() == "country") break;
            iit.Next();
        }
        std::vector<int64_t> city_vids;
        for (auto country_cities = lgraph_api::LabeledInEdgeIterator(txn, country_x_vid, ISPARTOF);
             country_cities.IsValid(); country_cities.Next()) {
            int64_t city_vid = country_cities.GetSrc();
            city_vids.emplace_back(city_vid);
        }
        std::sort(city_vids.begin(), city_vids.end());
        for (int64_t city_vid : city_vids) {
            for (auto city_persons = lgraph_api::LabeledInEdgeIterator(txn, city_vid, PERSONISLOCATEDIN);
                 city_persons.IsValid(); city_persons.Next()) {
                int64_t person_vid = city_persons.GetSrc();
                xy_person_vids.emplace(person_vid);
            }
        }
        for (auto country_posts = lgraph_api::LabeledInEdgeIterator(txn, country_x_vid, POSTISLOCATEDIN);
             country_posts.IsValid(); country_posts.Next()) {
            int64_t creation_date = country_posts[POSTISLOCATEDIN_CREATIONDATE].integer();
            if (creation_date < start_date || creation_date >= end_date) continue;
            int64_t post_vid = country_posts.GetSrc();
            message_vids.emplace_back(post_vid, -1);
        }
        for (auto country_comments = lgraph_api::LabeledInEdgeIterator(txn, country_x_vid, COMMENTISLOCATEDIN);
             country_comments.IsValid(); country_comments.Next()) {
            int64_t creation_date = country_comments[COMMENTISLOCATEDIN_CREATIONDATE].integer();
            if (creation_date < start_date || creation_date >= end_date) continue;
            int64_t comment_vid = country_comments.GetSrc();
            message_vids.emplace_back(comment_vid, -2);
        }
    }
    int64_t country_y_vid = -1;
    {
        auto fd = lgraph_api::FieldData::String(country_y_name);
        auto iit = txn.GetVertexIndexIterator(PLACE, PLACE_NAME, fd, fd);
        while (iit.IsValid()) {
            country_y_vid = iit.GetVid();
            place.Goto(country_y_vid);
            if (place[PLACE_TYPE].string() == "country") break;
            iit.Next();
        }
        std::vector<int64_t> city_vids;
        for (auto country_cities = lgraph_api::LabeledInEdgeIterator(txn, country_y_vid, ISPARTOF);
             country_cities.IsValid(); country_cities.Next()) {
            int64_t city_vid = country_cities.GetSrc();
            city_vids.emplace_back(city_vid);
        }
        std::sort(city_vids.begin(), city_vids.end());
        for (int64_t city_vid : city_vids) {
            for (auto city_persons = lgraph_api::LabeledInEdgeIterator(txn, city_vid, PERSONISLOCATEDIN);
                 city_persons.IsValid(); city_persons.Next()) {
                int64_t person_vid = city_persons.GetSrc();
                xy_person_vids.emplace(person_vid);
            }
        }
        for (auto country_posts = lgraph_api::LabeledInEdgeIterator(txn, country_y_vid, POSTISLOCATEDIN);
             country_posts.IsValid(); country_posts.Next()) {
            int64_t creation_date = country_posts[POSTISLOCATEDIN_CREATIONDATE].integer();
            if (creation_date < start_date || creation_date >= end_date) continue;
            int64_t post_vid = country_posts.GetSrc();
            message_vids.emplace_back(post_vid, +1);
        }
        for (auto country_comments = lgraph_api::LabeledInEdgeIterator(txn, country_y_vid, COMMENTISLOCATEDIN);
             country_comments.IsValid(); country_comments.Next()) {
            int64_t creation_date = country_comments[COMMENTISLOCATEDIN_CREATIONDATE].integer();
            if (creation_date < start_date || creation_date >= end_date) continue;
            int64_t comment_vid = country_comments.GetSrc();
            message_vids.emplace_back(comment_vid, +2);
        }
    }

    tsl::hopscotch_set<int64_t> visited({start_vid});
    tsl::hopscotch_map<int64_t, std::tuple<int32_t, int32_t> > person_info;
    auto person = txn.GetVertexIterator();
    std::vector<int64_t> curr_frontier({start_vid});
    for (int hop = 0; hop < 2; hop++) {
        std::vector<int64_t> next_frontier;
        for (auto vid : curr_frontier) {
            person.Goto(vid);
            for (auto person_friends = lgraph_api::LabeledOutEdgeIterator(person, KNOWS); person_friends.IsValid();
                 person_friends.Next()) {
                int64_t friend_vid = person_friends.GetDst();
                if (visited.find(friend_vid) == visited.end()) {
                    visited.emplace(friend_vid);
                    next_frontier.emplace_back(friend_vid);
                    if (xy_person_vids.find(friend_vid) == xy_person_vids.end()) {
                        person_info.emplace(friend_vid, std::make_tuple(0, 0));
                    }
                }
            }
            for (auto person_friends = lgraph_api::LabeledInEdgeIterator(person, KNOWS); person_friends.IsValid();
                 person_friends.Next()) {
                int64_t friend_vid = person_friends.GetSrc();
                if (visited.find(friend_vid) == visited.end()) {
                    visited.emplace(friend_vid);
                    next_frontier.emplace_back(friend_vid);
                    if (xy_person_vids.find(friend_vid) == xy_person_vids.end()) {
                        person_info.emplace(friend_vid, std::make_tuple(0, 0));
                    }
                }
            }
        }
        std::sort(next_frontier.begin(), next_frontier.end());
        curr_frontier.swap(next_frontier);
    }

    std::sort(message_vids.begin(), message_vids.end());
    auto message = txn.GetVertexIterator();
    for (auto& p : message_vids) {
        int64_t message_vid = p.first;
        int8_t message_type = p.second;
        message.Goto(message_vid);
        switch (message_type) {
            case -1: {
                int64_t creator_vid = message[POST_CREATOR].integer();
                auto it = person_info.find(creator_vid);
                if (it != person_info.end()) {
                    std::get<0>(it.value())++;
                }
                break;
            }
            case -2: {
                int64_t creator_vid = message[COMMENT_CREATOR].integer();
                auto it = person_info.find(creator_vid);
                if (it != person_info.end()) {
                    std::get<0>(it.value())++;
                }
                break;
            }
            case +1: {
                int64_t creator_vid = message[POST_CREATOR].integer();
                auto it = person_info.find(creator_vid);
                if (it != person_info.end()) {
                    std::get<1>(it.value())++;
                }
                break;
            }
            case +2: {
                int64_t creator_vid = message[COMMENT_CREATOR].integer();
                auto it = person_info.find(creator_vid);
                if (it != person_info.end()) {
                    std::get<1>(it.value())++;
                }
                break;
            }
            default:;
        }
    }

    std::set<std::tuple<int32_t, int64_t, int64_t> > candidates;
    for (auto it = person_info.begin(); it != person_info.end(); it++) {
        int32_t x_count, y_count;
        std::tie(x_count, y_count) = it->second;
        if (x_count == 0 || y_count == 0) continue;
        int64_t person_vid = it->first;
        int64_t person_id = -1;
        if (candidates.size() >= limit_results) {
            auto& candidate = *candidates.rbegin();
            if (0 - x_count - y_count > std::get<0>(candidate)) continue;
            if (0 - x_count - y_count == std::get<0>(candidate)) {
                person.Goto(person_vid);
                person_id = person[PERSON_ID].integer();
                if (person_id > std::get<1>(candidate)) continue;
            }
        }
        if (person_id == -1) {
            person.Goto(person_vid);
            person_id = person[PERSON_ID].integer();
        }
        candidates.emplace(0 - x_count - y_count, person_id, person_vid);
        if (candidates.size() > limit_results) {
            candidates.erase(--candidates.end());
        }
    }
    // output results
    std::stringstream oss;
    WriteInt16(oss, candidates.size());
    for (auto& tup : candidates) {
        WriteInt64(oss, std::get<1>(tup));
        person.Goto(std::get<2>(tup));
        WriteString(oss, person[PERSON_FIRSTNAME].string());
        WriteString(oss, person[PERSON_LASTNAME].string());
        int32_t x_count, y_count;
        std::tie(x_count, y_count) = person_info.find(std::get<2>(tup))->second;
        WriteInt32(oss, x_count);
        WriteInt32(oss, y_count);
        WriteInt32(oss, 0 - std::get<0>(tup));
    }
    response = oss.str();
    return true;
}
