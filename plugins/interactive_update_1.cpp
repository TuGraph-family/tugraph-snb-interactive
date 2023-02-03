#include "lgraph/lgraph.h"
#include "snb_common.h"
#include "snb_constants.h"

extern "C" bool Process(lgraph_api::GraphDB& db, const std::string& request, std::string& response) {
    std::stringstream oss;
    try {
        auto txn = db.CreateReadTxn();
        std::string input = lgraph_api::base64::Decode(request);
        std::stringstream iss(input);
        int64_t person_id = ReadInt64(iss);
        std::string person_first_name = ReadString(iss);
        std::string person_last_name = ReadString(iss);
        std::string gender = ReadString(iss);
        int64_t birthday = ReadInt64(iss);
        int64_t creation_date = ReadInt64(iss);
        std::string location_ip = ReadString(iss);
        std::string browser_used = ReadString(iss);
        int64_t city_id = ReadInt64(iss);
        int64_t place_vid;
        {
            auto fd = lgraph_api::FieldData::Int64(city_id);
            auto iit = txn.GetVertexIndexIterator(PLACE, PLACE_ID, fd, fd);
            place_vid = iit.GetVid();
        }
        std::string speaks = ReadString(iss);
        std::string email = ReadString(iss);
        int16_t num_items;
        std::vector<int64_t> tag_vids;
        num_items = ReadInt16(iss);
        for (int i = 0; i < num_items; i++) {
            int64_t tag_id = ReadInt64(iss);
            auto fd = lgraph_api::FieldData::Int64(tag_id);
            auto iit = txn.GetVertexIndexIterator(TAG, TAG_ID, fd, fd);
            int64_t tag_vid = iit.GetVid();
            tag_vids.emplace_back(tag_vid);
        }
        std::vector<std::pair<int64_t, int32_t> > study_at;
        num_items = ReadInt16(iss);
        for (int i = 0; i < num_items; i++) {
            int64_t university_id = ReadInt64(iss);
            auto fd = lgraph_api::FieldData::Int64(university_id);
            auto iit = txn.GetVertexIndexIterator(ORGANISATION, ORGANISATION_ID, fd, fd);
            int64_t vid = iit.GetVid();
            study_at.emplace_back(vid, ReadInt32(iss));
        }
        std::vector<std::pair<int64_t, int32_t> > work_at;
        num_items = ReadInt16(iss);
        for (int i = 0; i < num_items; i++) {
            int64_t company_id = ReadInt64(iss);
            auto fd = lgraph_api::FieldData::Int64(company_id);
            auto iit = txn.GetVertexIndexIterator(ORGANISATION, ORGANISATION_ID, fd, fd);
            int64_t vid = iit.GetVid();
            work_at.emplace_back(vid, ReadInt32(iss));
        }
        bool committed = false;
        try {
            txn.Abort();
            txn = db.CreateWriteTxn();
            int64_t person_vid = txn.AddVertex(
                PERSON,
                {PERSON_ID, PERSON_FIRSTNAME, PERSON_LASTNAME, PERSON_GENDER, PERSON_BIRTHDAY, PERSON_CREATIONDATE,
                 PERSON_LOCATIONIP, PERSON_BROWSERUSED, PERSON_PLACE, PERSON_SPEAKS, PERSON_EMAIL},
                {lgraph_api::FieldData::Int64(person_id), lgraph_api::FieldData::String(person_first_name),
                 lgraph_api::FieldData::String(person_last_name), lgraph_api::FieldData::String(gender),
                 lgraph_api::FieldData::Int64(birthday), lgraph_api::FieldData::Int64(creation_date),
                 lgraph_api::FieldData::String(location_ip), lgraph_api::FieldData::String(browser_used),
                 lgraph_api::FieldData::Int64(place_vid), lgraph_api::FieldData::String(speaks),
                 lgraph_api::FieldData::String(email)});
            txn.AddEdge(person_vid, place_vid, PERSONISLOCATEDIN, {}, {});
            for (auto& tag_vid : tag_vids) {
                txn.AddEdge(person_vid, tag_vid, HASINTEREST, {}, {});
            }
            for (auto& vid_year : study_at) {
                txn.AddEdge(person_vid, vid_year.first, STUDYAT, {STUDYAT_CLASSYEAR},
                            {lgraph_api::FieldData::Int32(vid_year.second)});
            }
            for (auto& vid_year : work_at) {
                txn.AddEdge(person_vid, vid_year.first, WORKAT, {WORKAT_WORKFROM},
                            {lgraph_api::FieldData::Int32(vid_year.second)});
            }
            txn.Commit();
            committed = true;
        } catch (std::exception& e) {
            std::cout << "interactive_update_1 exception: " << e.what() << std::endl;
        }
        if (committed) {
            WriteInt16(oss, 0);
        } else {
            std::cout << "interactive_update_1 failed" << std::endl;
            WriteInt16(oss, 1);
        }
    } catch (std::exception& e) {
        std::cout << "interactive_update_1 exception: " << e.what() << std::endl;
        std::cout << "interactive_update_1 failed" << std::endl;
        WriteInt16(oss, 1);
    }
    response = oss.str();
    return true;
}
