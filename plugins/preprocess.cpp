#include "lgraph/olap_on_db.h"

#include "snb_constants.h"
#include "snb_common.h"

#include <unordered_map>
#include <tuple>
#include <functional>
#include <iostream>

using namespace lgraph_api;

void ConvertForeignKeyToVid(Transaction& txn, VertexIterator& vit, size_t fid, size_t foreign_lid, size_t foreign_fid) {
    // static std::mutex mutex;
    try {
        size_t id = vit[fid].integer();
        auto foreign_vit = txn.GetVertexByUniqueIndex(foreign_lid, foreign_fid, FieldData::Int64(id));
        vit.SetField(fid, FieldData::Int64(foreign_vit.GetId()));
    } catch (std::exception& e) {
        // mutex.lock();
        // std::cerr << vit.GetLabel() << " " << fid << " " << foreign_lid << " " << foreign_fid << std::endl;
        // mutex.unlock();
    }
}

void ConvertForeignKeys(GraphDB& db) {
    double conversion_time = - omp_get_wtime();

    auto worker = olap::Worker::SharedWorker();

    size_t num_vertices = db.EstimateNumVertices();

    worker->Delegate([&](){
        constexpr size_t chunk_size = 64;
        size_t cursor = 0;
        #pragma omp parallel
        {
            while (true) {
                size_t chunk_begin = __sync_fetch_and_add(&cursor, chunk_size);
                if (chunk_begin >= num_vertices) break;
                size_t chunk_end = chunk_begin + chunk_size;
                auto txn = db.CreateWriteTxn(true);
                auto vit = txn.GetVertexIterator(chunk_begin, true);
                while (vit.IsValid()) {
                    size_t vid = vit.GetId();
                    if (vid >= chunk_end) break;
                    size_t lid = vit.GetLabelId();
                    switch (lid) {
                        case COMMENT: {
                            ConvertForeignKeyToVid(txn, vit, COMMENT_CREATOR, PERSON, PERSON_ID);
                            ConvertForeignKeyToVid(txn, vit, COMMENT_PLACE, PLACE, PLACE_ID);
                            if (!vit[COMMENT_REPLYOFPOST].is_null()) {
                                ConvertForeignKeyToVid(txn, vit, COMMENT_REPLYOFPOST, POST, POST_ID);
                            } else {
                                ConvertForeignKeyToVid(txn, vit, COMMENT_REPLYOFCOMMENT, COMMENT, COMMENT_ID);
                            }
                            break;
                        }
                        case FORUM: {
                            ConvertForeignKeyToVid(txn, vit, FORUM_MODERATOR, PERSON, PERSON_ID);
                            break;
                        }
                        case ORGANISATION: {
                            ConvertForeignKeyToVid(txn, vit, ORGANISATION_PLACE, PLACE, PLACE_ID);
                            break;
                        }
                        case PERSON: {
                            ConvertForeignKeyToVid(txn, vit, PERSON_PLACE, PLACE, PLACE_ID);
                            break;
                        }
                        case PLACE: {
                            ConvertForeignKeyToVid(txn, vit, PLACE_ISPARTOF, PLACE, PLACE_ID);
                            break;
                        }
                        case POST: {
                            ConvertForeignKeyToVid(txn, vit, POST_CREATOR, PERSON, PERSON_ID);
                            ConvertForeignKeyToVid(txn, vit, POST_PLACE, PLACE, PLACE_ID);
                            ConvertForeignKeyToVid(txn, vit, POST_CONTAINER, FORUM, FORUM_ID);
                            break;
                        }
                        case TAG: {
                            ConvertForeignKeyToVid(txn, vit, TAG_HASTYPE, TAGCLASS, TAGCLASS_ID);
                            break;
                        }
                        case TAGCLASS: {
                            ConvertForeignKeyToVid(txn, vit, TAGCLASS_ISSUBCLASSOF, TAGCLASS, TAGCLASS_ID);
                            break;
                        }
                        default: {
                            throw std::runtime_error("Unknown vertex label");
                            break;
                        }
                    }
                    vit.Next();
                }
                txn.Commit();
            }
        }
    });

    conversion_time += omp_get_wtime();

    std::cout << conversion_time << std::endl;
}

void AddIndices(GraphDB& db) {
    double exec_time = - omp_get_wtime();

    std::vector< std::tuple<std::string, std::string, bool> > index_list{
        std::make_tuple("Place", "name", false),
        std::make_tuple("Tag", "name", true),
        std::make_tuple("Tagclass", "name", true)
    };
    for (auto& tup : index_list) {
        auto label = std::get<0>(tup);
        auto field = std::get<1>(tup);
        db.AddVertexIndex(label, field, std::get<2>(tup));
    }

    exec_time += omp_get_wtime();

    std::cout << exec_time << std::endl;
}

void FillInFields(GraphDB& db) {
    double exec_time = - omp_get_wtime();

    auto worker = lgraph_api::olap::Worker::SharedWorker();

    size_t num_vertices = db.EstimateNumVertices();

    std::mutex mutex;

    std::vector< std::tuple<int64_t, int64_t, int32_t> > forum_hasmember_person_edges;
    std::vector< std::tuple<int64_t, int64_t, double> > person_knows_person_edges;

    worker->Delegate([&](){
        constexpr size_t threshold = 256;
        constexpr size_t chunk_size = 64;
        size_t cursor = 0;
        #pragma omp parallel
        {
            std::vector< std::tuple<int64_t, int64_t, int32_t> > forum_hasmember_person_edges_;
            std::vector< std::tuple<int64_t, int64_t, double> > person_knows_person_edges_;
            auto txn = db.CreateReadTxn();
            while (true) {
                size_t chunk_begin = __sync_fetch_and_add(&cursor, chunk_size);
                if (chunk_begin >= num_vertices) break;
                size_t chunk_end = chunk_begin + chunk_size;
                if (chunk_end > num_vertices) chunk_end = num_vertices;
                for (size_t vid = chunk_begin; vid < chunk_end; vid ++) {
                    auto vit = txn.GetVertexIterator(vid);
                    size_t lid = vit.GetLabelId();
                    switch (lid) {
                        case PERSON: {
                            auto& person = vit;
                            std::unordered_map< int64_t, int32_t > post_count;
                            for (auto person_posts = lgraph_api::LabeledInEdgeIterator(person, POSTHASCREATOR); person_posts.IsValid(); person_posts.Next()) {
                                auto post = txn.GetVertexIterator(person_posts.GetSrc());
                                int64_t forum_vid = post[POST_CONTAINER].integer();
                                auto it = post_count.find(forum_vid);
                                if (it != post_count.end()) {
                                    it->second += 1;
                                } else {
                                    post_count.emplace(forum_vid, 1);
                                }
                            }
                            std::unordered_map< int64_t, double > weight_info;
                            for (auto person_comments = lgraph_api::LabeledInEdgeIterator(person, COMMENTHASCREATOR); person_comments.IsValid(); person_comments.Next()) {
                                auto comment = txn.GetVertexIterator(person_comments.GetSrc());
                                auto fd = comment[COMMENT_REPLYOFPOST];
                                if (!fd.is_null()) {
                                    int64_t post_vid = fd.integer();
                                    auto& post = comment;
                                    post.Goto(post_vid);
                                    int64_t person_vid = post[POST_CREATOR].integer();
                                    auto it = weight_info.find(person_vid);
                                    if (it != weight_info.end()) {
                                        it->second += 1.0;
                                    } else {
                                        weight_info.emplace(person_vid, 1.0);
                                    }
                                } else {
                                    int64_t comment_vid = comment[COMMENT_REPLYOFCOMMENT].integer();
                                    comment.Goto(comment_vid);
                                    int64_t person_vid = comment[COMMENT_CREATOR].integer();
                                    auto it = weight_info.find(person_vid);
                                    if (it != weight_info.end()) {
                                        it->second += 0.5;
                                    } else {
                                        weight_info.emplace(person_vid, 0.5);
                                    }
                                }
                            }
                            for (auto person_forums = lgraph_api::LabeledInEdgeIterator(person, HASMEMBER); person_forums.IsValid(); person_forums.Next()) {
                                int64_t forum_vid = person_forums.GetSrc();
                                auto it = post_count.find(forum_vid);
                                if (it == post_count.end()) continue;
                                forum_hasmember_person_edges_.emplace_back(forum_vid, vid, it->second);
                                if (forum_hasmember_person_edges_.size() >= threshold) {
                                    mutex.lock();
                                    forum_hasmember_person_edges.insert(forum_hasmember_person_edges.end(), forum_hasmember_person_edges_.begin(), forum_hasmember_person_edges_.end());
                                    mutex.unlock();
                                    forum_hasmember_person_edges_.clear();
                                }
                            }
                            mutex.lock();
                            forum_hasmember_person_edges.insert(forum_hasmember_person_edges.end(), forum_hasmember_person_edges_.begin(), forum_hasmember_person_edges_.end());
                            mutex.unlock();
                            forum_hasmember_person_edges_.clear();
                            for (auto it = weight_info.begin(); it != weight_info.end(); it ++) {
                                int64_t person_vid = it->first;
                                double weight = it->second;
                                auto oeit = person.GetOutEdgeIterator(lgraph_api::EdgeUid(vid, person_vid, KNOWS, 0, 0));
                                if (oeit.IsValid()) {
                                    person_knows_person_edges_.emplace_back(vid, person_vid, weight);
                                }
                                auto ieit = person.GetInEdgeIterator(lgraph_api::EdgeUid(person_vid, vid, KNOWS, 0, 0));
                                if (ieit.IsValid()) {
                                    person_knows_person_edges_.emplace_back(person_vid, vid, weight);
                                }
                                if (person_knows_person_edges_.size() >= threshold) {
                                    mutex.lock();
                                    person_knows_person_edges.insert(person_knows_person_edges.end(), person_knows_person_edges_.begin(), person_knows_person_edges_.end());
                                    mutex.unlock();
                                    person_knows_person_edges_.clear();
                                }
                            }
                            mutex.lock();
                            person_knows_person_edges.insert(person_knows_person_edges.end(), person_knows_person_edges_.begin(), person_knows_person_edges_.end());
                            mutex.unlock();
                            person_knows_person_edges_.clear();
                            break;
                        }
                        default: {
                            break;
                        }
                    }
                }
            }
        }
    });

    constexpr size_t batch_size = 1024;
    auto txn = db.CreateWriteTxn();
    std::sort(forum_hasmember_person_edges.begin(), forum_hasmember_person_edges.end());
    for (size_t i = 0; i < forum_hasmember_person_edges.size(); i ++) {
        int64_t src, dst;
        int32_t num_posts;
        std::tie(src, dst, num_posts) = forum_hasmember_person_edges[i];
        auto eit = txn.GetOutEdgeIterator(src, dst, HASMEMBER);
        assert(eit.IsValid());
        eit.SetField(HASMEMBER_NUMPOSTS, lgraph_api::FieldData::Int32(num_posts));
        if (i % batch_size == batch_size - 1) {
            txn.Commit();
            txn = db.CreateWriteTxn();
        }
    }
    if (txn.IsValid()) txn.Commit();
    txn = db.CreateWriteTxn();
    std::sort(person_knows_person_edges.begin(), person_knows_person_edges.end());
    for (size_t i = 0; i < person_knows_person_edges.size(); i ++) {
        int64_t src, dst;
        double weight;
        std::tie(src, dst, weight) = person_knows_person_edges[i];
        auto eit = txn.GetOutEdgeIterator(lgraph_api::EdgeUid(src, dst, KNOWS, 0, 0));
        eit.SetField(KNOWS_WEIGHT, lgraph_api::FieldData::Double(weight + eit[KNOWS_WEIGHT].real()));
        if (i % batch_size == batch_size - 1) {
            txn.Commit();
            txn = db.CreateWriteTxn();
        }
    }
    if (txn.IsValid()) txn.Commit();

    exec_time += omp_get_wtime();

    std::cout << exec_time << std::endl;
}

int main(int argc, char** argv) {
    std::string db_path(argv[1]);

    lgraph_api::Galaxy galaxy(db_path, "admin", "73@TuGraph", true, false);
    lgraph_api::GraphDB db = galaxy.OpenGraph("default");

    ConvertForeignKeys(db);
    AddIndices(db);
    FillInFields(db);

    return 0;
}
