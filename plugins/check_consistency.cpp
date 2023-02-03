#include "lgraph/olap_on_db.h"

#include "snb_constants.h"
#include "snb_common.h"

#include <unordered_map>
#include <tuple>
#include <functional>
#include <iostream>

using namespace lgraph_api;

void CheckConsistency(GraphDB& db) {
    double exec_time = - omp_get_wtime();

    auto worker = lgraph_api::olap::Worker::SharedWorker();

    size_t num_vertices = db.EstimateNumVertices();

    std::mutex mutex;

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
                            std::unordered_map< int64_t, double > weight_info;
                            for (auto person_posts = lgraph_api::LabeledInEdgeIterator(person, POSTHASCREATOR); person_posts.IsValid(); person_posts.Next()) {
                                auto post = txn.GetVertexIterator(person_posts.GetSrc());
                                for (auto replies = lgraph_api::LabeledInEdgeIterator(post, REPLYOF); replies.IsValid(); replies.Next()) {
                                    auto comment = txn.GetVertexIterator(replies.GetSrc());
                                    int64_t friend_vid = comment[COMMENT_CREATOR].integer();
                                    auto it = weight_info.find(friend_vid);
                                    if (it != weight_info.end()) {
                                        it->second += 1.0;
                                    } else {
                                        weight_info.emplace(friend_vid, 1.0);
                                    }
                                }
                                int64_t forum_vid = post[POST_CONTAINER].integer();
                                auto it = post_count.find(forum_vid);
                                if (it != post_count.end()) {
                                    it->second += 1;
                                } else {
                                    post_count.emplace(forum_vid, 1);
                                }
                            }
                            for (auto person_comments = lgraph_api::LabeledInEdgeIterator(person, COMMENTHASCREATOR); person_comments.IsValid(); person_comments.Next()) {
                                auto comment = txn.GetVertexIterator(person_comments.GetSrc());
                                for (auto replies = lgraph_api::LabeledInEdgeIterator(comment, REPLYOF); replies.IsValid(); replies.Next()) {
                                    auto comment = txn.GetVertexIterator(replies.GetSrc());
                                    int64_t friend_vid = comment[COMMENT_CREATOR].integer();
                                    auto it = weight_info.find(friend_vid);
                                    if (it != weight_info.end()) {
                                        it->second += 0.5;
                                    } else {
                                        weight_info.emplace(friend_vid, 0.5);
                                    }
                                }
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
                                int32_t count = 0;
                                auto it = post_count.find(forum_vid);
                                if (it != post_count.end()) count = it->second;
                                if (count != person_forums[HASMEMBER_NUMPOSTS].integer()) {
                                    mutex.lock();
                                    printf("%lu -[hasMember]-> %lu .numPosts expects %d but gets %ld\n", forum_vid, vid, count, person_forums[HASMEMBER_NUMPOSTS].integer());
                                    mutex.unlock();
                                }
                            }
                            for (auto person_friends = lgraph_api::LabeledOutEdgeIterator(person, KNOWS); person_friends.IsValid(); person_friends.Next()) {
                                int64_t friend_vid = person_friends.GetDst();
                                double weight = 0;
                                auto it = weight_info.find(friend_vid);
                                if (it != weight_info.end()) weight = it->second;
                                if (weight != person_friends[KNOWS_WEIGHT].real()) {
                                    mutex.lock();
                                    printf("%lu -[knows]- %lu .weight expects %lf but gets %lf\n", vid, friend_vid, weight, person_friends[KNOWS_WEIGHT].real());
                                    mutex.unlock();
                                }
                            }
                            for (auto person_friends = lgraph_api::LabeledInEdgeIterator(person, KNOWS); person_friends.IsValid(); person_friends.Next()) {
                                int64_t friend_vid = person_friends.GetSrc();
                                double weight = 0;
                                auto it = weight_info.find(friend_vid);
                                if (it != weight_info.end()) weight = it->second;
                                if (weight != person_friends[KNOWS_WEIGHT].real()) {
                                    mutex.lock();
                                    printf("%lu -[knows]- %lu .weight expects %lf but gets %lf\n", friend_vid, vid, weight, person_friends[KNOWS_WEIGHT].real());
                                    mutex.unlock();
                                }
                            }
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

    exec_time += omp_get_wtime();

    std::cout << exec_time << std::endl;
}

int main(int argc, char** argv) {
    std::string db_path(argv[1]);

    lgraph_api::Galaxy galaxy(db_path, "admin", "73@TuGraph", true, false);
    lgraph_api::GraphDB db = galaxy.OpenGraph("default");

    CheckConsistency(db);

    return 0;
}
