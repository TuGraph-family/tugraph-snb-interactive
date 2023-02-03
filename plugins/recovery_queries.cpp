#include "lgraph/olap_on_db.h"

#include "snb_common.h"
#include "snb_constants.h"

#include <iostream>

using namespace lgraph_api;

void CheckLastUpdates(GraphDB &db) {

  auto u1_personId = 35184372098657;

  auto u2_personId = 2199023315315;
  auto u2_postId = 35184483345996;

  auto u3_personId = 35184372272544;
  auto u3_commentId = 35184407206205;

  auto u4_forumId = 35184377414070;

  auto u5_forumId = 8796105651235;
  auto u5_personId = 21990232735623;

  auto u6_postId = 35184488680471;

  auto u7_commentId = 35184509037491;

  auto u8_personId1 = 32985349008011;
  auto u8_personId2 = 35184372223886;

  auto txn = db.CreateReadTxn();

  std::string firstName = "NOT FOUND";
  int64_t postLikeDate = -1;
  int64_t commentLikeDate = -1;
  std::string forumTitle = "NOT FOUND";
  std::string language = "NOT FOUND";
  std::string browser = "NOT FOUND";
  bool foundHasMember = false;
  bool foundKnows = false;

  for (auto vit = txn.GetVertexIterator(); vit.IsValid(); vit.Next()) {
    if (vit["id"].AsInt64() == u1_personId) {
      // Update 1: find the new person
      firstName = vit["firstName"].AsString();
    } else if (vit["id"].AsInt64() == u2_personId) {
      // Update 2: find the new like of post
      for (auto person_likes = lgraph_api::LabeledOutEdgeIterator(vit, LIKES);
           person_likes.IsValid(); person_likes.Next()) {
        auto vit2 = txn.GetVertexIterator(person_likes.GetDst());
        if (vit2["id"].AsInt64() == u2_postId) {
          postLikeDate = person_likes["creationDate"].AsInt64();
        }
      }
    } else if (vit["id"].AsInt64() == u3_personId) {
      // Update 3: find the new like of comment
      for (auto person_likes = lgraph_api::LabeledOutEdgeIterator(vit, LIKES);
           person_likes.IsValid(); person_likes.Next()) {
        auto vit2 = txn.GetVertexIterator(person_likes.GetDst());
        if (vit2["id"].AsInt64() == u3_commentId) {
          commentLikeDate = person_likes["creationDate"].AsInt64();
        }
      }
    } else if (vit["id"].AsInt64() == u4_forumId) {
      // Update 4: find the new forum
      forumTitle = vit["title"].AsString();
    } else if (vit["id"].AsInt64() == u5_forumId) {
      // Update 5: find the new membership edge
      for (auto forum_persons =
               lgraph_api::LabeledOutEdgeIterator(vit, HASMEMBER);
           forum_persons.IsValid(); forum_persons.Next()) {
        auto it2 = txn.GetVertexIterator(forum_persons.GetDst());
        if (it2["id"].AsInt64() == u5_personId) {
          foundHasMember = true;
        }
      }
    } else if (vit["id"].AsInt64() == u6_postId) {
      // Update 6: find the new post
      language = vit["language"].AsString();
    } else if (vit["id"].AsInt64() == u7_commentId) {
      // Update 7: find the new comment
      browser = vit["browserUsed"].AsString();
    } else if (vit["id"].AsInt64() == u8_personId1) {
      // Update 8: find the friendship
      for (auto person_knows = lgraph_api::LabeledInEdgeIterator(vit, KNOWS);
           person_knows.IsValid(); person_knows.Next()) {
        auto it2 = txn.GetVertexIterator(person_knows.GetSrc());
        if (it2["id"].AsInt64() == u8_personId2) {
          foundKnows = true;
        }
      }
    } else if (vit["id"].AsInt64() == u8_personId2) {
      // Update 8: find the friendship (check opposite direction)
      for (auto person_knows = lgraph_api::LabeledInEdgeIterator(vit, KNOWS);
           person_knows.IsValid(); person_knows.Next()) {
        auto it2 = txn.GetVertexIterator(person_knows.GetSrc());
        if (it2["id"].AsInt64() == u8_personId1) {
          foundKnows = true;
        }
      }
    }
  }

  std::cout << "Q1: " << firstName << std::endl;
  std::cout << "Q2: " << postLikeDate << std::endl;
  std::cout << "Q3: " << commentLikeDate << std::endl;
  std::cout << "Q4: " << forumTitle << std::endl;
  std::cout << "Q5: " << (foundHasMember ? "FOUND" : "NOT FOUND") << std::endl;
  std::cout << "Q6: " << language << std::endl;
  std::cout << "Q7: " << browser << std::endl;
  std::cout << "Q8: " << (foundKnows ? "FOUND" : "NOT FOUND") << std::endl;
}

int main(int argc, char **argv) {
  std::string db_path(argv[1]);

  lgraph_api::Galaxy galaxy(db_path, "admin", "73@TuGraph", true, false);
  lgraph_api::GraphDB db = galaxy.OpenGraph("default");

  CheckLastUpdates(db);

  return 0;
}
