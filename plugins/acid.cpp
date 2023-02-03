#include "lgraph/lgraph.h"

#include <omp.h>

#include <iostream>
#include <tuple>
#include <thread>
#include <future>
#include <chrono>
#include <random>

bool optimistic = false;

// common utilities

void Initialize(auto& db) {
    db.DropAllData();
    bool ok;
    ok = db.AddVertexLabel("Forum", {
        {"id", lgraph_api::FieldType::INT64, false}
    }, "id");
    if (!ok) throw std::runtime_error("failed to register label Forum");
    ok = db.AddVertexLabel("Person", {
        {"id", lgraph_api::FieldType::INT64, false},
        {"numFriends", lgraph_api::FieldType::INT64, true},
        {"value", lgraph_api::FieldType::INT64, true},
        {"version", lgraph_api::FieldType::INT64, true},
        {"versionHistory", lgraph_api::FieldType::STRING, true},
        {"name", lgraph_api::FieldType::STRING, true},
        {"emails", lgraph_api::FieldType::STRING, true}
    }, "id");
    if (!ok) throw std::runtime_error("failed to register label Person");
    ok = db.AddVertexLabel("Post", {
        {"id", lgraph_api::FieldType::INT64, false}
    }, "id");
    if (!ok) throw std::runtime_error("failed to register label Post");
    ok = db.AddEdgeLabel("hasModerator", {

    });
    if (!ok) throw std::runtime_error("failed to register label hasModerator");
    ok = db.AddEdgeLabel("knows", {
        {"since", lgraph_api::FieldType::INT64, true},
        {"versionHistory", lgraph_api::FieldType::STRING, true}
    });
    if (!ok) throw std::runtime_error("failed to register label knows");
    ok = db.AddEdgeLabel("likes", {

    });
    if (!ok) throw std::runtime_error("failed to register label likes");
}

void AppendStringToField(auto& vit, const std::string& field, const std::string& s) {
    if (vit[field].IsNull()) {
        vit.SetField(field, lgraph_api::FieldData::String(s));
    } else {
        vit.SetField(field, lgraph_api::FieldData::String(vit[field].AsString() + ";" + s));
    }
}

int64_t CountItemsInField(auto& vit, const std::string& field) {
    if (vit[field].IsNull()) return 0;
    int64_t num_items = 1;
    auto s = vit[field].AsString();
    for (auto c : s) {
        if (c == ';') num_items ++;
    }
    return num_items;
}

// Atomicity

void AtomicityInit(auto& db) {
    auto txn = db.CreateWriteTxn(optimistic);
    txn.AddVertex(
        "Person",
        {
            "id", "name", "emails"
        },
        {
            lgraph_api::FieldData::Int64(1),
            lgraph_api::FieldData::String("Alice"),
            lgraph_api::FieldData::String("alice@aol.com")
        }
    );
    txn.AddVertex(
        "Person",
        {
            "id", "name", "emails"
        },
        {
            lgraph_api::FieldData::Int64(2),
            lgraph_api::FieldData::String("Bob"),
            lgraph_api::FieldData::String("bob@hotmail.com;bobby@yahoo.com")
        }
    );
    txn.Commit();
}

bool AtomicityC(auto& db, int64_t person1_id, int64_t person2_id, const std::string& new_email, int64_t since) {
    auto txn = db.CreateWriteTxn(optimistic);
    auto vit = txn.GetVertexIterator();
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Person" && vit["id"].AsInt64() == person1_id) break;
    }
    auto p1_vid = vit.GetId();
    AppendStringToField(vit, "emails", new_email);
    auto p2_vid = txn.AddVertex(
        "Person",
        {"id"},
        {lgraph_api::FieldData::Int64(person2_id)}
    );
    txn.AddEdge(
        p1_vid, p2_vid, "knows",
        {"since"},
        {lgraph_api::FieldData::Int64(since)}
    );
    txn.Commit();
    return true;
}

bool AtomicityRB(auto& db, int64_t person1_id, int64_t person2_id, const std::string& new_email, int64_t since) {
    auto txn = db.CreateWriteTxn(optimistic);
    auto vit = txn.GetVertexIterator();
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Person" && vit["id"].AsInt64() == person1_id) break;
    }
    auto p1_vid = vit.GetId();
    AppendStringToField(vit, "emails", new_email);
    vit.Goto(0, true);
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Person" && vit["id"].AsInt64() == person2_id) {
            txn.Abort();
            return false;
        }
    }
    auto p2_vid = txn.AddVertex(
        "Person",
        {"id"},
        {lgraph_api::FieldData::Int64(person2_id)}
    );
    txn.Commit();
    return true;
}

std::tuple<int64_t, int64_t, int64_t> AtomicityCheck(auto& db) {
    auto txn = db.CreateReadTxn();
    int64_t num_persons = 0;
    int64_t num_names = 0;
    int64_t num_emails = 0;
    for (auto vit = txn.GetVertexIterator(); vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() != "Person") continue;
        num_persons ++;
        if (!vit["name"].IsNull()) num_names ++;
        num_emails += CountItemsInField(vit, "emails");
    }
    return std::make_tuple(num_persons, num_names, num_emails);
}

void AtomicityCTest(auto& db) {
    Initialize(db);

    AtomicityInit(db);

    auto committed = AtomicityCheck(db);

    int64_t num_txns = 50;
    int64_t num_aborted_txns = 0;

    #pragma omp parallel for reduction(+:num_aborted_txns)
    for (int64_t i = 0; i < num_txns; i ++) {
        bool successful;
        try {
            successful = AtomicityC(db, 1, 3 + i, "alice@otherdomain.net", 2020);
        } catch (std::exception& e) {
            successful = false;
        }
        if (successful) {
            #pragma omp critical
            {
                std::get<0>(committed) ++;
                std::get<2>(committed) ++;
            }
        } else {
            num_aborted_txns ++;
        }
    }

    std::cout << "Number of aborted txns: " << num_aborted_txns << std::endl;

    auto finalstate = AtomicityCheck(db);

    if (committed == finalstate) {
        std::cout << "AtomicityCTest passed" << std::endl;
    } else {
        std::cout << "AtomicityCTest failed" << std::endl;
    }
}

void AtomicityRBTest(auto& db) {
    Initialize(db);

    AtomicityInit(db);

    auto committed = AtomicityCheck(db);

    int64_t num_txns = 50;
    int64_t num_aborted_txns = 0;

    #pragma omp parallel for reduction(+:num_aborted_txns)
    for (int64_t i = 0; i < num_txns; i ++) {
        bool successful;
        try {
            if (i % 2 == 0) {
                successful = AtomicityRB(db, 1, 2, "alice@otherdomain.net", 2020);
            } else {
                successful = AtomicityRB(db, 1, 3 + i, "alice@otherdomain.net", 2020);
            }
        } catch (std::exception& e) {
            successful = false;
        }
        if (successful) {
            #pragma omp critical
            {
                std::get<0>(committed) ++;
                std::get<2>(committed) ++;
            }
        } else {
            num_aborted_txns ++;
        }
    }

    std::cout << "Number of aborted txns: " << num_aborted_txns << std::endl;

    auto finalstate = AtomicityCheck(db);

    if (committed == finalstate) {
        std::cout << "AtomicityRBTest passed" << std::endl;
    } else {
        std::cout << "AtomicityRBTest failed" << std::endl;
    }
}

// Dirty Writes

void G0Init(auto& db) {
    auto txn = db.CreateWriteTxn(optimistic);
    auto p1_vid = txn.AddVertex(
        "Person",
        {"id", "versionHistory"},
        {
            lgraph_api::FieldData::Int64(1),
            lgraph_api::FieldData::String("0")
        }
    );
    auto p2_vid = txn.AddVertex(
        "Person",
        {"id", "versionHistory"},
        {
            lgraph_api::FieldData::Int64(2),
            lgraph_api::FieldData::String("0")
        }
    );
    txn.AddEdge(
        p1_vid, p2_vid, "knows", 
        {"versionHistory"},
        {lgraph_api::FieldData::String("0")}
    );
    txn.Commit();
}

void G0(auto& db, int64_t person1_id, int64_t person2_id, int64_t txn_id) {
    auto txn = db.CreateWriteTxn(optimistic);
    auto vit = txn.GetVertexIterator();
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Person" && vit["id"].AsInt64() == person1_id) break;
    }
    auto p1_vid = vit.GetId();
    AppendStringToField(vit, "versionHistory", std::to_string(txn_id));
    vit.Goto(0, true);
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Person" && vit["id"].AsInt64() == person2_id) break;
    }
    auto p2_vid = vit.GetId();
    AppendStringToField(vit, "versionHistory", std::to_string(txn_id));
    auto KNOWS = txn.GetEdgeLabelId("knows");
    auto oeit = txn.GetOutEdgeIterator(lgraph_api::EdgeUid(p1_vid, p2_vid, KNOWS, 0, 0));
    AppendStringToField(oeit, "versionHistory", std::to_string(txn_id));
    txn.Commit();
}

std::tuple<std::string, std::string, std::string> G0Check(auto& db, int64_t person1_id, int64_t person2_id) {
    auto txn = db.CreateReadTxn();
    auto vit = txn.GetVertexIterator();
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Person" && vit["id"].AsInt64() == person1_id) break;
    }
    auto p1_vid = vit.GetId();
    std::string p1_version_history = vit["versionHistory"].AsString();
    vit.Goto(0, true);
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Person" && vit["id"].AsInt64() == person2_id) break;
    }
    auto p2_vid = vit.GetId();
    std::string p2_version_history = vit["versionHistory"].AsString();
    auto KNOWS = txn.GetEdgeLabelId("knows");
    auto oeit = txn.GetOutEdgeIterator(lgraph_api::EdgeUid(p1_vid, p2_vid, KNOWS, 0, 0));
    std::string k_version_history = oeit["versionHistory"].AsString();
    return std::make_tuple(p1_version_history, p2_version_history, k_version_history);
}

void G0Test(auto& db) {
    Initialize(db);

    G0Init(db);

    int64_t num_txns = 200;
    int64_t num_aborted_txns = 0;
    #pragma omp parallel for reduction(+:num_aborted_txns)
    for (int64_t i = 1; i <= num_txns; i ++) {
        try {
            G0(db, 1, 2, i);
        } catch (std::exception& e) {
            num_aborted_txns ++;
        }
    }

    std::cout << "Number of aborted txns: " << num_aborted_txns << std::endl;

    std::string p1_version_history, p2_version_history, k_version_history;
    std::tie(p1_version_history, p2_version_history, k_version_history) = G0Check(db, 1, 2);

    std::cout << p1_version_history << std::endl;
    std::cout << p2_version_history << std::endl;
    std::cout << k_version_history << std::endl;

    if (p1_version_history == k_version_history && p2_version_history == k_version_history) {
        std::cout << "G0Test passed" << std::endl;
    } else {
        std::cout << "G0Test failed" << std::endl;
    }
}

// Aborted Reads

void G1AInit(auto& db) {
    auto txn = db.CreateWriteTxn(optimistic);
    txn.AddVertex(
        "Person",
        {"id", "version"},
        {
            lgraph_api::FieldData::Int64(1),
            lgraph_api::FieldData::Int64(1)
        }
    );
    txn.Commit();
}

void G1A1(auto& db, int64_t person1_id, int64_t sleep_ms) {
    auto txn = db.CreateWriteTxn(optimistic);
    auto vit = txn.GetVertexIterator();
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Person" && vit["id"].AsInt64() == person1_id) break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
    vit.SetField("version", lgraph_api::FieldData::Int64(2));
    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
    txn.Abort();
}

int64_t G1A2(auto& db, int64_t person1_id) {
    auto txn = db.CreateReadTxn();
    auto vit = txn.GetVertexIterator();
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Person" && vit["id"].AsInt64() == person1_id) break;
    }
    return vit["version"].AsInt64();
}

void G1ATest(auto& db) {
    Initialize(db);

    G1AInit(db);

    std::promise<int64_t> p1;
    auto f1 = p1.get_future();
    std::thread t1([&](std::promise<int64_t> && p1){
        int wc = 5;
        int64_t num_aborted_txns = 0;
        #pragma omp parallel for reduction(+:num_aborted_txns)
        for (int i = 0; i < wc; i ++) {
            try {
                G1A1(db, 1, 250);
            } catch (std::exception& e) {
                num_aborted_txns ++;
            }
        }
        p1.set_value(num_aborted_txns);
    }, std::move(p1));

    std::promise<int64_t> p2;
    auto f2 = p2.get_future();
    std::thread t2([&](std::promise<int64_t> && p2){
        int rc = 5;
        int64_t num_incorrect_checks = 0;
        #pragma omp parallel for reduction(+:num_incorrect_checks)
        for (int i = 0; i < rc; i ++) {
            auto p_version = G1A2(db, 1);
            if (p_version != 1) num_incorrect_checks ++;
        }
        p2.set_value(num_incorrect_checks);
    }, std::move(p2));

    t1.join();
    t2.join();
    int64_t num_aborted_txns = f1.get();
    int64_t num_incorrect_checks = f2.get();

    std::cout << "Number of aborted txns: " << num_aborted_txns << std::endl;

    if (num_incorrect_checks == 0) {
        std::cout << "G1ATest passed" << std::endl;
    } else {
        std::cout << "G1ATest failed" << std::endl;
    }
}

// Intermediate Reads

void G1BInit(auto& db) {
    auto txn = db.CreateWriteTxn(optimistic);
    txn.AddVertex(
        "Person",
        {"id", "version"},
        {
            lgraph_api::FieldData::Int64(1),
            lgraph_api::FieldData::Int64(99)
        }
    );
    txn.Commit();
}

void G1B1(auto& db, int64_t person1_id, int64_t sleep_ms, int64_t even, int64_t odd) {
    auto txn = db.CreateWriteTxn(optimistic);
    auto vit = txn.GetVertexIterator();
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Person" && vit["id"].AsInt64() == person1_id) break;
    }
    vit.SetField("version", lgraph_api::FieldData::Int64(even));
    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
    vit.SetField("version", lgraph_api::FieldData::Int64(odd));
    txn.Commit();
}

int64_t G1B2(auto& db, int64_t person1_id) {
    auto txn = db.CreateReadTxn();
    auto vit = txn.GetVertexIterator();
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Person" && vit["id"].AsInt64() == person1_id) break;
    }
    return vit["version"].AsInt64();
}

void G1BTest(auto& db) {
    Initialize(db);

    G1BInit(db);

    std::promise<int64_t> p1;
    auto f1 = p1.get_future();
    std::thread t1([&](std::promise<int64_t> && p1){
        int wc = 50;
        int64_t num_aborted_txns = 0;
        #pragma omp parallel for reduction(+:num_aborted_txns)
        for (int i = 0; i < wc; i ++) {
            try {
                G1B1(db, 1, 250, 0, 1);
            } catch (std::exception& e) {
                num_aborted_txns ++;
            }
        }
        p1.set_value(num_aborted_txns);
    }, std::move(p1));

    std::promise<int64_t> p2;
    auto f2 = p2.get_future();
    std::thread t2([&](std::promise<int64_t> && p2){
        int rc = 100;
        int64_t num_incorrect_checks = 0;
        #pragma omp parallel for reduction(+:num_incorrect_checks)
        for (int i = 0; i < rc; i ++) {
            auto p_version = G1B2(db, 1);
            if (p_version % 2 != 1) num_incorrect_checks ++;
        }
        p2.set_value(num_incorrect_checks);
    }, std::move(p2));

    t1.join();
    t2.join();
    int64_t num_aborted_txns = f1.get();
    int64_t num_incorrect_checks = f2.get();

    std::cout << "Number of aborted txns: " << num_aborted_txns << std::endl;

    if (num_incorrect_checks == 0) {
        std::cout << "G1BTest passed" << std::endl;
    } else {
        std::cout << "G1BTest failed" << std::endl;
    }
}

// Circular Information Flow

void G1CInit(auto& db) {
    auto txn = db.CreateWriteTxn(optimistic);
    txn.AddVertex(
        "Person",
        {"id", "version"},
        {
            lgraph_api::FieldData::Int64(1),
            lgraph_api::FieldData::Int64(0)
        }
    );
    txn.AddVertex(
        "Person",
        {"id", "version"},
        {
            lgraph_api::FieldData::Int64(2),
            lgraph_api::FieldData::Int64(0)
        }
    );
    txn.Commit();
}

int64_t G1C(auto& db, int64_t person1_id, int64_t person2_id, int64_t txn_id) {
    auto txn = db.CreateWriteTxn(optimistic);
    auto vit = txn.GetVertexIterator();
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Person" && vit["id"].AsInt64() == person1_id) break;
    }
    vit.SetField("version", lgraph_api::FieldData::Int64(txn_id));
    vit.Goto(0, true);
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Person" && vit["id"].AsInt64() == person2_id) break;
    }
    int64_t p2_version = vit["version"].AsInt64();
    txn.Commit();
    return p2_version;
}

void G1CTest(auto& db) {
    Initialize(db);

    G1CInit(db);

    int64_t c = 100;

    std::vector<int64_t> results(c);

    std::vector<std::mt19937> gens;
    for (int i = 0; i < omp_get_num_procs(); i ++) {
        gens.emplace_back(i);
    }

    #pragma omp parallel for
    for (int64_t i = 1; i <= c; i ++) {
        auto& gen = gens[omp_get_thread_num()];
        std::uniform_int_distribution<> dist(0, 1);
        try {
            if (dist(gen)) {
                results[i - 1] = G1C(db, 1, 2, i);
            } else {
                results[i - 1] = G1C(db, 2, 1, i);
            }
        } catch (std::exception& e) {
            results[i - 1] = -1;
        }
    }

    int64_t num_aborted_txns = 0;
    int64_t num_incorrect_checks = 0;
    for (int64_t i = 1; i <= c; i ++) {
        auto v1 = results[i - 1];
        if (v1 == -1) {
            num_aborted_txns ++;
        }
        if (v1 == 0) continue;
        auto v2 = results[v1 - 1];
        if (v2 == -1 || i == v2) {
            num_incorrect_checks ++;
        }
    }

    std::cout << "Number of aborted txns: " << num_aborted_txns << std::endl;

    if (num_incorrect_checks == 0) {
        std::cout << "G1CTest passed" << std::endl;
    } else {
        std::cout << "G1CTest failed" << std::endl;
    }
}

// Item-Many-Preceders

void IMPInit(auto& db) {
    auto txn = db.CreateWriteTxn(optimistic);
    txn.AddVertex(
        "Person",
        {"id", "version"},
        {
            lgraph_api::FieldData::Int64(1),
            lgraph_api::FieldData::Int64(1)
        }
    );
    txn.Commit();
}

void IMP1(auto& db, int64_t person1_id) {
    auto txn = db.CreateWriteTxn(optimistic);
    auto vit = txn.GetVertexIterator();
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Person" && vit["id"].AsInt64() == person1_id) break;
    }
    vit.SetField("version", lgraph_api::FieldData::Int64(vit["version"].AsInt64() + 1));
    txn.Commit();
}

std::tuple<int64_t, int64_t> IMP2(auto& db, int64_t person1_id, int64_t sleep_ms) {
    auto txn = db.CreateReadTxn();
    auto vit = txn.GetVertexIterator();
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Person" && vit["id"].AsInt64() == person1_id) break;
    }
    int64_t v1 = vit["version"].AsInt64();
    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
    vit.Goto(0, true);
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Person" && vit["id"].AsInt64() == person1_id) break;
    }
    int64_t v2 = vit["version"].AsInt64();
    return std::make_tuple(v1, v2);
}

void IMPTest(auto& db) {
    Initialize(db);

    IMPInit(db);

    std::promise<int64_t> p1;
    auto f1 = p1.get_future();
    std::thread t1([&](std::promise<int64_t> && p1){
        int wc = 50;
        int64_t num_aborted_txns = 0;
        #pragma omp parallel for reduction(+:num_aborted_txns)
        for (int i = 0; i < wc; i ++) {
            try {
                IMP1(db, 1);
            } catch (std::exception& e) {
                num_aborted_txns ++;
            }
        }
        p1.set_value(num_aborted_txns);
    }, std::move(p1));

    std::promise<int64_t> p2;
    auto f2 = p2.get_future();
    std::thread t2([&](std::promise<int64_t> && p2){
        int rc = 50;
        int64_t num_incorrect_checks = 0;
        #pragma omp parallel for reduction(+:num_incorrect_checks)
        for (int i = 0; i < rc; i ++) {
            int64_t v1, v2;
            std::tie(v1, v2) = IMP2(db, 1, 250);
            if (v1 != v2) num_incorrect_checks ++;
        }
        p2.set_value(num_incorrect_checks);
    }, std::move(p2));

    t1.join();
    t2.join();
    int64_t num_aborted_txns = f1.get();
    int64_t num_incorrect_checks = f2.get();

    std::cout << "Number of aborted txns: " << num_aborted_txns << std::endl;

    if (num_incorrect_checks == 0) {
        std::cout << "IMPTest passed" << std::endl;
    } else {
        std::cout << "IMPTest failed" << std::endl;
    }
}

// Predicate-Many-Preceders

void PMPInit(auto& db) {
    auto txn = db.CreateWriteTxn(optimistic);
    txn.AddVertex(
        "Person",
        {"id"},
        {
            lgraph_api::FieldData::Int64(1)
        }
    );
    txn.AddVertex(
        "Post",
        {"id"},
        {
            lgraph_api::FieldData::Int64(1)
        }
    );
    txn.Commit();
}

void PMP1(auto& db, int64_t person_id, int64_t post_id) {
    auto txn = db.CreateWriteTxn(optimistic);
    auto vit = txn.GetVertexIterator();
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Person" && vit["id"].AsInt64() == person_id) break;
    }
    auto p1_vid = vit.GetId();
    vit.Goto(0, true);
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Post" && vit["id"].AsInt64() == post_id) break;
    }
    auto p2_vid = vit.GetId();
    txn.AddEdge(
        p1_vid, p2_vid, "likes",
        std::vector<std::string>{},
        std::vector<lgraph_api::FieldData>{}
    );
    txn.Commit();
}

std::tuple<int64_t, int64_t> PMP2(auto& db, int64_t post_id, int64_t sleep_ms) {
    auto txn = db.CreateReadTxn();
    auto vit = txn.GetVertexIterator();
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Post" && vit["id"].AsInt64() == post_id) break;
    }
    int64_t c1 = 0;
    for (auto ieit = vit.GetInEdgeIterator(); ieit.IsValid(); ieit.Next()) {
        if (ieit.GetLabel() == "likes") c1 ++;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
    vit.Goto(0, true);
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Post" && vit["id"].AsInt64() == post_id) break;
    }
    int64_t c2 = 0;
    for (auto ieit = vit.GetInEdgeIterator(); ieit.IsValid(); ieit.Next()) {
        if (ieit.GetLabel() == "likes") c2 ++;
    }
    return std::make_tuple(c1, c2);
}

void PMPTest(auto& db) {
    Initialize(db);

    PMPInit(db);

    std::promise<int64_t> p1;
    auto f1 = p1.get_future();
    std::thread t1([&](std::promise<int64_t> && p1){
        int wc = 50;
        int64_t num_aborted_txns = 0;
        #pragma omp parallel for reduction(+:num_aborted_txns)
        for (int i = 0; i < wc; i ++) {
            try {
                PMP1(db, 1, 1);
            } catch (std::exception& e) {
                num_aborted_txns ++;
            }
        }
        p1.set_value(num_aborted_txns);
    }, std::move(p1));

    std::promise<int64_t> p2;
    auto f2 = p2.get_future();
    std::thread t2([&](std::promise<int64_t> && p2){
        int rc = 50;
        int64_t num_incorrect_checks = 0;
        #pragma omp parallel for reduction(+:num_incorrect_checks)
        for (int i = 0; i < rc; i ++) {
            int64_t v1, v2;
            std::tie(v1, v2) = PMP2(db, 1, 250);
            if (v1 != v2) num_incorrect_checks ++;
        }
        p2.set_value(num_incorrect_checks);
    }, std::move(p2));

    t1.join();
    t2.join();
    int64_t num_aborted_txns = f1.get();
    int64_t num_incorrect_checks = f2.get();

    std::cout << "Number of aborted txns: " << num_aborted_txns << std::endl;

    if (num_incorrect_checks == 0) {
        std::cout << "PMPTest passed" << std::endl;
    } else {
        std::cout << "PMPTest failed" << std::endl;
    }
}

// Observed Transaction Vanishes

void OTVInit(auto& db) {
    auto txn = db.CreateWriteTxn(optimistic);
    std::vector<int64_t> vids;
    for (int i = 1; i <= 4; i ++) {
        auto vid = txn.AddVertex(
            "Person",
            {"id", "version"},
            {
                lgraph_api::FieldData::Int64(i),
                lgraph_api::FieldData::Int64(0)
            }
        );
        vids.emplace_back(vid);
    }
    for (int i = 0; i < 4; i ++) {
        txn.AddEdge(
            vids[i], vids[(i + 1) % 4], "knows",
            std::vector<std::string>{},
            std::vector<lgraph_api::FieldData>{}
        );
    }
    txn.Commit();
}

void OTV1(auto& db, int64_t person_id) {
    auto txn = db.CreateWriteTxn(optimistic);
    auto vit1 = txn.GetVertexIterator();
    for (; vit1.IsValid(); vit1.Next()) {
        if (vit1.GetLabel() == "Person" && vit1["id"].AsInt64() == person_id) break;
    }
    int64_t vid1 = vit1.GetId();
    for (auto eit1 = vit1.GetOutEdgeIterator(); eit1.IsValid(); eit1.Next()) {
        int64_t vid2 = eit1.GetDst();
        auto vit2 = txn.GetVertexIterator(vid2);
        for (auto eit2 = vit2.GetOutEdgeIterator(); eit2.IsValid(); eit2.Next()) {
            int64_t vid3 = eit2.GetDst();
            auto vit3 = txn.GetVertexIterator(vid3);
            for (auto eit3 = vit3.GetOutEdgeIterator(); eit3.IsValid(); eit3.Next()) {
                int64_t vid4 = eit3.GetDst();
                auto vit4 = txn.GetVertexIterator(vid4);
                for (auto eit4 = vit4.GetOutEdgeIterator(); eit4.IsValid(); eit4.Next()) {
                    if (eit4.GetDst() == vid1) {
                        vit1.SetField("version", lgraph_api::FieldData::Int64(vit1["version"].AsInt64() + 1));
                        vit2.SetField("version", lgraph_api::FieldData::Int64(vit2["version"].AsInt64() + 1));
                        vit3.SetField("version", lgraph_api::FieldData::Int64(vit3["version"].AsInt64() + 1));
                        vit4.SetField("version", lgraph_api::FieldData::Int64(vit4["version"].AsInt64() + 1));
                        txn.Commit();
                        return;
                    }
                }
            }
        }
    }
}

std::tuple< std::tuple<int64_t, int64_t, int64_t, int64_t>, std::tuple<int64_t, int64_t, int64_t, int64_t> > OTV2(auto& db, int64_t person_id, int64_t sleep_ms) {
    auto txn = db.CreateReadTxn();
    auto vit1 = txn.GetVertexIterator();

    auto get_versions = [&]() -> std::tuple<int64_t, int64_t, int64_t, int64_t> {
        int64_t vid1 = vit1.GetId();
        for (auto eit1 = vit1.GetOutEdgeIterator(); eit1.IsValid(); eit1.Next()) {
            int64_t vid2 = eit1.GetDst();
            auto vit2 = txn.GetVertexIterator(vid2);
            for (auto eit2 = vit2.GetOutEdgeIterator(); eit2.IsValid(); eit2.Next()) {
                int64_t vid3 = eit2.GetDst();
                auto vit3 = txn.GetVertexIterator(vid3);
                for (auto eit3 = vit3.GetOutEdgeIterator(); eit3.IsValid(); eit3.Next()) {
                    int64_t vid4 = eit3.GetDst();
                    auto vit4 = txn.GetVertexIterator(vid4);
                    for (auto eit4 = vit4.GetOutEdgeIterator(); eit4.IsValid(); eit4.Next()) {
                        if (eit4.GetDst() == vid1) {
                            return std::make_tuple(
                                vit1["version"].AsInt64(), vit2["version"].AsInt64(), vit3["version"].AsInt64(), vit4["version"].AsInt64()
                            );
                        }
                    }
                }
            }
        }
    };

    for (; vit1.IsValid(); vit1.Next()) {
        if (vit1.GetLabel() == "Person" && vit1["id"].AsInt64() == person_id) break;
    }
    auto tup1 = get_versions();
    
    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));

    vit1.Goto(0, true);
    for (; vit1.IsValid(); vit1.Next()) {
        if (vit1.GetLabel() == "Person" && vit1["id"].AsInt64() == person_id) break;
    }
    auto tup2 = get_versions();

    return std::make_tuple(tup1, tup2);
}

void OTVTest(auto& db) {
    Initialize(db);

    OTVInit(db);

    std::promise<int64_t> p1;
    auto f1 = p1.get_future();
    std::thread t1([&](std::promise<int64_t> && p1){
        int wc = 50;
        int64_t num_aborted_txns = 0;
        #pragma omp parallel for reduction(+:num_aborted_txns)
        for (int i = 0; i < wc; i ++) {
            try {
                OTV1(db, 1);
            } catch (std::exception& e) {
                num_aborted_txns ++;
            }
        }
        p1.set_value(num_aborted_txns);
    }, std::move(p1));

    std::promise<int64_t> p2;
    auto f2 = p2.get_future();
    std::thread t2([&](std::promise<int64_t> && p2){
        int rc = 50;
        int64_t num_incorrect_checks = 0;
        #pragma omp parallel for reduction(+:num_incorrect_checks)
        for (int i = 0; i < rc; i ++) {
            std::tuple<int64_t, int64_t, int64_t, int64_t> tup1, tup2;
            std::tie(tup1, tup2) = OTV2(db, 1, 250);
            int64_t v1_max = std::max(std::max(std::get<0>(tup1), std::get<1>(tup1)), std::max(std::get<2>(tup1), std::get<3>(tup1)));
            int64_t v2_min = std::min(std::min(std::get<0>(tup2), std::get<1>(tup2)), std::min(std::get<2>(tup2), std::get<3>(tup2)));
            if (v1_max > v2_min) num_incorrect_checks ++;
        }
        p2.set_value(num_incorrect_checks);
    }, std::move(p2));

    t1.join();
    t2.join();
    int64_t num_aborted_txns = f1.get();
    int64_t num_incorrect_checks = f2.get();

    std::cout << "Number of aborted txns: " << num_aborted_txns << std::endl;

    if (num_incorrect_checks == 0) {
        std::cout << "OTVTest passed" << std::endl;
    } else {
        std::cout << "OTVTest failed" << std::endl;
    }
}

// Fractured Reads

void FRInit(auto& db) {
    OTVInit(db);
}

void FR1(auto& db, int64_t person_id) {
    OTV1(db, person_id);
}

std::tuple< std::tuple<int64_t, int64_t, int64_t, int64_t>, std::tuple<int64_t, int64_t, int64_t, int64_t> > FR2(auto& db, int64_t person_id, int64_t sleep_ms) {
    return OTV2(db, person_id, sleep_ms);
}

void FRTest(auto& db) {
    Initialize(db);

    FRInit(db);

    std::promise<int64_t> p1;
    auto f1 = p1.get_future();
    std::thread t1([&](std::promise<int64_t> && p1){
        int wc = 100;
        int64_t num_aborted_txns = 0;
        #pragma omp parallel for reduction(+:num_aborted_txns)
        for (int i = 0; i < wc; i ++) {
            try {
                FR1(db, 1);
            } catch (std::exception& e) {
                num_aborted_txns ++;
            }
        }
        p1.set_value(num_aborted_txns);
    }, std::move(p1));

    std::promise<int64_t> p2;
    auto f2 = p2.get_future();
    std::thread t2([&](std::promise<int64_t> && p2){
        int rc = 100;
        int64_t num_incorrect_checks = 0;
        #pragma omp parallel for reduction(+:num_incorrect_checks)
        for (int i = 0; i < rc; i ++) {
            std::tuple<int64_t, int64_t, int64_t, int64_t> tup1, tup2;
            std::tie(tup1, tup2) = FR2(db, 1, 250);
            if (tup1 != tup2) num_incorrect_checks ++;
        }
        p2.set_value(num_incorrect_checks);
    }, std::move(p2));

    t1.join();
    t2.join();
    int64_t num_aborted_txns = f1.get();
    int64_t num_incorrect_checks = f2.get();

    std::cout << "Number of aborted txns: " << num_aborted_txns << std::endl;

    if (num_incorrect_checks == 0) {
        std::cout << "FRTest passed" << std::endl;
    } else {
        std::cout << "FRTest failed" << std::endl;
    }
}

// Lost Updates

void LUInit(auto& db) {
    auto txn = db.CreateWriteTxn(optimistic);
    txn.AddVertex(
        "Person",
        {"id", "numFriends"},
        {
            lgraph_api::FieldData::Int64(1),
            lgraph_api::FieldData::Int64(0)
        }
    );
    txn.Commit();
}

void LU1(auto& db, int64_t person1_id, int64_t person2_id) {
    auto txn = db.CreateWriteTxn(optimistic);
    auto vit = txn.GetVertexIterator();
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Person" && vit["id"].AsInt64() == person1_id) break;
    }
    vit.SetField("numFriends", lgraph_api::FieldData::Int64(vit["numFriends"].AsInt64() + 1));
    auto p1_vid = vit.GetId();
    auto p2_vid = txn.AddVertex(
        "Person",
        {"id"},
        {lgraph_api::FieldData::Int64(person2_id)}
    );
    txn.AddEdge(
        p1_vid, p2_vid, "knows",
        std::vector<std::string>{},
        std::vector<lgraph_api::FieldData>{}
    );
    txn.Commit();
}

std::tuple<int64_t, int64_t> LU2(auto& db, int64_t person1_id) {
    auto txn = db.CreateReadTxn();
    auto vit = txn.GetVertexIterator();
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Person" && vit["id"].AsInt64() == person1_id) break;
    }
    int64_t num_friends = vit["numFriends"].AsInt64();
    int64_t num_knows_edges = 0;
    for (auto oeit = vit.GetOutEdgeIterator(); oeit.IsValid(); oeit.Next()) {
        if (oeit.GetLabel() == "knows") num_knows_edges ++;
    }
    return std::make_tuple(num_friends, num_knows_edges);
}

void LUTest(auto& db) {
    Initialize(db);

    LUInit(db);

    int64_t num_txns = 200;
    int64_t num_aborted_txns = 0;
    #pragma omp parallel for reduction(+:num_aborted_txns)
    for (int64_t i = 0; i < num_txns; i ++) {
        try {
            LU1(db, 1, i + 2);
        } catch (std::exception& e) {
            num_aborted_txns ++;
        }
    }

    std::cout << "Number of aborted txns: " << num_aborted_txns << std::endl;

    int64_t num_friends, num_knows_edges;
    std::tie(num_friends, num_knows_edges) = LU2(db, 1);

    std::cout << num_txns << " " << num_aborted_txns << " " << num_friends << " " << num_knows_edges << std::endl;

    if (num_friends == num_txns - num_aborted_txns && num_knows_edges == num_txns - num_aborted_txns) {
        std::cout << "LUTest passed" << std::endl;
    } else {
        std::cout << "LUTest failed" << std::endl;
    }
}

// Write Skews

void WSInit(auto& db) {
    auto txn = db.CreateWriteTxn(optimistic);
    for (int i = 1; i <= 10; i ++) {
        txn.AddVertex(
            "Person",
            {"id", "value"},
            {
                lgraph_api::FieldData::Int64(2 * i - 1),
                lgraph_api::FieldData::Int64(70)
            }
        );
        txn.AddVertex(
            "Person",
            {"id", "value"},
            {
                lgraph_api::FieldData::Int64(2 * i),
                lgraph_api::FieldData::Int64(80)
            }
        );
    }
    txn.Commit();
}

void WS1(auto& db, int64_t person1_id, int64_t person2_id, int64_t sleep_ms, std::mt19937& gen) {
    auto txn = db.CreateWriteTxn(optimistic);
    auto vit1 = txn.GetVertexIterator();
    for (; vit1.IsValid(); vit1.Next()) {
        if (vit1.GetLabel() == "Person" && vit1["id"].AsInt64() == person1_id) break;
    }
    int64_t vid1 = vit1.GetId();
    int64_t p1_value = vit1["value"].AsInt64();
    auto vit2 = txn.GetVertexIterator();
    for (; vit2.IsValid(); vit2.Next()) {
        if (vit2.GetLabel() == "Person" && vit2["id"].AsInt64() == person2_id) break;
    }
    int64_t vid2 = vit2.GetId();
    int64_t p2_value = vit2["value"].AsInt64();
    if (p1_value + p2_value - 100 < 0) return;
    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
    std::uniform_int_distribution<> dist(0, 1);
    // Write Skews can happen in TuGraph as optimistic writers only guarantee snapshot isolation
    if (dist(gen)) {
        vit1.SetField("value", lgraph_api::FieldData::Int64(p1_value - 100));
    } else {
        vit2.SetField("value", lgraph_api::FieldData::Int64(p2_value - 100));
    }
    txn.Commit();
}

void WS1Explicit(auto& db, int64_t person1_id, int64_t person2_id, int64_t sleep_ms, std::mt19937& gen) {
    auto txn = db.CreateWriteTxn(optimistic);
    auto vit1 = txn.GetVertexIterator();
    for (; vit1.IsValid(); vit1.Next()) {
        if (vit1.GetLabel() == "Person" && vit1["id"].AsInt64() == person1_id) break;
    }
    int64_t vid1 = vit1.GetId();
    int64_t p1_value = vit1["value"].AsInt64();
    auto vit2 = txn.GetVertexIterator();
    for (; vit2.IsValid(); vit2.Next()) {
        if (vit2.GetLabel() == "Person" && vit2["id"].AsInt64() == person2_id) break;
    }
    int64_t vid2 = vit2.GetId();
    int64_t p2_value = vit2["value"].AsInt64();
    if (p1_value + p2_value - 100 < 0) return;
    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
    std::uniform_int_distribution<> dist(0, 1);
    // Write Skews can be avoided in TuGraph by adding the read set into the write set for conflict detection
    if (dist(gen)) {
        vit1.SetField("value", lgraph_api::FieldData::Int64(p1_value - 100));
        vit2.SetField("value", lgraph_api::FieldData::Int64(p2_value));
    } else {
        vit2.SetField("value", lgraph_api::FieldData::Int64(p2_value - 100));
        vit1.SetField("value", lgraph_api::FieldData::Int64(p1_value));
    }
    txn.Commit();
}

std::vector< std::tuple<int64_t, int64_t, int64_t, int64_t> > WS2(auto& db) {
    std::vector< std::tuple<int64_t, int64_t, int64_t, int64_t> > results;
    auto txn = db.CreateReadTxn();
    for (auto vit1 = txn.GetVertexIterator(); vit1.IsValid(); vit1.Next()) {
        if (vit1.GetLabel() != "Person") continue;
        int64_t person1_id = vit1["id"].AsInt64();
        if (person1_id % 2 != 1) continue;
        int64_t p1_value = vit1["value"].AsInt64();
        for (auto vit2 = txn.GetVertexIterator(); vit2.IsValid(); vit2.Next()) {
            if (vit2.GetLabel() != "Person") continue;
            int64_t person2_id = vit2["id"].AsInt64();
            if (person2_id != person1_id + 1) continue;
            int64_t p2_value = vit2["value"].AsInt64();
            if (p1_value + p2_value <= 0) {
                results.emplace_back(person1_id, p1_value, person2_id, p2_value);
            }
        }
    }
    return results;
}

void WSTest(auto& db) {
    Initialize(db);

    WSInit(db);

    int wc = 50;

    int64_t num_aborted_txns = 0;

    std::vector<std::mt19937> gens;
    for (int i = 0; i < omp_get_num_procs(); i ++) {
        gens.emplace_back(i);
    }

    #pragma omp parallel for reduction(+:num_aborted_txns)
    for (int i = 0; i < wc; i ++) {
        auto& gen = gens[omp_get_thread_num()];
        std::uniform_int_distribution<> dist(1, 10);
        try {
            int64_t person1_id = dist(gen) * 2 - 1;
            int64_t person2_id = person1_id + 1;
            WS1(db, person1_id, person2_id, 250, gen);
        } catch (std::exception& e) {
            num_aborted_txns ++;
        }
    }

    std::cout << "Number of aborted txns: " << num_aborted_txns << std::endl;

    auto results = WS2(db);

    if (results.empty()) {
        std::cout << "WSTest passed" << std::endl;
    } else {
        std::cout << "WSTest failed" << std::endl;
        for (auto& tup : results) {
            std::cout << std::get<0>(tup) << " " << std::get<1>(tup) << " " << std::get<2>(tup) << " " << std::get<3>(tup) << std::endl;
        }
    }
}

void WSTestExplicit(auto& db) {
    Initialize(db);

    WSInit(db);

    int wc = 50;

    int64_t num_aborted_txns = 0;

    std::vector<std::mt19937> gens;
    for (int i = 0; i < omp_get_num_procs(); i ++) {
        gens.emplace_back(i);
    }

    #pragma omp parallel for reduction(+:num_aborted_txns)
    for (int i = 0; i < wc; i ++) {
        auto& gen = gens[omp_get_thread_num()];
        std::uniform_int_distribution<> dist(1, 10);
        try {
            int64_t person1_id = dist(gen) * 2 - 1;
            int64_t person2_id = person1_id + 1;
            WS1Explicit(db, person1_id, person2_id, 250, gen);
        } catch (std::exception& e) {
            num_aborted_txns ++;
        }
    }

    std::cout << "Number of aborted txns: " << num_aborted_txns << std::endl;

    auto results = WS2(db);

    if (results.empty()) {
        std::cout << "WSTest passed" << std::endl;
    } else {
        std::cout << "WSTest failed" << std::endl;
        for (auto& tup : results) {
            std::cout << std::get<0>(tup) << " " << std::get<1>(tup) << " " << std::get<2>(tup) << " " << std::get<3>(tup) << std::endl;
        }
    }
}

void TestAll(auto& db) {
    AtomicityCTest(db);

    AtomicityRBTest(db);

    G0Test(db);

    G1ATest(db);

    G1BTest(db);

    G1CTest(db);

    IMPTest(db);

    PMPTest(db);

    OTVTest(db);

    FRTest(db);

    LUTest(db);

    WSTest(db);

    // explicitly generating write-write conflicts to avoid write skews
    WSTestExplicit(db);
}

int main(int argc, char ** argv) {
    std::string db_path = "./testdb";

    lgraph_api::Galaxy galaxy(db_path, "admin", "73@TuGraph", false, true);
    auto db = galaxy.OpenGraph("default");

    optimistic = false;
    std::cout << "Serializable" << std::endl;
    std::cout << "--------------------" << std::endl;
    TestAll(db); // should be able to pass all tests
    std::cout << "--------------------" << std::endl;

    optimistic = true;
    std::cout << "Snapshot Isolation" << std::endl;
    std::cout << "--------------------" << std::endl;
    TestAll(db); // should be able to pass all tests except WSTest
    std::cout << "--------------------" << std::endl;

    return 0;
}
