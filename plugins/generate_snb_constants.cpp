#include "lgraph/lgraph.h"

#include <fstream>

std::string ToUpper(const std::string& input) {
    std::string output;
    for (auto c : input) {
        output.push_back((c >= 'a' && c <= 'z') ? (c - 'a' + 'A') : c);
    }
    return output;
}

int main(int argc, char** argv) {
    std::string db_path(argv[1]);
    std::string file_path(argv[2]);

    lgraph_api::Galaxy galaxy(db_path, "admin", "73@TuGraph", true, false);
    lgraph_api::GraphDB db = galaxy.OpenGraph("default");

    auto txn = db.CreateReadTxn();

    std::ofstream ofs(file_path, std::ofstream::out | std::ofstream::trunc);

    auto v_labels = txn.ListVertexLabels();
    for (auto& v_label : v_labels) {
        auto v_label_id = txn.GetVertexLabelId(v_label);
        ofs << "#define " << ToUpper(v_label) << " " << v_label_id << std::endl;
        auto v_fields = txn.GetVertexSchema(v_label);
        for (auto& v_field : v_fields) {
            auto& v_field_name = v_field.name;
            auto v_field_id = txn.GetVertexFieldId(v_label_id, v_field_name);
            ofs << "#define " << ToUpper(v_label) << "_" << ToUpper(v_field_name) << " " << v_field_id << std::endl;
        }
        ofs << std::endl;
    }

    auto e_labels = txn.ListEdgeLabels();
    for (auto& e_label : e_labels) {
        auto e_label_id = txn.GetEdgeLabelId(e_label);
        ofs << "#define " << ToUpper(e_label) << " " << e_label_id << std::endl;
        auto e_fields = txn.GetEdgeSchema(e_label);
        for (auto& e_field : e_fields) {
            auto& e_field_name = e_field.name;
            auto e_field_id = txn.GetEdgeFieldId(e_label_id, e_field_name);
            ofs << "#define " << ToUpper(e_label) << "_" << ToUpper(e_field_name) << " " << e_field_id << std::endl;
        }
        ofs << std::endl;
    }

    return 0;
}
