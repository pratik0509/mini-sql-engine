#include<iostream>
#include<fstream>
#include<algorithm>
#include<vector>
#include<unordered_map>
#include<string>
#include <boost/algorithm/string.hpp>
#include "SQLParser.h"
#include "util/sqlhelper.h"

#define USAGE_ERROR "Usage: sqlparse <QUERY>"
#define SUCCESSFUL_PARSE "Sucessfully parsed"
#define UNSUCCESSFUL_PARSE "Error while parsing"
#define METADATA_FILE "files/metadata.txt"
#define TABLE_NOT_FOUND "Table not found: "
#define INVALID_SELECT_LIST_INT 4
#define INVALID_SELECT_LIST_INT 4
#define TABLE_NOT_FOUND_INT 3
#define UNSUCCESSFUL_PARSE_INT 2
#define USAGE_ERROR_INT 1
#define echo(STR) std::cerr << STR << std::endl;
#define eecho(STR) std::cerr << STR << std::endl;
#define eechon(STR,NUM) std::cerr << STR << NUM << std::endl;
#define echon(STR,NUM) std::cerr << STR << NUM << std::endl;
#define uvmap std::unordered_map<std::string, std::vector<std::string>>
#define vstring std::vector<std::string>>


bool in_scope(const hsql::SQLStatement *stmnt) {
    if (!stmnt->isType(hsql::kStmtSelect)) {
        eecho("Not a SELECT Query!")
        eecho("WIP!!")
    }
    return true;
}

enum META_TYPE {
    BEGIN_TABLE,
    TABLE_NAME,
    COLUMN_NAME,
    END_TABLE
};

void get_state(std::string& line, META_TYPE& state) {
    if (line == "<BEGIN_TABLE>") {
        state = META_TYPE::BEGIN_TABLE;
    } else if (state == META_TYPE::BEGIN_TABLE) {
        state = META_TYPE::TABLE_NAME;
    } else if (line == "<END_TABLE>") {
        state = META_TYPE::END_TABLE;
    } else {
        state = META_TYPE::COLUMN_NAME;
    }
    return;
}

void load_metadata(uvmap& metadata) {
    std::ifstream infile(METADATA_FILE);
    std::string line, cur_table;
    META_TYPE state = META_TYPE::END_TABLE;
    while (infile >> line) {
        boost::to_upper(line);
        get_state(line, state);
        switch (state) {
            case META_TYPE::TABLE_NAME:
                metadata.insert({line, {}});
                cur_table = line;
                break;
            case META_TYPE::COLUMN_NAME:
                metadata[cur_table].push_back(line);
                break;
            case META_TYPE::END_TABLE:
            default:
                break;
        }
    }
}

void print_metadata(uvmap& meta) {
    eecho("Reading metadata files:");
    eecho(METADATA_FILE);
    eecho("Tables found:");
    for (auto k: meta) {
        eecho(k.first);
        eecho("Columns:")
        for (auto l: k.second) {
            eecho(l)
        }
    }
    return;
}

bool check_table_exists(uvmap metadata, std::string name) {
    return metadata.find(name) != metadata.end();
}

bool exists_table(uvmap metadata, hsql::TableRef *tables) {
    bool flag = true;
    std::string name = "";
    if(tables->type == hsql::kTableName) {
        name = tables->getName();
        flag = flag && check_table_exists(metadata, name);
    } else if (tables->type == hsql::kTableCrossProduct) {
        for (auto table : *tables->list) {
            name = table->getName();
            flag = flag && check_table_exists(metadata, name);
            if (!flag)
                break;
        }
    } else {
        flag = false;
    }
    if (!flag) eecho(std::string("Table not found: ") + name)

    return flag;
}

std::vector<hsql::TableRef*> get_tables(hsql::TableRef *tables) {
    if (tables->type == hsql::kTableCrossProduct)
        return *tables->list;
    return std::vector<hsql::TableRef*>{tables};
}

bool valid_columns(uvmap &metadata, std::vector<hsql::Expr*> list, hsql::TableRef *tables) {
    if (list.size() == 1 && (list[0]->type == hsql::kExprStar || list[0]->type == hsql::kExprFunctionRef))
        return true;
    bool flag = true;
    std::string err_msg = "";
    for (auto col: list) {
        if (col->hasTable()) {
            flag = flag && (metadata[col->table].end() !=
                    find(metadata[col->table].begin(), metadata[col->table].end(), col->name));
            if (!flag)
                err_msg = "Column: " + std::string(col->name) + " not found!!";
        } else {
            auto table_list = get_tables(tables);
            int occurrence = 0;
            for (auto table: table_list) {
                occurrence += 
                    (std::find(metadata[table->getName()].begin(), metadata[table->getName()].end(),
                    col->name) != metadata[table->getName()].end()) ? 1 : 0;
            }
            if (occurrence == 0) {
                err_msg = "Column: " + std::string(col->name) + " not found!";
                flag = false;
            }
            if (occurrence > 1) {
                err_msg = "Column: " + std::string(col->name) + " ambiguous!";
                flag = false;
            }
        }
        if (!flag) {
            eecho(err_msg)
            break;
        }
    }
    return flag;
}

bool valid_where_operand(uvmap &metadata, hsql::Expr* expr, hsql::Expr* parent) {
    bool flag = true;
    
    return flag;
}

bool valid_where(uvmap &metadata, hsql::Expr* where_clause) {
    if(!where_clause)
        return true;
    if (!where_clause->isType(hsql::kExprOperator))
        return false;
    std::vector<hsql::OperatorType> supported_op{
        hsql::OperatorType::kOpEquals,
        hsql::OperatorType::kOpLess,
        hsql::OperatorType::kOpLessEq,
        hsql::OperatorType::kOpGreater,
        hsql::OperatorType::kOpGreaterEq,
        hsql::OperatorType::kOpUnaryMinus,
    };
    if (std::find(supported_op.begin(), supported_op.end(), where_clause->opType) != supported_op.end())
        return
            valid_where_operand(metadata, where_clause->expr, where_clause) &&
            valid_where_operand(metadata, where_clause->expr2, where_clause);
    if (where_clause->opType == hsql::OperatorType::kOpAnd || where_clause->opType == hsql::OperatorType::kOpOr)
        return
            valid_where_operand(metadata, where_clause->expr->expr, where_clause->expr) &&
            valid_where_operand(metadata, where_clause->expr->expr2, where_clause->expr) &&
            valid_where_operand(metadata, where_clause->expr2->expr, where_clause->expr2) &&
            valid_where_operand(metadata, where_clause->expr2->expr2, where_clause->expr2);

    return false;
}

int main(int argv, char *argc[]) {
    if (argv < 2) {
        eecho(USAGE_ERROR);
        return -USAGE_ERROR_INT;
    }
    std::string args(argc[1]);
    const std::string query(boost::to_upper_copy(args));
    hsql::SQLParserResult parsed_query;
    hsql::SQLParser::parse(query, &parsed_query);
    if (parsed_query.isValid()) {
        uvmap metadata;
        eecho(SUCCESSFUL_PARSE);
        load_metadata(metadata);
        // print_metadata(metadata);
        for (auto i = 0u; i < parsed_query.size(); ++i) {
            auto stmnt = parsed_query.getStatement(i);
            hsql::printStatementInfo(stmnt);
            if(!in_scope(stmnt)) continue;
            hsql::SelectStatement* select_stmnt = (hsql::SelectStatement*)stmnt;
            std::vector<hsql::Expr*> select_list = *select_stmnt->selectList;
            hsql::TableRef *select_table = select_stmnt->fromTable;
            if(!exists_table(metadata, select_table)) return -TABLE_NOT_FOUND_INT;
            if(!valid_columns(metadata, select_list, select_table)) return -INVALID_SELECT_LIST_INT;
            hsql::Expr* where_clause = select_stmnt->whereClause;
        }
    } else {
        eecho(UNSUCCESSFUL_PARSE)
        eecho(parsed_query.errorMsg())
        eechon("Line number: ",parsed_query.errorLine()+1)
        eechon("Column number: ",parsed_query.errorColumn()+1)
        return -UNSUCCESSFUL_PARSE_INT;
    }
    return 0;
}
