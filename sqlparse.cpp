#include<iostream>
#include<fstream>
#include<algorithm>
#include<vector>
#include<unordered_map>
#include<set>
#include<string>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/lexical_cast.hpp>
#include "SQLParser.h"
#include "util/sqlhelper.h"

#define ll long long

#define USAGE_ERROR "Usage: sqlparse <QUERY>"
#define SUCCESSFUL_PARSE "Sucessfully parsed"
#define UNSUCCESSFUL_PARSE "Error while parsing"
#define METADATA_FILE "files/metadata.txt"
#define TABLE_NOT_FOUND "Table not found: "
#define INVALID_WHERE_CLAUSE 4
#define INVALID_SELECT_LIST_INT 4
#define TABLE_NOT_FOUND_INT 3
#define UNSUCCESSFUL_PARSE_INT 2
#define USAGE_ERROR_INT 1
#define echo(STR) std::cout << STR << std::endl;
#define eecho(STR) std::cerr << STR << std::endl;
#define eechon(STR,NUM) std::cerr << STR << NUM << std::endl;
#define echon(STR,NUM) std::cout << STR << NUM << std::endl;
#define uvmap std::unordered_map<std::string, std::vector<std::string>>
#define umapsll std::unordered_map<std::string, ll>
#define vstring std::vector<std::string>
#define sset std::set<std::string>
#define svrumap std::unordered_map<std::string, std::vector<Row>>


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

enum ColumnType {
    STAR,
    COLUMN,
    FUNCTION
};

class Row {
    private:
    std::string table_name;
    umapsll data;
    public:
    Row(std::string table, std::string line, uvmap &metadata) {
        table_name = table;
        vstring split_val;
        boost::split(split_val, line, boost::is_any_of(","));
        for (int i = 0; i < (int)split_val.size(); ++i) {
            boost::trim_left_if(split_val[i],boost::is_any_of("\""));
            boost::trim_right_if(split_val[i],boost::is_any_of("\""));
            data.insert({metadata[table][i], boost::lexical_cast<ll>(split_val[i])});
        }
    }

    ll get_data(std::string col) {
        return data[col];
    }

    std::string get_table() {
        return table_name;
    }

    ll& operator[] (std::string index) {
        return data[index];
    }

    std::string get_value(std::string index) {
        // eecho(index)
        return std::to_string(data[index]);
    }
};

class Column {
    private:
    std::string table_name;
    std::string column_name;
    std::string function;
    ColumnType type;
    public:
    Column(std::string table, std::string column) {
        table_name = table;
        column_name = column;
        function = "";
        type = ColumnType::COLUMN;
    }
    Column(std::string table, std::string column, std::string func) {
        table_name = table;
        column_name = column;
        function = func;
        type = ColumnType::FUNCTION;
    }
    Column() {
        table_name = "";
        column_name = "";
        type = ColumnType::STAR;
    }

    std::string get_column() {
        return column_name;
    }

    bool is_type(ColumnType t) {
        return t == type;
    }

    std::string get_table() {
        return table_name;
    }

    std::string get_function() {
        return function;
    }

    ColumnType get_type() {
        return type;
    }

    std::string get_full_name() {
        if (type == ColumnType::STAR)
            return "*";
        std::string full_name = table_name + std::string(".") + column_name;
        if (type == ColumnType::FUNCTION)
            full_name = function + "(" + full_name + ")";
        return full_name;
    }
};

class Condition {
    private:
    Column col1;
    Column col2;
    hsql::OperatorType op;
    public:
    static const std::vector<hsql::OperatorType> supported_op; 
    bool get_validity(Row &r1, Row &r2) {
        bool value = true;
        switch(op) {
        case hsql::OperatorType::kOpEquals :
            value = r1[col1.get_column()] == r2[col2.get_column()];
        break;
        case hsql::OperatorType::kOpLess :
            value = r1[col1.get_column()] < r2[col2.get_column()];
        break;
        case hsql::OperatorType::kOpLessEq :
            value = r1[col1.get_column()] <= r2[col2.get_column()];
        break;
        case hsql::OperatorType::kOpGreater :
            value = r1[col1.get_column()] > r2[col2.get_column()];
        break;
        case hsql::OperatorType::kOpGreaterEq :
            value = r1[col1.get_column()] >= r2[col2.get_column()];
        break;
        default:
        value = false;
        };
        return value;
    }

    void set_operator(hsql::OperatorType o) {
        op = o;
    }

    void set_columns(Column c1, Column c2) {
        col1 = c1;
        col2 = c2;
    }
};

class ConditionList {
    private:
    std::vector<Condition> cond;
    hsql::OperatorType op;
    public:
    ConditionList() {
        op = hsql::OperatorType::kOpAnd;
    }
    void add_condition(const Condition& c) {
        cond.push_back(c);
    }
    void set_op(hsql::OperatorType o) {
        op = o;
    }
    bool get_truth(Row& r1, Row& r2) {
        bool truth = op != hsql::OperatorType::kOpOr;
        for (auto a: cond) {
            switch(op) {
                case hsql::OperatorType::kOpAnd:
                truth = truth && a.get_validity(r1, r2);
                break;
                case hsql::OperatorType::kOpOr:
                truth = truth || a.get_validity(r1, r2);
                default:
                truth = false;
            }
        }
        return truth;
    }
};

const std::vector<hsql::OperatorType> Condition::supported_op = std::vector<hsql::OperatorType>{
        hsql::OperatorType::kOpEquals,
        hsql::OperatorType::kOpLess,
        hsql::OperatorType::kOpLessEq,
        hsql::OperatorType::kOpGreater,
        hsql::OperatorType::kOpGreaterEq,
        hsql::OperatorType::kOpUnaryMinus,
    };

void get_state(std::string& line, META_TYPE& state) {
    if (line == "<begin_table>") {
        state = META_TYPE::BEGIN_TABLE;
    } else if (state == META_TYPE::BEGIN_TABLE) {
        state = META_TYPE::TABLE_NAME;
    } else if (line == "<end_table>") {
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
        boost::to_lower(line);
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

bool exists_table(uvmap metadata, hsql::TableRef *tables, sset &tables_referenced) {
    bool flag = true;
    std::string name = "";
    if(tables->type == hsql::kTableName) {
        name = tables->getName();
        flag = flag && check_table_exists(metadata, name);
        tables_referenced.insert(name);
    } else if (tables->type == hsql::kTableCrossProduct) {
        for (auto table : *tables->list) {
            name = table->getName();
            flag = flag && check_table_exists(metadata, name);
            if (!flag)
                break;
            tables_referenced.insert(name);
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

bool valid_columns(uvmap &metadata, std::vector<hsql::Expr*> list,
            hsql::TableRef *tables, const sset &tables_used, std::vector<Column> &columns_ref) {
    if (list.size() == 1 && list[0]->type == hsql::kExprStar) {
        for (auto t: tables_used)
            for (auto c: metadata[t])
                columns_ref.push_back(Column(t, c));
        return true;
    }
    if (list.size() == 1 && list[0]->isType( hsql::kExprFunctionRef)) {
        columns_ref.push_back(Column(*tables_used.begin(), (*list[0]->exprList)[0]->getName(), list[0]->getName()));
        return true;
    }
      
    bool flag = true;
    std::string err_msg = "";
    for (auto col: list) {
        std::string clmn, tbl;
        if (col->hasTable()) {
            flag = flag && (metadata[col->table].end() !=
                    find(metadata[col->table].begin(), metadata[col->table].end(), col->name)) &&
                    tables_used.find(col->table) != tables_used.end();
            clmn = col->name;
            tbl = col->table;
            if (!flag)
                err_msg = "Column: " + std::string(col->name) + " not found!!";
        } else {
            auto table_list = get_tables(tables);
            int occurrence = 0;
            for (auto table: table_list) {
                if(
                    std::find(metadata[table->getName()].begin(), metadata[table->getName()].end(),
                    col->name) != metadata[table->getName()].end() &&
                    tables_used.find(table->getName()) != tables_used.end()
                ) {
                    occurrence++;
                    tbl = table->getName();
                    clmn = col->getName();
                }
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
        if (!flag) break;
        columns_ref.push_back(Column(tbl, clmn));
    }
    eecho(err_msg)
    return flag;
}

bool valid_where_operand(uvmap &metadata, hsql::Expr* expr, hsql::Expr* parent, const sset &tables_used, Column &c) {
    bool flag = true;
    std::string err_msg = "";
    if (!expr->isType(hsql::kExprColumnRef)) return flag;
    if (expr->hasTable()) {
        if (tables_used.find(expr->table) == tables_used.end()) {
            err_msg = "Table: " + std::string(expr->table) + "not specified in where clause!";
            flag = false;
        } else {
            flag = flag &&
                std::find(metadata[expr->table].begin(), metadata[expr->table].end(), expr->getName()) != metadata[expr->table].end();
                if (!flag)
                    err_msg = "Column: " + std::string(expr->name) + " not found!!";
                else
                    c = Column(expr->table, expr->getName());
        }
    } else {
        int occurrence = 0;
        for (auto table: tables_used) {
            occurrence += 
            std::find(metadata[table].begin(), metadata[table].end(), expr->name) != metadata[table].end() ? (c = Column(table, expr->name), 1) : 0;
        }
        if (occurrence == 0) {
            err_msg = "Column: " + std::string(expr->name) + " not found!";
            flag = false;
        } else if (occurrence > 1) {
            err_msg = "Column: " + std::string(expr->name) + " ambiguous!";
            flag = false;
        }
    }
    eecho(err_msg)
    return flag;
}

bool valid_where(uvmap &metadata, hsql::Expr* where_clause, const sset &tables_used, ConditionList &cond) {
    if(!where_clause)
        return true;
    if (!where_clause->isType(hsql::kExprOperator))
        return false;
    bool validity = false;
    if (std::find(Condition::supported_op.begin(), Condition::supported_op.end(), where_clause->opType) !=
        Condition::supported_op.end()) {
        Condition cond1;
        Column c1, c2;
        validity = valid_where_operand(metadata, where_clause->expr, where_clause, tables_used, c1) &&
        valid_where_operand(metadata, where_clause->expr2, where_clause, tables_used, c2);
        cond1.set_operator(where_clause->opType);
        cond1.set_columns(c1, c2);
        cond.add_condition(cond1);
    } else if (where_clause->opType == hsql::OperatorType::kOpAnd || where_clause->opType == hsql::OperatorType::kOpOr){
        Column c1, c2, c3, c4;
        Condition cond1, cond2;
        validity = valid_where_operand(metadata, where_clause->expr->expr, where_clause->expr, tables_used, c1) &&
            valid_where_operand(metadata, where_clause->expr->expr2, where_clause->expr, tables_used, c2) &&
            valid_where_operand(metadata, where_clause->expr2->expr, where_clause->expr2, tables_used, c3) &&
            valid_where_operand(metadata, where_clause->expr2->expr2, where_clause->expr2, tables_used, c4);
        cond1.set_operator(where_clause->expr->opType);
        cond1.set_columns(c1, c2);
        cond1.set_operator(where_clause->expr2->opType);
        cond1.set_columns(c3, c4);
        cond.add_condition(cond1);
        cond.add_condition(cond2);
        cond.set_op(where_clause->opType);
    }
    return validity;
}

std::vector<Row> read_table(std::string path, std::string table_name, uvmap &metadata) {
    std::ifstream infile(path);
    std::string line;
    std::vector<Row> table;
    while (infile >> line) {
        table.push_back(Row(table_name, line, metadata));
    }
    return table;
}

void pretty_print(std::vector<vstring> list) {
    for (auto line : list) {
        std::cout << "|\t";
        for (auto str: line)
            std::cout << str << "\t|\t";
        std::cout << std::endl;
    }
}

void pretty_print(std::vector<Column> list) {
    std::cout << "|";
    for (auto str: list) {
        std::cout << str.get_full_name() << "\t|";
    }
    std::cout << std::endl;
}

template<typename T>
void pretty_print(T val) {
    std::cout << "|\t" << val << "\t|" << std::endl;
}

class State {
    private:
    static ll minimum;
    static ll maximum;
    static ll count;
    static double average;
    static ll sum;
    static bool is_init;
    public:
    static void set() {
        minimum = LONG_LONG_MAX;
        maximum = LONG_LONG_MIN;
        average = 0;
        sum = 0;
        count = 0;
        is_init = true;
    }
    static bool is_set() {
        return is_init;
    }
    static void add(std::string func, ll num) {
        if (func == "min") minimum = std::min(minimum, num);
        else if (func == "max") maximum = std::max(maximum, num);
        sum += num;
        ++count;
        average = (sum*1.0)/count;
    }
    static double get(std::string func) {
        if (func == "min") return minimum;
        else if (func == "max") return maximum;
        else if (func == "sum") return sum;
        return average;
    }
};
ll State::minimum = LONG_LONG_MAX;
ll State::maximum = LONG_LONG_MIN;
double State::average = 0;
ll State::sum = 0;
ll State::count = 0;
bool State::is_init = false;


void single_table_execute(const std::vector<hsql::Expr*> &select_list, const sset &tables_ref,
    std::vector<Column> &columns_ref, ConditionList &cond, uvmap &metadata, bool distinct) {
    std::string path = "files/" + *tables_ref.begin() + ".csv";
    // eecho(path)
    std::vector<vstring> output;
    std::set<vstring> unique;
    auto table = read_table(path, *tables_ref.begin(), metadata);
    pretty_print(columns_ref);
    State::set();
    bool has_func = false;
    for (auto r: table) {
        
        vstring line;
        for (auto c: columns_ref) {
            if(c.is_type(ColumnType::FUNCTION))
                State::add(c.get_function(), r[c.get_column()]), has_func = true;
            else
                line.push_back(r.get_value(c.get_column()));
        }
        if (distinct && unique.count(line) && !has_func) continue;
        unique.insert(line);
        output.push_back(line);
    }
    if(has_func)
        pretty_print(State::get(columns_ref[0].get_function()));
    else
        pretty_print(output);
}

void multi_table_execute(const std::vector<hsql::Expr*> &select_list, const sset &tables_ref,
    std::vector<Column> &columns_ref, ConditionList &cond, uvmap &metadata) {
    
}

void execute_query(const std::vector<hsql::Expr*> &select_list, const sset &tables_ref,
    std::vector<Column> &columns_ref, ConditionList &cond, uvmap &metadata, bool distinct) {
    if (columns_ref.size() < 1) return;
    if (tables_ref.size() < 2) {
        single_table_execute(select_list, tables_ref, columns_ref, cond, metadata, distinct);
    } else {
        multi_table_execute(select_list, tables_ref, columns_ref, cond, metadata);
    }
    return;
}

int main(int argv, char *argc[]) {
    if (argv < 2) {
        eecho(USAGE_ERROR);
        return -USAGE_ERROR_INT;
    }
    std::string args(argc[1]);
    const std::string query(boost::to_lower_copy(args));
    hsql::SQLParserResult parsed_query;
    hsql::SQLParser::parse(query, &parsed_query);
    if (parsed_query.isValid()) {
        uvmap metadata;
        // eecho(SUCCESSFUL_PARSE);
        load_metadata(metadata);
        // print_metadata(metadata);
        for (auto i = 0u; i < parsed_query.size(); ++i) {
            sset tables_ref;
            std::vector<Column> columns_ref;
            ConditionList cond;
            auto stmnt = parsed_query.getStatement(i);
            // hsql::printStatementInfo(stmnt);
            if(!in_scope(stmnt)) continue;
            hsql::SelectStatement* select_stmnt = (hsql::SelectStatement*)stmnt;
            std::vector<hsql::Expr*> select_list = *select_stmnt->selectList;
            hsql::TableRef *select_table = select_stmnt->fromTable;
            if(!exists_table(metadata, select_table, tables_ref)) return -TABLE_NOT_FOUND_INT;
            if(!valid_columns(metadata, select_list, select_table, tables_ref, columns_ref)) return -INVALID_SELECT_LIST_INT;
            hsql::Expr* where_clause = select_stmnt->whereClause;
            if(!valid_where(metadata, where_clause, tables_ref, cond)) return -INVALID_WHERE_CLAUSE;
            execute_query(select_list, tables_ref, columns_ref, cond, metadata, select_stmnt->selectDistinct);
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
