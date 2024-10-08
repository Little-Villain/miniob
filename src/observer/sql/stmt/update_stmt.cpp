/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2022/5/22.
//

#include "sql/stmt/update_stmt.h"
#include "common/log/log.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "sql/stmt/filter_stmt.h"

UpdateStmt::UpdateStmt(Table *table, const Value *values, int value_amount,FilterStmt*filter_stmt,string attribute_name)
    : table_(table), values_(values), value_amount_(value_amount),filter_stmt_(filter_stmt),attribute_name_(attribute_name)
{}

RC UpdateStmt::create(Db *db, const UpdateSqlNode &update, Stmt *&stmt)
{
  // collect tables in `from` statement
  BinderContext binder_context;
  vector<Table *>                tables;
  unordered_map<string, Table *> table_map;

  const char *table_name = update.relation_name.c_str();
  if (nullptr == db || nullptr == table_name) {
    LOG_WARN("UpdateStmt::invalid argument. db=%p, table_name=%p",
        db, table_name);
    return RC::INVALID_ARGUMENT;
  }

  // check whether the table exists
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("UpdateStmt::no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }
  
  binder_context.add_table(table);
  tables.push_back(table);
  table_map.insert({table_name, table});
  
  Table *default_table = nullptr;
  if (tables.size() == 1) {
    default_table = tables[0];
  }
  // create filter statement in `where` statement
  FilterStmt *filter_stmt = nullptr;
  RC          rc          = FilterStmt::create(db,
      default_table,
      &table_map,
      update.conditions.data(),
      static_cast<int>(update.conditions.size()),
      filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("cannot construct filter stmt");
    return rc;
  }
  
  //const int        value_num  = static_cast<int>(update.value.size());
  
  stmt = new UpdateStmt ( table, &update.value,1,filter_stmt,update.attribute_name);
  return RC::SUCCESS;
  
}
