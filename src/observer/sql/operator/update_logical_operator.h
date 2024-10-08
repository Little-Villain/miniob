#pragma once

#include "sql/operator/logical_operator.h"
#include "sql/parser/parse_defs.h"

/**
 * @brief 逻辑算子，用于执行update语句
 * @ingroup LogicalOperator
 */
class UpdateLogicalOperator : public LogicalOperator
{
public:
  UpdateLogicalOperator(Table *table,std::string  attribute_name,const Value *values);
  virtual ~UpdateLogicalOperator() = default;

  LogicalOperatorType type() const override { return LogicalOperatorType::UPDATE; }
  Table              *table() const { return table_; }
  string  attribute_name()const { return attribute_name_;}
  const Value *values() const{ return values_;}
 
private:
  Table *table_ = nullptr;
  std::string  attribute_name_;
  const Value *values_    = nullptr;
};