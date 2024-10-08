#pragma once

#include "sql/operator/physical_operator.h"
#include "sql/parser/parse_defs.h"
#include "storage/table/table.h"
class Trx;
class UpdateStmt;

/**
 * @brief 物理算子，update
 * @ingroup PhysicalOperator
 */
class UpdatePhysicalOperator : public PhysicalOperator
{
public:
  UpdatePhysicalOperator(Table *table,std::string  attribute_name,const Value *values) : table_(table) ,attribute_name_(attribute_name),values_(values) {}

  virtual ~UpdatePhysicalOperator() = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::UPDATE; }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  Tuple *current_tuple() override { return nullptr; }

private:
  Table              *table_ = nullptr;
  std::string         attribute_name_;
  const Value        *values_= nullptr;
  Trx                *trx_   = nullptr;
  std::vector<Record> records_;
};