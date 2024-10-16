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
// Created by Wangyunlai on 2024/05/29.
//

#include "sql/expr/aggregator.h"
#include "common/log/log.h"
#include "aggregator.h"

RC SumAggregator::accumulate(const Value &value)
{
  if(value.is_null())
    return RC::SUCCESS;
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = value;
    return RC::SUCCESS;
  }
  
  ASSERT(value.attr_type() == value_.attr_type(), "type mismatch. value type: %s, value_.type: %s", 
        attr_type_to_string(value.attr_type()), attr_type_to_string(value_.attr_type()));
  
  Value::add(value, value_, value_);
  return RC::SUCCESS;
}

RC SumAggregator::evaluate(Value& result)
{
  result = value_;
  return RC::SUCCESS;
}

RC CountAggregator::accumulate(const Value &value) 
{ 
  if(value.is_null())
    return RC::SUCCESS;
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = Value(1);
    return RC::SUCCESS;
  }
  
  Value::add(Value(1), value_, value_);
  return RC::SUCCESS;
}

RC CountAggregator::evaluate(Value &result) 
{ 
  result = value_;
  return RC::SUCCESS; 
}

RC AvgAggregator::accumulate(const Value &value) 
{ 
  if(value.is_null())
    return RC::SUCCESS;
  if (value_0f_row_num.attr_type() == AttrType::UNDEFINED) {
    value_0f_row_num = Value(1);
  }
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = value;
    return RC::SUCCESS;
  }
  
  ASSERT(value.attr_type() == value_.attr_type(), "type mismatch. value type: %s, value_.type: %s", 
        attr_type_to_string(value.attr_type()), attr_type_to_string(value_.attr_type()));
  
  Value::add(Value(1), value_0f_row_num, value_0f_row_num);
  Value::add(value, value_, value_);

  return RC::SUCCESS;
}

RC AvgAggregator::evaluate(Value &result) 
{ 
  if (result.attr_type() == AttrType::UNDEFINED) {
    result.set_type(AttrType::FLOATS);
  }
  Value::divide(value_, value_0f_row_num, result);
  return RC::SUCCESS; 
}
/*
INSERT INTO aggregation_func VALUES (40, 1, 6.0, 'F', '2033-09-28');
INSERT INTO aggregation_func VALUES (70, 1, 1.0, 'HEGK', '2021-11-26');
INSERT INTO aggregation_func VALUES (60, 1, 6.0, 'B0', '2029-04-23');
*/

RC MaxAggregator::accumulate(const Value &value) 
{ 
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = value;
    return RC::SUCCESS;
  }
  
  ASSERT(value.attr_type() == value_.attr_type(), "type mismatch. value type: %s, value_.type: %s", 
        attr_type_to_string(value.attr_type()), attr_type_to_string(value_.attr_type()));
  
  if(value_.compare(value)==-1){
    value_=value;
  }
  return RC::SUCCESS;
}

RC MaxAggregator::evaluate(Value &result) 
{ 
  result = value_;
  return RC::SUCCESS;
}

RC MinAggregator::accumulate(const Value &value)
{ 
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = value;
    return RC::SUCCESS;
  }
  
  ASSERT(value.attr_type() == value_.attr_type(), "type mismatch. value type: %s, value_.type: %s", 
        attr_type_to_string(value.attr_type()), attr_type_to_string(value_.attr_type()));
  
  if(value_.compare(value)==1){
    value_=value;
  }
  return RC::SUCCESS;
}


RC MinAggregator::evaluate(Value &result)
{ 
  result = value_;
  return RC::SUCCESS;
}
