/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "common/lang/comparator.h"
#include "common/log/log.h"
#include "common/type/char_type.h"
#include "common/value.h"
#include "common/time/datetime.h"

int CharType::compare(const Value &left, const Value &right) const
{
  ASSERT(left.attr_type() == AttrType::CHARS && right.attr_type() == AttrType::CHARS, "invalid type");
  return common::compare_string(
      (void *)left.value_.pointer_value_, left.length_, (void *)right.value_.pointer_value_, right.length_);
}
int CharType::compare_like(const Value &left, const Value &right) const
{
  ASSERT(left.attr_type() == AttrType::CHARS && right.attr_type() == AttrType::CHARS, "invalid type");
  return common::compare_string_like(
      (void *)left.value_.pointer_value_, left.length_, (void *)right.value_.pointer_value_, right.length_);
}

RC CharType::set_value_from_str(Value &val, const string &data) const
{
  val.set_string(data.c_str());
  return RC::SUCCESS;
}

RC CharType::cast_to(const Value &val, AttrType type, Value &result) const
{
  if(val.is_null()){
    result.set_null(1);
    // switch (type) {
    //   case AttrType::INTS:result.set_int(int(0));break;
    //   case AttrType::FLOATS:result.set_float(float(0));break;
    //   case AttrType::CHARS:result.set_string("null");break;
    //   default: return RC::UNIMPLEMENTED;
    // }
    result.set_string("!");
    return RC::SUCCESS;
  }
  switch (type) {
    case AttrType::DATES:
    {
      result.attr_type_=AttrType::DATES;
      int year,month,day;
      if(sscanf(val.value_.pointer_value_,"%d-%d-%d",&year,&month,&day)!=3){
        LOG_WARN("invalid date format: %s",val.value_.pointer_value_);
        return RC::INVALID_ARGUMENT;    
      }
      bool check = common::check_date(year,month,day);
      if(!check){
        LOG_WARN("invalid date format: %s",val.value_.pointer_value_);
        return RC::INVALID_ARGUMENT; 
      }
      result.set_date(year,month,day);
    }break;
    default: return RC::UNIMPLEMENTED;
  }
  return RC::SUCCESS;
}

int CharType::cast_cost(AttrType type)
{
  if (type == AttrType::CHARS) {
    return 0;
  }
  if (type == AttrType::DATES) {
    return 1;
  }
  return INT32_MAX;
}

RC CharType::to_string(const Value &val, string &result) const
{
  if(val.is_null()){
    result="null";
    return RC::SUCCESS;
  }
  stringstream ss;
  ss << val.value_.pointer_value_;
  result = ss.str();
  return RC::SUCCESS;
}