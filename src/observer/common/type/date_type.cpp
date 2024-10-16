#include <iomanip>

#include "common/lang/comparator.h"
#include "common/lang/sstream.h"
#include "common/log/log.h"
#include "common/type/date_type.h"
#include "common/value.h"

int DateType:: compare(const Value &left, const Value &right) const
{
    return common::compare_int((void*)&left.value_.int_value_,(void*)&right.value_.int_value_);
}

RC DateType::to_string(const Value &val, string &result)const
{
    if(val.is_null()){
      result="null";
      return RC::SUCCESS;
    }
    stringstream ss;
    ss<<std::setw(4)<<std::setfill('0')<<val.value_.int_value_/10000<<"-"
      <<std::setw(2)<<std::setfill('0')<<(val.value_.int_value_%10000)/100<<"-"
      <<std::setw(2)<<std::setfill('0')<<val.value_.int_value_%100;
    result=ss.str();
    return RC::SUCCESS;
} 

RC DateType::cast_to(const Value &val, AttrType type, Value &result) const 
{ 
  if(val.is_null()){
    result.set_null(1);
    result.set_string("!!!");
    return RC::SUCCESS;
  }
  switch (type) {
    case AttrType::INTS:
    {
      result.attr_type_=AttrType::INTS;
      result.set_int(int(0));
      LOG_WARN("cast date to int, it's only allowed when compares to null!");
    }break;//for compare to null
    default: return RC::UNIMPLEMENTED;
  }
  return RC::SUCCESS; 
}
int DateType::cast_cost(AttrType type)
{ 
  if (type == AttrType::INTS) 
  {
    LOG_WARN("cast date to int, it's only allowed when compares to null!");
    return 100;
  }
  return INT32_MAX;
}