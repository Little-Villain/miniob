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
// Created by wangyunlai on 2021/6/11.
//

#include "common/defs.h"
#include <string.h>
#include <queue>
#include "common/lang/algorithm.h"
#include <string>
using namespace std;


namespace common {

int compare_int(void *arg1, void *arg2)
{
  int v1 = *(int *)arg1;
  int v2 = *(int *)arg2;
  if (v1 > v2) {
    return 1;
  } else if (v1 < v2) {
    return -1;
  } else {
    return 0;
  }
}

int compare_float(void *arg1, void *arg2)
{
  float v1  = *(float *)arg1;
  float v2  = *(float *)arg2;
  float cmp = v1 - v2;
  if (cmp > EPSILON) {
    return 1;
  }
  if (cmp < -EPSILON) {
    return -1;
  }
  return 0;
}

int compare_string(void *arg1, int arg1_max_length, void *arg2, int arg2_max_length)
{
  const char *s1     = (const char *)arg1;
  const char *s2     = (const char *)arg2;
  int         maxlen = min(arg1_max_length, arg2_max_length);
  int         result = strncmp(s1, s2, maxlen);
  if (0 != result) {
    return result < 0 ? -1 : 1;
  }

  if (arg1_max_length > maxlen) {
    return 1;
  }

  if (arg2_max_length > maxlen) {
    return -1;
  }
  return 0;
}

int compare_string_like(void *arg1, int arg1_max_length, void *arg2, int arg2_max_length)//左值为确定值，右值为匹配% _
{
  //返回值成功为1，不成功为0
  const char *s1     = (const char *)arg1;
  const char *s2     = (const char *)arg2;
  int         leftlen= arg1_max_length;
  queue<string> que;

  string s="";
  for(int i=0;i<arg2_max_length;i++){//处理%
    if(s2[i]!='%')
      s+=s2[i];
    else{
      que.push(s);
      s="";
    }
  }
  if(s!="")
  	que.push(s);
  int p=0;//匹配已到左表达式的某处
  int num=que.size(); 
  while(!que.empty()){
    string str=que.front();
    int rightlen=str.length();
    int i=0,j=0;
    while(i<leftlen&&j<rightlen){
      if(str[j]==s1[i]||str[j]=='_'){
        i++;
        j++;
      }
      else if(str[j]!=s1[i]){
      	if(que.size()==(long unsigned int)num&&s2[0]!='%'){
          while(!que.empty())
          	que.pop();
          return 0;
        }
        i++;
        j=0;
      }
    }
    if(j==rightlen){//匹配成功
      que.pop();
      p=i;
    }
    else{
      while(!que.empty())
        que.pop();
      return 0;
    }
  }
  if(p!=leftlen&&s2[arg2_max_length-1]!='%'){
  	return 0;
  }
  return 1;	
}

}  // namespace common
