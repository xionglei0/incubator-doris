// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "exprs/timestamp_functions.h"

#include <gtest/gtest.h>
#include <boost/scoped_ptr.hpp>

#include "testutil/function_utils.h"
#include "exprs/timezone_db.h"
#include "udf/udf.h"
#include "udf/udf_internal.h"
#include "runtime/runtime_state.h"
#include "runtime/exec_env.h"
#include "runtime/test_env.h"


namespace doris {
class FunctionContextImpl;

class TimestampFunctionsTest : public testing::Test {
public:
    TimestampFunctionsTest() { }

    void SetUp() {
        TimezoneDatabase::init();
        
        TQueryGlobals globals;
        globals.__set_now_string("1565080737805");
        globals.__set_time_zone("America/Los_Angeles");
        state = new RuntimeState(globals);
        utils = new FunctionUtils(state);
        ctx = utils->get_fn_ctx();
    }
    void TearDown() {
        delete state;
        delete utils;
    }
private:
    RuntimeState* state;
    FunctionUtils* utils;
    FunctionContext* ctx;
};

TEST_F(TimestampFunctionsTest, day_of_week_test) {
    doris_udf::FunctionContext *context = new doris_udf::FunctionContext();

    doris_udf::DateTimeVal tv;
    //2001-02-03 12:34:56
    tv.packed_time = 1830650338932162560L;
    tv.type = TIME_DATETIME;

    ASSERT_EQ(7, TimestampFunctions::day_of_week(context, tv).val);
}

TEST_F(TimestampFunctionsTest, time_diff_test) {
    DateTimeValue dt1(20190718120000);
    dt1.set_type(TIME_DATETIME);
    doris_udf::DateTimeVal tv1;
    dt1.to_datetime_val(&tv1);
    
    DateTimeValue dt2(20190718130102);
    dt2.set_type(TIME_DATETIME);
    doris_udf::DateTimeVal tv2;
    dt2.to_datetime_val(&tv2);
    
    ASSERT_EQ(-3662, TimestampFunctions::time_diff(ctx, tv1, tv2).val);
}

TEST_F(TimestampFunctionsTest, now) {
    DateTimeVal now = TimestampFunctions::now(ctx);
    DateTimeValue dt = DateTimeValue::from_datetime_val(now);
    ASSERT_EQ(20190806013857, dt.to_int64());
}

TEST_F(TimestampFunctionsTest, from_unix) {
    IntVal unixtimestamp(1565080737);
    StringVal sval = TimestampFunctions::from_unix(ctx, unixtimestamp);
    ASSERT_EQ("2019-08-06 01:38:57", std::string((char*) sval.ptr, sval.len));
}

TEST_F(TimestampFunctionsTest, to_unix) {
    DateTimeVal dt_val ;
    dt_val.packed_time = 1847544683002068992;
    dt_val.type = TIME_DATETIME;
    ASSERT_EQ(1565080737, TimestampFunctions::to_unix(ctx).val);
    ASSERT_EQ(1565080737, TimestampFunctions::to_unix(ctx, dt_val).val);
    ASSERT_EQ(1565080737, TimestampFunctions::to_unix(ctx, StringVal("2019-08-06 01:38:57"), "%Y-%m-%d %H:%i:%S").val);
}

TEST_F(TimestampFunctionsTest, curtime) {
    ASSERT_EQ(3600 + 38*60 + 57, TimestampFunctions::curtime(ctx).val);
}

TEST_F(TimestampFunctionsTest, convert_tz_test) {
    doris_udf::FunctionContext *context = new doris_udf::FunctionContext();
    DateTimeValue dt1(20190806163857);
    dt1.set_type(TIME_DATETIME);
    doris_udf::DateTimeVal tv1;
    dt1.to_datetime_val(&tv1);   
    DateTimeVal t = TimestampFunctions::convert_tz(context, tv1, StringVal("Asia/Shanghai"), StringVal("America/Los_Angeles"));
    DateTimeValue dt2 = DateTimeValue::from_datetime_val(t);
    ASSERT_EQ(20190806013857, dt2.to_int64());

    t = TimestampFunctions::convert_tz(context, tv1, StringVal("CST"), StringVal("America/Los_Angeles"));
    DateTimeValue dt3 = DateTimeValue::from_datetime_val(t);
    ASSERT_EQ(20190806013857, dt3.to_int64());
}

}
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
