#pragma once

#include "udf/udf.h"

namespace doris_udf {

/// This is an example of the COUNT aggregate function.
void CountInit(FunctionContext* context, BigIntVal* val);
void CountUpdate(FunctionContext* context, const IntVal& input, BigIntVal* val);
void CountMerge(FunctionContext* context, const BigIntVal& src, BigIntVal* dst);
BigIntVal CountFinalize(FunctionContext* context, const BigIntVal& val);

}
