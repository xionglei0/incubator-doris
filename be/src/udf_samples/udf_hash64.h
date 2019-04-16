
#pragma once

#include "udf/udf.h"

namespace doris_udf {

    BigIntVal AddUdf(FunctionContext* context, const StringVal& arg1);

}