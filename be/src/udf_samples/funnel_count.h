#pragma once

#include "udf/udf.h"
#include <iostream>
#include <list>

using namespace std;
namespace doris_udf {

    class FunnelCountAgg: public AnyVal{
            
    }

    void FunnelCountInit(FunctionContext* context, FunnelCountAgg* val);
    void FunnelCountUpdate(FunctionContext* context, const StringVal& src, FunnelCountAgg* val);
    void FunnelCountMerge(FunctionContext* context, const FunnelCountAgg& src, FunnelCountAgg* dst);
    StringVal FunnelCountFinalize(FunctionContext* context, const FunnelCountAgg& val);

}
