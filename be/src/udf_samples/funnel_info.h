#pragma once

#include "udf/udf.h"
#include <iostream>
#include <list>

using namespace std;
namespace doris_udf {
    struct Event {
        short _num;
        long _ts;
        int _event_distinct_id;
        Event(short num, long ts, int eventDistinctId) : _num(num), _ts(ts), _event_distinct_id(eventDistinctId) { }
    }

    class FunnelInfoAgg: public AnyVal {
        list<Event> _events;
        long _time_window;
        long _start_time;
        int _max_distinct_id;

        public FunnelInfoAgg() {
            _time_window = 0L;
            _start_time = 0L;
            _max_distinctid = 0;
        }

        public void addEvent(short num, long ts, int event_distinct_id) {
            events.add(new Event(num, ts, event_distinct_id));
            if (event_distinct_id >  _max_distinct_id) {
                _max_distinct_id = event_distinct_id;
            }
        }

        public List<Event> getEvents();

        public void reset();

        List<Event> trim(List<Event> events);

        short makeValue(int row, int column);

        String output();
    }

    void FunnelInfoInit(FunctionContext* context, FunnelInfoAgg* FunnelInfoAgg);
    void FunnelInfoUpdate(FunctionContext* context, BigIntVal* from_time, IntVal* time_window, TinyIntVal steps, BigIntVal* event_time, FunnelInfoAgg* aggInfo);
    void FunnelInfoMerge(FunctionContext* context, const FunnelInfoAgg& srcAggInfo, FunnelInfoAgg* DestAggInfo);
    StringVal FunnelInfoFinalize(FunctionContext* context, const FunnelInfoAgg& aggInfo);

    class FunnelCountAgg: public AnyVal{
            
    }

    void FunnelCountInit(FunctionContext* context, FunnelCountAgg* val);
    void FunnelCountUpdate(FunctionContext* context, const StringVal& src, FunnelCountAgg* val);
    void FunnelCountMerge(FunctionContext* context, const FunnelCountAgg& src, FunnelCountAgg* dst);
    StringVal FunnelCountFinalize(FunctionContext* context, const FunnelCountAgg& val);

}
