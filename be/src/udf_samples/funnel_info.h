#pragma once

#include "udf/udf.h"
#include <iostream>
#include <list>
#include <vector>
#inculde <set>
#include <string.h>


using namespace std;
namespace doris_udf {

#define MAX_EVENT_COUNT 5000
#define MAGIC_INF 32

    static int event_distinct_id = 0;
    static const char funnel_tag[] = "funnel";
    int tag_size = sizeof funnel_tag;

    long get_start_of_day(long ts) {
        long mod = (ts - 57600000) % 86400000;
        return ts - mod;
    }

    struct Event {
        short _num;
        long _ts;
        int _event_distinct_id;
        Event(short num, long ts, int eventDistinctId) : _num(num), _ts(ts), _event_distinct_id(eventDistinctId) { }
        init(short num, long ts, int eventDistinctId) {
            _num = num;
            _ts = ts;
            _event_distinct_id = eventDistinctId;
        }

        bool operator < (Event& other) {
            if (_ts < other._ts) {
                return true;
            } else if (_ts == other._ts) {
                if (_num >= other._num) {
                    return false;
                } else if (_num < other._num ) {
                    return true;
                }
            } else {
                return false;
            }
        }
    };

    struct FunnelInfoAgg: public AnyVal {
        int[MAX_EVENT_COUNT] old_ids;
        list<Event> _events;
        long _time_window;
        long _start_time;
        int _max_distinct_id;

        public FunnelInfoAgg() {
            _time_window = 0L;
            _start_time = 0L;
            _max_distinctid = 0;
        }
        public void init() {
            _time_window = 0;
            _start_time = 0;
            _max_distinct_id = 0;
        }

        public static FunnelInfoAgg parse(FunctionContext* context, StringVal& aggInfoVal);

        public void add_event(Event *e) {
            events.push_back(*e);
            if (e->event_distinct_id > _max_distinct_id) {
                _max_distinct_id = e->event_distinct_id;
            }
        }

        public void add_event(short num, long ts, int event_distinct_id) {
            Event event(num, ts, event_distinct_id);
            _events.push_back(event);
            if (event_distinct_id >  _max_distinct_id) {
                _max_distinct_id = event_distinct_id;
            }
        }

        public List<Event> get_events() { return _events; }


        List<Event> trim(List<Event> events);

        short make_value(int row, int column);

        String output(StringVal* rst);
    };

    typedef StringVal FunnelInfoAggVal;

    void FunnelInfoInit(FunctionContext* context, FunnelInfoAggVal* FunnelInfoAggVal);
    void FunnelInfoUpdate(FunctionContext* context, BigIntVal* from_time, IntVal* time_window, TinyIntVal steps, BigIntVal* event_time, FunnelInfoAgg* aggInfo);
    void FunnelInfoMerge(FunctionContext* context, const FunnelInfoAgg& srcAggInfo, FunnelInfoAgg* DestAggInfo);
    StringVal FunnelInfoFinalize(FunctionContext* context, const FunnelInfoAgg& aggInfo);
}
