#pragma once

#include "udf/udf.h"
#include <iostream>
#include <list>
#include <vector>
#include <set>
#include <string.h>


using namespace std;
namespace doris_udf {

#define MAX_EVENT_COUNT 5000
#define MAGIC_INT 32

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
        void init(short num, long ts, int eventDistinctId) {
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
	    return false;
        }
    };

    struct FunnelInfoAgg {
        int old_ids[MAX_EVENT_COUNT];
        list<Event> _events;
        long _time_window;
        long _start_time;
        int _max_distinct_id;

        FunnelInfoAgg() {
            _time_window = 0L;
            _start_time = 0L;
            _max_distinct_id = 0;
        }
        void init() {
            _time_window = 0;
            _start_time = 0;
            _max_distinct_id = 0;
        }


        void add_event(Event *e) {
            _events.push_back(*e);
            if (e->_event_distinct_id > _max_distinct_id) {
                _max_distinct_id = e->_event_distinct_id;
            }
        }

        void add_event(short num, long ts, int event_distinct_id) {
            Event event(num, ts, event_distinct_id);
            _events.push_back(event);
            if (event_distinct_id >  _max_distinct_id) {
                _max_distinct_id = event_distinct_id;
            }
        }

        list<Event> get_events() { return _events; }


        void trim(list<Event>& events, vector<Event> &rst);

        short make_value(int row, int column);

        StringVal output(FunctionContext* context, StringVal* rst);
    };


    void funnel_info_init(FunctionContext* context, StringVal* funnelInfoAggVal);
    void funnel_info_update(FunctionContext* context, BigIntVal* from_time, IntVal* time_window, TinyIntVal steps, BigIntVal* event_time, FunnelInfoAgg* aggInfo);
    void funnel_info_merge(FunctionContext* context, const FunnelInfoAgg& srcAggInfo, FunnelInfoAgg* DestAggInfo);
    StringVal FunnelInfoFinalize(FunctionContext* context, const FunnelInfoAgg& aggInfo);

    FunnelInfoAgg parse(FunctionContext* context, const StringVal& aggInfoVal);
}
