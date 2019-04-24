#include "udf_samples/funnel_info.h"

namespace doris_udf {

    public static FunnelInfoAgg FunnelInfoAgg::parse(FunctionContext* context, StringVal& aggInfoVal) {
            FunnelInfoAgg* agg = context.allocate(sizeof(FunnelInfoAgg));
            agg->init();
            uint8_t* index = aggInfoVal.val + tag_size;
            uint8_t* end = aggInfoVal.val + aggInfoVal.len;

            _time_window = (int64_t) (*(index ))
            index += sizeof int64_t;
            _start_time = (int64_t) (*(index))
            index += sizeof int64_t;
            while(index < aggInfoVal.len){
                short step = (int32_t) (*(index));
                index += sizeof int32_t;

                int64_t event_time = (int64_t) (*(index))
                index += sizeof int64_t;

                int32_t event_distinct_id = (int32_t) (*(index))
                index += sizeof int32_t;
                agg->add_event(step, event_time, event_distinct_id);
            }
            
    }


    public short FunnelInfoAgg::make_value(int row, int column) {
        if (row < 0) {
            return (short) ((-1) * (Math.abs(row) * 100 + column));
        } else {
            return (short) (row * 100 + column);
        }
    }


    public void FunnelInfoAgg::trim(list<Event>& events, Vector<Event> & rst) {
        if (events.size() <= 0) {
            return;
        }

        Event* first = null, second = null;
        int count = 0;
        long day_of_start = 0l;
        for (list<Event>:: const_iterator iter = events.begin(); iter != events.end() ; iter++) {
            if (null == first) {
                first = iter;
                first->_event_distinct_id = count++ ;
                rst.push_back(*first);
                day_of_start = get_start_of_day(first->_ts);
            } else if (null == second) {
                Event* tmp = iter;
                if (tmp->num == first->num && day_of_start == get_start_of_day(tmp->_ts)) {
                    second = tmp;
                } else {
                    first = tmp;
                    first->_event_distinct_id = count++;
                    rst.push_back(*first);
                    day_of_start = get_start_of_day(first->_ts);
                }
            } else {
                Event* tmp = iter;
                if (tmp->_num == first->_num && day_of_start == get_start_of_day(tmp->_ts)) {
                    second = tmp;
                } else {
                    second._event_distinct_id = count++;
                    rst.push_back(*second);
                    first = tmp;
                    first->_event_distinct_id = count++;
                    rst.push_back(*first);
                    second = null;
                    day_of_start = 0L;
                }
            }
        }

        if (second != null) {
            second->_event_distinct_id = count++;
            rst.push_back(*second);
        }
    }

    public StringVal FunnelInfoAgg::String output(StringVal* rst) {
        // 1: order by events
        events.sort();
        
        // 2: trim events
        vector<Event> rst
        trim(events, &rst);
        
        // 2.1: check events if exceeds max_event_count;
        if (events.size > MAX_EVENT_COUNT) {
            return *rst;
        }

        // 3: calc funnel metric & get distinct short encoded num
        set<short> cache;
        int event_count = rst.size();
        for (int i = 0; i < event_count; i++) {
            Event& first_event = rst.get(i);
            if (first_event._num != 1) {
                continue;
            }

            int row = (int) ( (get_start_of_day(firstEvent._ts) - get_start_of_day(_start_time)) )/86400000)
            cache.insert(make_value(-1, 1));
            cache.insert(make_value(row, 1));
            int target_num = 2;
            for (int c = 0; c MAX_EVENT_COUNT; c++) {
                old_ids[c] = 0;
            }
            old_ids[first_event._event_distinct_id] = 1;

            for (int j = i+1; j < event_count; j++) {
                Event event = events.get(j);
                long delta = event._ts - first_event.ts;
                if (event.num == target_num && delta <= _time_window && old_ids[event._event_distinct_id] == 0) {
                    cache.insert(make_value(row, target_num));
                    cache.insert(make_value(-1, target_num));

                    target_num++;
                    old_ids[event._event_distinct_id] = 1;
                } else if (delta > _time_window) {
                    break;
                }
            }
        }
        return *rst;
    }

    void FunnelInfoInit(FunctionContext* context, StringVal* info_agg_val) {
        info_agg_val->is_null = false;
        info_agg_val->val = context->allocate(tag_size);
        info_agg_val->len = tag_size;
        memcpy(info_agg_val->val, funnel_tag, tag_size);
    }

    void FunnelInfoUpdate(FunctionContext *context, BigIntVal *from_time, BigIntVal *time_window, TinyIntVal steps, BigIntVal *event_time, StringVal *info_agg_val)
    {
        if (info_agg_val->len == tag_size)
        {
            info_agg_val->append(context, (const uint8_t*)&(time_window->val), sizeof(int64_t));
            info_agg_val->append(context, (const uint8_t*)&(from_time->val), sizeof(int64_t));
        }

        for (short i = 0; i < MAGIC_INT; i++)
        {
            int target = 1 << i;
            int r = target & steps;
            if (target <= num && r == target) {
                // Event * e = reinterpret_cast<Event *> context.allocate(sizeof(Event));
                // e->init(i+1, event_time->val, event_distinct_id);
                // agg_ptr->add_event(e);
                short step = i + 1;
                info_agg_val->append(context, (const uint8_t*)&step, sizeof(step));
                info_agg_val->append(context, (const uint8_t*)&(event_time->val), sizeof(int64_t));
                info_agg_val->append(context, (const uint8_t*)&event_distinct_id, sizeof(int32_t));
            }
        }
        event_distinct_id++;
    }
    
    //map side merge and reduce side merge
    void FunnelInfoMerge(FunctionContext* context, const StringVal& src_agginfo_val, StringVal* dest_agginfo_val) {
        dest_agginfo_val->append(context, src_agginfo_val.val + tag_size ,src_agginfo_val.len - tag_size) ;
    }

    StringVal FunnelInfoFinalize(FunctionContext* context, const StringVal& aggInfoVal) {
        FunnelInfoAgg* finalAgg = FunnelInfoAgg.parse(context, aggInfoVal);
        StringVal* result = context.allocate(sizeof StringVal);
        return finalAgg->output(&result);
    }
}
