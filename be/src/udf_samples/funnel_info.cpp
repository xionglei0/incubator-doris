#include "udf_samples/funnel_info.h"

namespace doris_udf
{

FunnelInfoAgg parse(FunctionContext *context, const StringVal &aggInfoVal)
{
    FunnelInfoAgg agg;
    agg.init();
    uint8_t *index = aggInfoVal.ptr + tag_size;

    agg._time_window = (int64_t)(*(index));
    index += sizeof(int64_t);
    agg._start_time = (int64_t)(*(index));
    index += sizeof(int64_t);
    uint8_t *end_index = aggInfoVal.ptr + aggInfoVal.len;
    while (index < end_index)
    {
        short step = (int32_t)(*(index));
        index += sizeof(int32_t);

        int64_t event_time = (int64_t)(*(index));
        index += sizeof(int64_t);

        int32_t event_distinct_id = (int32_t)(*(index));
        index += sizeof(int32_t);
        agg.add_event(step, event_time, event_distinct_id);
    }
    return agg;
}

short FunnelInfoAgg::make_value(int row, int column)
{
    if (row < 0)
    {
        return (short)((-1) * ((-1 * row) * 100 + column));
    }
    else
    {
        return (short)(row * 100 + column);
    }
}

void FunnelInfoAgg::trim(list<Event> &events, vector<Event> &rst)
{
    if (events.size() <= 0)
    {
        return;
    }

    list<Event>::iterator first = events.end();
    list<Event>::iterator second = events.end();
    int count = 0;
    long day_of_start = 0l;
    for (list<Event>::iterator iter = events.begin(); iter != events.end(); iter++)
    {
        if (first == events.end())
        {
            first = iter;
            first->_event_distinct_id = count++;
            rst.push_back(*first);
            day_of_start = get_start_of_day(first->_ts);
        }
        else if (second == events.end())
        {
            list<Event>::iterator tmp = iter;
            if (tmp->_num == first->_num && day_of_start == get_start_of_day(tmp->_ts))
            {
                second = tmp;
            }
            else
            {
                first = tmp;
                first->_event_distinct_id = count++;
                rst.push_back(*first);
                day_of_start = get_start_of_day(first->_ts);
            }
        }
        else
        {
            list<Event>::iterator tmp = iter;
            if (tmp->_num == first->_num && day_of_start == get_start_of_day(tmp->_ts))
            {
                second = tmp;
            }
            else
            {
                second->_event_distinct_id = count++;
                rst.push_back(*second);
                first = tmp;
                first->_event_distinct_id = count++;
                rst.push_back(*first);
                second = events.end();
                day_of_start = 0L;
            }
        }
    }

    if (second != events.end())
    {
        second->_event_distinct_id = count++;
        rst.push_back(*second);
    }
}

StringVal FunnelInfoAgg::output(FunctionContext *context, StringVal *rst_str)
{
    // 1: order by events
    bool show = false;
    if (_events.size() > 0)
        show = true;

    if (show)
        cout << "_events size:" << _events.size() << endl;
    _events.sort();

    // 2: trim events
    vector<Event> rst;
    this->trim(_events, rst);
    if (show)
        cout << "after trim, left event size:" << rst.size() << endl;

    // 2.1: check events if exceeds max_event_count;
    if (rst.size() > MAX_EVENT_COUNT)
    {
        return *rst_str;
    }

    // 3: calc funnel metric & get distinct short encoded num
    set<short> cache;
    int event_count = rst.size();
    for (int i = 0; i < event_count; i++)
    {
        Event &first_event = rst[i];
        if (first_event._num != 1)
        {
            continue;
        }

        int row = (int)((get_start_of_day(first_event._ts) - get_start_of_day(_start_time)) / 86400000);
        cache.insert(make_value(-1, 1));
        cache.insert(make_value(row, 1));
        int target_num = 2;
        for (int c = 0; c < MAX_EVENT_COUNT; c++)
        {
            old_ids[c] = 0;
        }
        old_ids[first_event._event_distinct_id] = 1;

        for (int j = i + 1; j < event_count; j++)
        {
            Event event = rst[j];
            long delta = event._ts - first_event._ts;
            if (event._num == target_num && delta <= _time_window && old_ids[event._event_distinct_id] == 0)
            {
                cache.insert(make_value(row, target_num));
                cache.insert(make_value(-1, target_num));

                target_num++;
                old_ids[event._event_distinct_id] = 1;
            }
            else if (delta > _time_window)
            {
                break;
            }
        }
    }

    if (show)
        cout << "cache size:" << cache.size() << endl;
    // 4: make result
    if (rst_str->len == 0)
    {
        rst_str->is_null = false;
        rst_str->ptr = context->allocate(tag_size);
        rst_str->len = tag_size;
        memcpy(rst_str->ptr, funnel_tag, tag_size);
    }

    for (set<short>::iterator iterator = cache.begin(); iterator != cache.end(); iterator++)
    {
        short value = *iterator;
        string encoded_str = encoder.encode((unsigned char *)&value, sizeof(short));
        rst_str->append(context, (uint8_t *)(encoded_str.c_str()), encoded_str.length());
    }

    return *rst_str;
}

void funnel_info_init(FunctionContext *context, StringVal *info_agg_val)
{
    info_agg_val->is_null = false;
    info_agg_val->ptr = context->allocate(tag_size);
    info_agg_val->len = tag_size;
    memcpy(info_agg_val->ptr, funnel_tag, tag_size);
}

void funnel_info_update(FunctionContext *context, BigIntVal *from_time, BigIntVal *time_window, TinyIntVal *steps, BigIntVal *event_time, StringVal *info_agg_val)
{
    if (info_agg_val->len == tag_size)
    {
        info_agg_val->append(context, (const uint8_t *)&(time_window->val), sizeof(int64_t));
        info_agg_val->append(context, (const uint8_t *)&(from_time->val), sizeof(int64_t));
    }

    for (short i = 0; i < MAGIC_INT; i++)
    {
        int target = 1 << i;
        int r = target & steps->val;
        if (target <= steps->val && r == target)
        {
            short step = i + 1;
            info_agg_val->append(context, (const uint8_t *)&step, sizeof(short));
            info_agg_val->append(context, (const uint8_t *)&(event_time->val), sizeof(int64_t));
            info_agg_val->append(context, (const uint8_t *)&event_distinct_id, sizeof(int32_t));
        }
    }
    event_distinct_id++;
}

//map side merge and reduce side merge
void funnel_info_merge(FunctionContext *context, const StringVal &src_agginfo_val, StringVal *dest_agginfo_val)
{
    dest_agginfo_val->append(context, src_agginfo_val.ptr + tag_size, src_agginfo_val.len - tag_size);
}

StringVal funnel_info_finalize(FunctionContext *context, const StringVal &aggInfoVal)
{
    FunnelInfoAgg finalAgg = parse(context, aggInfoVal);
    StringVal result;
    finalAgg.output(context, &result);
    return result;
}
} // namespace doris_udf
