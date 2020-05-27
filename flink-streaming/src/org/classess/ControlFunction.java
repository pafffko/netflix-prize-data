package org.classess;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.structures.Film;
import org.structures.Rate;
import org.structures.Score;

public class ControlFunction extends RichCoFlatMapFunction<Film, Rate, Score>
{
    private ValueState<Film> f;
    private ValueState<Rate> r;

    public void open(Configuration config)
    {
        f = getRuntimeContext().getState(new ValueStateDescriptor<Film>("films", Film.class));
        r = getRuntimeContext().getState(new ValueStateDescriptor<Rate>("rates", Rate.class));
    }

    @Override
    public void flatMap1(Film film, Collector<Score> collector) throws Exception
    {
        Rate rate = r.value();

        if(rate != null)
        {
            r.clear();
            collector.collect(new Score(rate.date, film.title, film.year_prod, rate.rate, rate.user_id));
        }
        else
        {
            f.update(film);
        }
    }

    @Override
    public void flatMap2(Rate rate, Collector<Score> collector) throws Exception
    {
        Film film = f.value();

        if(film != null)
        {
            f.clear();
            collector.collect(new Score(rate.date, film.title, film.year_prod, rate.rate, rate.user_id));
        }
        else
        {
            r.update(rate);
        }
    }
}
