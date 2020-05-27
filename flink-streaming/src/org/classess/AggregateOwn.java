package org.classess;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.structures.Eval;
import org.structures.Score;
import org.structures.TempEval;

public class AggregateOwn
        implements AggregateFunction<Score, TempEval, Eval>
{

    @Override
    public TempEval createAccumulator()
    {
        return new TempEval();
    }

    @Override
    public TempEval add(Score score, TempEval tempEval)
    {
        TempEval te = new TempEval();
        te.title = score.title;
        te.year_prod = score.year_prod;
        te.counts = 1 + tempEval.counts;
        te.rates = score.rate + tempEval.rates;
        return te;
    }

    @Override
    public Eval getResult(TempEval tempEval)
    {
        Eval e = new Eval();
        e.title = tempEval.title;
        e.year_prod = tempEval.year_prod;
        e.count = tempEval.counts;
        e.average = (double) tempEval.rates / (double) tempEval.counts;
        return e;
    }

    @Override
    public TempEval merge(TempEval tempEval, TempEval acc1)
    {
        TempEval te = new TempEval();
        te.title = tempEval.title;
        te.year_prod = tempEval.year_prod;
        te.counts = tempEval.counts + acc1.counts;
        te.rates = tempEval.rates + acc1.rates;
        return te;
    }
}
