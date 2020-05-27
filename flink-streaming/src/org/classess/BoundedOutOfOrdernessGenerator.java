package org.classess;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.structures.Score;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;

public class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<Score> {

    public long maxOutOfOrderness = 600000L;

    private long currentMaxTimestamp;

    @Nullable
    @Override
    public Watermark getCurrentWatermark()
    {
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(Score score, long l)
    {
        try
        {
            long timestamp = getTimestamp(score.date);
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }
        catch(Exception ex)
        {
            return 0;
        }
    }

    public long getTimestamp(String s) throws Exception
    {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return format.parse(s).getTime();
    }
}
