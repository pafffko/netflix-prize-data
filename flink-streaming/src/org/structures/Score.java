package org.structures;

import java.util.Date;

public class Score
{
    public String date;
    public String title;
    public int year_prod;
    public int rate;
    public int user_id;

    public Score()
    {

    }

    public Score(String date, String title, int year_prod, int rate, int user_id)
    {
        this.date = date;
        this.title = title;
        this.year_prod = year_prod;
        this.rate = rate;
        this.user_id = user_id;
    }
}
