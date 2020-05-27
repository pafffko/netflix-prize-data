package org.structures;

import java.util.Date;

public class Rate
{
    public String date;
    public int film_id;
    public int user_id;
    public int rate;

    public Rate()
    {
    }

    public Rate(String date, int film_id, int user_id, int rate)
    {
        this.date = date;
        this.film_id = film_id;
        this.user_id = user_id;
        this.rate = rate;
    }
}
