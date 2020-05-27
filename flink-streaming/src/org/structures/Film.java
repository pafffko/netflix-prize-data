package org.structures;

public class Film
{
    public int film_id;
    public int year_prod;
    public String title;

    public Film()
    {

    }

    public Film(int film_id, int year_prod, String title)
    {
        this.film_id = film_id;
        this.year_prod = year_prod;
        this.title = title;
    }
}
