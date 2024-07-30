package ldbc.snb.datagen.entities.statictype.place;

import java.io.Serializable;

@SuppressWarnings("serial")
public class Place implements Serializable {

    public static final String CITY = "City";
    public static final String COUNTRY = "Country";
    public static final String CONTINENT = "Continent";

    private int id;
    private int zId;
    private String name;
    private double latitude;
    private double longitude;
    private long population;
    private String type;

    public Place() {
    }

    public Place(int id, String name, double longitude, double latitude, int population, String type) {
        this.id = id;
        this.name = name;
        this.longitude = longitude;
        this.latitude = latitude;
        this.population = population;
        this.type = type;
    }

    public int getZId() {
        return zId;
    }

    public void setZId(int zId) {
        this.zId = zId;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public long getPopulation() {
        return population;
    }

    public void setPopulation(long population) {
        this.population = population;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

}
