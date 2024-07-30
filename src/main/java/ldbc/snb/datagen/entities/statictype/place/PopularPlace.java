package ldbc.snb.datagen.entities.statictype.place;

public class PopularPlace {

    private String name;
    private double latitude;
    private double longitude;

    public PopularPlace(String name, double latitude, double longitude) {
        this.name = name;
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getLatitude() {
        return latitude;
    }

    public double getLongitude() {
        return longitude;
    }

}
