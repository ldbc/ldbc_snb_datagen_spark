package ldbc.snb.datagen.util;

class Pair {
    private double l;
    private String r;

    public Pair(double l, String r) {
        this.l = l;
        this.r = r;
    }

    public double getL() {
        return l;
    }

    public String getR() {
        return r;
    }

    public void setL(double l) {
        this.l = l;
    }

    public void setR(String r) {
        this.r = r;
    }
}
