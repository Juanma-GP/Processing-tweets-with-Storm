package util;

public class Persona {
    String screenName;
    long idKlout;
    double scoreKlout;

    public Persona(String screenName, long idKlout, double scoreKlout) {
        this.screenName = screenName;
        this.idKlout = idKlout;
        this.scoreKlout = scoreKlout;
    }

    public String getScreenName() {
        return screenName;
    }

    public void setScreenName(String screenName) {
        this.screenName = screenName;
    }

    public long getIdKlout() {
        return idKlout;
    }

    public void setIdKlout(int idKlout) {
        this.idKlout = idKlout;
    }

    public double getScoreKlout() {
        return scoreKlout;
    }

    public void setScoreKlout(int scoreKlout) {
        this.scoreKlout = scoreKlout;
    }
}
