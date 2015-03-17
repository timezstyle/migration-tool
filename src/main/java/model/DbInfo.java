package model;

/**
 * @author timling
 * @since 2015/3/17
 */
public class DbInfo {
    public String url;
    public String username;
    public String password;

    public DbInfo(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;
    }

}
