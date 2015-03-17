package model;

/**
 * @author timling
 * @since 2015/3/17
 */
public class Column {
    public String from;
    public String to;

    @Override
    public String toString() {
        return "Column{" +
                "from='" + from + '\'' +
                ", to='" + to + '\'' +
                '}';
    }
}
