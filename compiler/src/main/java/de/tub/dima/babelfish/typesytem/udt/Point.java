package de.tub.dima.babelfish.typesytem.udt;

@UserDefinedType(name = "point")
public final class Point implements UDT {

    public int x;
    public int y;

    public Point(int x, int y) {
        this.x = x;
        this.y = y;
    }

    public double distance(Point otherPoint){
       return Math.sqrt(Math.pow(otherPoint.x - this.x, 2) + Math.pow(otherPoint.y - this.y, 2));
    }
}
