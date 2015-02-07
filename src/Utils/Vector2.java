package Utils;

public class Vector2<T1, T2, T3> {
    protected T1 v1;
    protected T2 v2;
    protected T3 v3;
    public Vector2() {

    }

    public Vector2(T1 v1, T2 v2, T3 v3) {
            this.v1 = v1;
            this.v2 = v2;
            this.v3 = v3;
    }

    public T1 getV1() {
            return v1;
    }

    public T2 getV2() {
            return v2;
    }
    public T3 getV3() {
        return v3;
}

    @Override
    public String toString() {
            return v1 + "-" + v2;
    }
}
