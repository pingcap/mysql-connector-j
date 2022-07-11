package com.tidb.snapshot;

import java.util.concurrent.atomic.AtomicLong;

public class Ticdc {

    private AtomicLong globalSecondaryTs = new AtomicLong(0);

    private AtomicLong globallasttime = new AtomicLong(0);

    private AtomicLong name = new AtomicLong(0);


    public AtomicLong getGlobalSecondaryTs() {
        return globalSecondaryTs;
    }

    public void setGlobalSecondaryTs(AtomicLong globalSecondaryTs) {
        this.globalSecondaryTs = globalSecondaryTs;
    }

    public AtomicLong getGloballasttime() {
        return globallasttime;
    }

    public void setGloballasttime(AtomicLong globallasttime) {
        this.globallasttime = globallasttime;
    }


    public AtomicLong getName() {
        return name;
    }

    public void setName(AtomicLong name) {
        this.name = name;
    }
}
