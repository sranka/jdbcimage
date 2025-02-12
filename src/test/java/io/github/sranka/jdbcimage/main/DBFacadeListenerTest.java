package io.github.sranka.jdbcimage.main;

import io.github.sranka.jdbcimage.main.listener.DummyListener;
import io.github.sranka.jdbcimage.main.listener.OracleRestartGlobalSequenceListener;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DBFacadeListenerTest {

    @Test
    public void getInstance() {
        assertTrue(DBFacadeListener.getInstance("Dummy") instanceof DummyListener);
    }

    @Test
    public void getInstances() {
        assertTrue(DBFacadeListener.getInstances(null).isEmpty());
        assertTrue(DBFacadeListener.getInstances("").isEmpty());

        List<DBFacadeListener> listeners1 = DBFacadeListener.getInstances("Dummy");
        assertEquals(1, listeners1.size());
        assertEquals(DummyListener.class, listeners1.get(0).getClass());

        List<DBFacadeListener> listeners2 = DBFacadeListener.getInstances(" Dummy , OracleRestartGlobalSequence ");
        assertEquals(2, listeners2.size());
        assertEquals(DummyListener.class, listeners2.get(0).getClass());
        assertEquals(OracleRestartGlobalSequenceListener.class, listeners2.get(1).getClass());
    }
}