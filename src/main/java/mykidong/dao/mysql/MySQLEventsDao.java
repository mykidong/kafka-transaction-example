package mykidong.dao.mysql;

import mykidong.api.dao.EventsDao;
import mykidong.domain.avro.events.Events;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Date;

/**
 * Created by mykidong on 2019-09-18.
 */
public class MySQLEventsDao implements EventsDao{

    private static Logger log = LoggerFactory.getLogger(MySQLEventsDao.class);

    private Connection conn;

    public MySQLEventsDao()
    {
        try {
            conn = DriverManager.getConnection(
                    "jdbc:mysql://amaster.example.com/kafkatx", "mykidong", "icarus");

            conn.setAutoCommit(false);
        }catch (SQLException e)
        {
            e.printStackTrace();
        }
    }



    public void saveEventsToDB(Events events) {
        try {
            String sql = "INSERT INTO " + getBackTick() + "record" + getBackTick() + " (" + getBackTick() + "customer_id" + getBackTick() + ", " + getBackTick() + "record" + getBackTick() + ", " + getBackTick() + "when" + getBackTick() + ") VALUES (?,?,?)";
            PreparedStatement ps = conn.prepareStatement(sql);

            ps.setString(1, events.getCustomerId().toString());
            ps.setString(2, events.toString());
            ps.setLong(3, new Date().getTime());
            ps.execute();

        }catch (SQLException e)
        {
            e.printStackTrace();
        }
    }

    private String getBackTick()
    {
        return new Character((char) 96).toString();
    }

    public void saveOffsetsToDB(String topic, int partition, long offset) {
        try {
            boolean hasOffsetEntry = hasOffsetEntry(topic, partition);
            log.info("hasOffsetEntry: [{}]", hasOffsetEntry);

            String sql = null;
            if(hasOffsetEntry) {
                sql = "UPDATE " + getBackTick() + "offset" + getBackTick() + " SET " + getBackTick() + "offset" + getBackTick() + " = ? WHERE " + getBackTick() + "topic" + getBackTick() + " = ? AND " + getBackTick() + "partition" + getBackTick() + " = ?";
                PreparedStatement ps = conn.prepareStatement(sql);

                ps.setLong(1, offset);
                ps.setString(2, topic);
                ps.setInt(3, partition);
                ps.executeUpdate();
            }
            else
            {
                sql = "INSERT INTO " + getBackTick() + "offset" + getBackTick() + " (" + getBackTick() + "topic" + getBackTick() + ", " + getBackTick() + "partition" + getBackTick() + ", " + getBackTick() + "offset" + getBackTick() + ") VALUES (?,?,?)";
                PreparedStatement ps = conn.prepareStatement(sql);

                ps.setString(1, topic);
                ps.setInt(2, partition);
                ps.setLong(3, offset);
                ps.execute();
            }

        }catch (SQLException e)
        {
            e.printStackTrace();
        }
    }

    public void commitDBTransaction() {
        try {
            conn.commit();
        }catch (SQLException e)
        {
            e.printStackTrace();
        }
    }

    private boolean hasOffsetEntry(String topic, int partition) {
        boolean hasOffsetEntry = false;
        try {
            String sql = "SELECT count(*) as cnt FROM " + getBackTick() + "offset" + getBackTick() + " WHERE " + getBackTick() + "topic" + getBackTick() + " = ? AND " + getBackTick() + "partition" + getBackTick() + " = ?";
            PreparedStatement ps = conn.prepareStatement(sql);

            ps.setString(1, topic);
            ps.setInt(2, partition);

            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                hasOffsetEntry = (rs.getLong("cnt") == 0) ? false : true;
            }
        }catch (SQLException e)
        {
            e.printStackTrace();
        }

        return hasOffsetEntry;
    }

    public long getOffsetFromDB(TopicPartition topicPartition) {
        String topic = topicPartition.topic();
        int partition = topicPartition.partition();

        long offset = -1;

        ResultSet rs = null;
        try {
            String sql = "SELECT " + getBackTick() + "offset" + getBackTick() + " as os FROM " + getBackTick() + "offset" + getBackTick() + " WHERE " + getBackTick() + "topic" + getBackTick() + " = ? AND " + getBackTick() + "partition" + getBackTick() + " = ?";
            PreparedStatement ps = conn.prepareStatement(sql);

            ps.setString(1, topic);
            ps.setInt(2, partition);

            rs = ps.executeQuery();
            while (rs.next()) {
                offset = rs.getLong("os");
            }

            // increase one.
            offset = offset + 1;

        }catch (SQLException e)
        {
            e.printStackTrace();
        }

        return offset;
    }
}
