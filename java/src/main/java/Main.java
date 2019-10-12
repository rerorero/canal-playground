package rerorero.sandbox;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.*;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        CanalConnector connector = CanalConnectors.newSingleConnector(
                new InetSocketAddress(AddressUtils.getHostIp(),11111),
                "example", "", "");
        try {
            connector.connect();
            connector.subscribe();
            Main.loop(connector);
        } finally {
            connector.disconnect();
        }
    }

    private static void loop(CanalConnector conn) {
        while(true) {
            Message msg = conn.getWithoutAck(10);
            long id = msg.getId();
            List<CanalEntry.Entry> entries = msg.getEntries();

            try {
                if (id < 0 || entries.size() == 0) {
                    Thread.sleep(500);
                } else {
                    for (CanalEntry.Entry entry : entries) {
                        CanalEntry.EntryType type = entry.getEntryType();
                        switch (type) {
                            case TRANSACTIONBEGIN:
                                System.out.println("TRANSACTION BEGUIN");
                                break;
                            case ROWDATA:
                                System.out.println("ROWDATA");
                                RowChange row = RowChange.parseFrom(entry.getStoreValue());
                                Main.event(entry.getHeader(), row);
                                break;
                            case TRANSACTIONEND:
                                System.out.println("TRANSACTION END");
                                break;
                            case HEARTBEAT:
                                System.out.println("HEARTBEAT");
                                break;
                            case GTIDLOG:
                                System.out.println("GTIDLOG");
                                break;
                            default:
                                throw new RuntimeException("unknown event: " + type);
                        }
                    }
                }
                conn.ack(id);
            } catch (RuntimeException e) {
                throw e;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (InvalidProtocolBufferException e) {
                System.out.println(e);
            }
        }
    }

    private static void event(Header header, RowChange row) {
        StringBuilder sb = new StringBuilder();
        sb.append("  type=");
        sb.append(row.getEventType().toString());
        sb.append(", pos=");
        sb.append(header.getLogfileName());
        sb.append(", schema=");
        sb.append(header.getSchemaName());
        sb.append(", table=");
        sb.append(header.getTableName());

        sb.append(", sql=");
        sb.append(row.getSql());

        sb.append(System.lineSeparator());

        for (int i = 0; i < row.getRowDatasCount(); i++) {
            RowData data = row.getRowDatas(i);
            sb.append("  ");
            sb.append(i);

            sb.append(" : before=");
            sb.append(data.getBeforeColumnsCount());
            Main.columsToStr(data.getBeforeColumnsList(), sb);

            sb.append(", after=");
            sb.append(data.getAfterColumnsCount());
            Main.columsToStr(data.getAfterColumnsList(), sb);
        }
        System.out.println(sb.toString());
    }

    private static void columsToStr(List<Column> columns, StringBuilder sb) {
        for (Column c: columns) {
            sb.append("[type=");
            sb.append(c.getMysqlType());
            sb.append(", name=");
            sb.append(c.getName());
            sb.append(", value=");
            sb.append(c.getValue());
            sb.append("]");
        }
    }
}
