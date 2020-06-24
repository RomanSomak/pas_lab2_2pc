package lab;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import org.postgresql.xa.PGXADataSource;

import javax.transaction.xa.Xid;

import java.sql.Statement;

public class XASample {
    javax.sql.XADataSource xaDS1;
    javax.sql.XADataSource xaDS2;
    javax.sql.XAConnection xaconn1;
    javax.sql.XAConnection xaconn2;
    javax.sql.XAConnection xaconn3;
    javax.transaction.xa.XAResource xares1;
    javax.transaction.xa.XAResource xares2;
    javax.transaction.xa.XAResource xares3;
    java.sql.Connection conn1;
    java.sql.Connection conn2;
    java.sql.Connection conn3;



    @EqualsAndHashCode
    @AllArgsConstructor
    public class XID implements Xid {

        private Integer formatId;
        private byte[] globalTxId;
        private byte[] branchQualifier;

        @Override
        public int getFormatId() {
            return formatId;
        }

        @Override
        public byte[] getGlobalTransactionId() {
            return globalTxId;
        }

        @Override
        public byte[] getBranchQualifier() {
            return branchQualifier;
        }
    }


    public static void main(String args[]) throws java.sql.SQLException {
        XASample xat = new XASample();
        xat.runThis(args);
    }

    public void runThis(String[] args) {
        byte[] gtrid = new byte[]{0x45, 0x11, 0x55, 0x66};
        byte[] bqual = new byte[]{0x01, 0x22, 0x00};
        byte[] gtrid2 = new byte[]{0x41, 0x13, 0x55, 0x66};
        byte[] bqual2 = new byte[]{0x00, 0x23, 0x00};
        byte[] gtrid3 = new byte[]{0x41, 0x13, 0x13, 0x66};
        byte[] bqual3 = new byte[]{0x00, 0x13, 0x00};
        int rc1 = 0;
        int rc2 = 0;
        int rc3 = 0;

        try {
            javax.naming.InitialContext context = new javax.naming.InitialContext();

            PGXADataSource flyDataSource = new PGXADataSource();
            flyDataSource.setUrl("jdbc:postgresql://localhost:5432/fly_db");
            flyDataSource.setUser("postgres");
            flyDataSource.setPassword("1111");

            PGXADataSource hotelDataSource = new PGXADataSource();
            hotelDataSource.setUrl("jdbc:postgresql://localhost:5432/hotel_db");
            hotelDataSource.setUser("postgres");
            hotelDataSource.setPassword("1111");

            PGXADataSource amountDataSource = new PGXADataSource();
            amountDataSource.setUrl("jdbc:postgresql://localhost:5432/my_db");
            amountDataSource.setUser("postgres");
            amountDataSource.setPassword("1111");

            xaconn1 = flyDataSource.getXAConnection();
            xaconn2 = hotelDataSource.getXAConnection();
            xaconn3 = amountDataSource.getXAConnection();


            conn1 = xaconn1.getConnection();
            conn2 = xaconn2.getConnection();
            conn3 = xaconn3.getConnection();


            xares1 = xaconn1.getXAResource();
            xares2 = xaconn2.getXAResource();
            xares3 = xaconn3.getXAResource();


            javax.transaction.xa.Xid xid1 = this.new XID(100, gtrid, bqual);
            javax.transaction.xa.Xid xid2 = this.new XID(100, gtrid2, bqual2);
            javax.transaction.xa.Xid xid3 = this.new XID(100, gtrid3, bqual3);


            xares1.start(xid1, javax.transaction.xa.XAResource.TMNOFLAGS);
            xares2.start(xid2, javax.transaction.xa.XAResource.TMNOFLAGS);
            xares3.start(xid3, javax.transaction.xa.XAResource.TMNOFLAGS);


            String insertFlyQuery = "INSERT INTO fly_booking.fly_booking" +
                    "(id, client_name, fly_number, fly_from, fly_to, date) " +
                    "VALUES (3, 'ROMAN', 'BIRD 1', 'HERE', 'THERE', '01/01/2000')";

            Statement statement1 = conn1.createStatement();
            statement1.execute(insertFlyQuery);

            String insertHotelQuery = "INSERT INTO hotel_booking.hotel_booking" +
                    "(id, client_name, hotel_name, arrival, departure) " +
                    "VALUES (3, 'ROMAN', 'MIR', '01/01/2000', '07/01/2000')";

            Statement statement2 = conn2.createStatement();
            statement2.execute(insertHotelQuery);

            String reduceAmount = "UPDATE my_schema.my_table " +
                    "SET amount = amount - 100" +
                    "WHERE id = 111";

            Statement statement3 = conn3.createStatement();
            statement3.execute(reduceAmount);

            xares1.end(xid1, javax.transaction.xa.XAResource.TMSUCCESS);
            xares2.end(xid2, javax.transaction.xa.XAResource.TMSUCCESS);
            xares3.end(xid3, javax.transaction.xa.XAResource.TMSUCCESS);


            try {
                rc1 = xares1.prepare(xid1);
                rc2 = xares2.prepare(xid2);
                rc3 = xares3.prepare(xid3);

                xares1.commit(xid1, false);
                xares2.commit(xid2, false);
                xares3.commit(xid3, false);

            } catch (javax.transaction.xa.XAException xae) {
                System.out.println("Distributed transaction prepare/commit failed. " +
                        "Rolling it back.");
                System.out.println("XAException error code = " + xae.errorCode);
                System.out.println("XAException message = " + xae.getMessage());
                xae.printStackTrace();
                try {
                    xares1.rollback(xid1);
                } catch (javax.transaction.xa.XAException xae1) {
                    System.out.println("distributed Transaction rollback xares1 failed");
                    System.out.println("XAException error code = " + xae1.errorCode);
                    System.out.println("XAException message = " + xae1.getMessage());
                }
                try {
                    xares2.rollback(xid2);
                } catch (javax.transaction.xa.XAException xae2) {
                    System.out.println("distributed Transaction rollback xares2 failed");
                    System.out.println("XAException error code = " + xae2.errorCode);
                    System.out.println("XAException message = " + xae2.getMessage());
                }
                try {
                    xares3.rollback(xid3);
                } catch (javax.transaction.xa.XAException xae3) {
                    System.out.println("distributed Transaction rollback xares3 failed");
                    System.out.println("XAException error code = " + xae3.errorCode);
                    System.out.println("XAException message = " + xae3.getMessage());
                }
            }

            try {
                conn1.close();
                xaconn1.close();
            } catch (Exception e) {
                System.out.println("Failed to close connection 1: " + e.toString());
                e.printStackTrace();
            }
            try {
                conn2.close();
                xaconn2.close();
            } catch (Exception e) {
                System.out.println("Failed to close connection 2: " + e.toString());
                e.printStackTrace();
            }
            try {
                conn3.close();
                xaconn3.close();
            } catch (Exception e) {
                System.out.println("Failed to close connection 3: " + e.toString());
                e.printStackTrace();
            }
        } catch (java.sql.SQLException sqe) {
            System.out.println("SQLException caught: " + sqe.getMessage());
            sqe.printStackTrace();
        } catch (javax.transaction.xa.XAException xae) {
            System.out.println("XA error is " + xae.getMessage());
            xae.printStackTrace();
        } catch (javax.naming.NamingException nme) {
            System.out.println(" Naming Exception: " + nme.getMessage());
        }
    }
}
