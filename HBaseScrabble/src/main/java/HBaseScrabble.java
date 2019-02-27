import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.util.*;


public class HBaseScrabble {
    private static final String LEXICON = "LEXICON";
	private static final String DATE = "DATE";
	private static final String DIVISION = "DIVISION";
	private static final String ROUND = "ROUND";
	private static final String LOOSER_POS = "LOOSER_POS";
	private static final String LOOSER_NEW_RATING = "LOOSER_NEW_RATING";
	private static final String LOOSER_OLD_RATING = "LOOSER_OLD_RATING";
	private static final String LOOSER_NAME = "LOOSER_NAME";
	private static final String LOOSER_ID = "LOOSER_ID";
	private static final String WINNER_POS = "WINNER_POS";
	private static final String WINNER_SCORE = "WINNER_SCORE";
	private static final String WINNER_OLD_RATING = "WINNER_OLD_RATING";
	private static final String WINNER_NEW_RATING = "WINNER_NEW_RATING";
	private static final String LOOSER_SCORE = "LOOSER_SCORE";
	private static final String WINNER_NAME = "WINNER_NAME";
	private static final String TIE = "TIE";
	private static final String TOURNEY_ID = "TOURNEY_ID";
	private static final String GAME_ID = "GAME_ID";
	private static final String WINNER_ID = "WINNER_ID";
	private Configuration config;
    private HBaseAdmin hBaseAdmin;
    private static final String DB_NAME = "SGAME";
    private byte[] table = Bytes.toBytes(DB_NAME);
    byte[] cf = Bytes.toBytes("ScrabbleGame");
    private static final String REGEX = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";
    byte[] gameId = Bytes.toBytes(GAME_ID);
    byte[] tourneyId = Bytes.toBytes(TOURNEY_ID);
    byte[] tie = Bytes.toBytes(TIE);
    byte[] winnerId = Bytes.toBytes(WINNER_ID);
    byte[] winnerName = Bytes.toBytes(WINNER_NAME);
    byte[] winnerScore = Bytes.toBytes(WINNER_SCORE);
    byte[] winnerOldRating = Bytes.toBytes(WINNER_OLD_RATING);
    byte[] winnerNewRating = Bytes.toBytes(WINNER_NEW_RATING);
    byte[] winnerPos = Bytes.toBytes(WINNER_POS);
    byte[] looserId = Bytes.toBytes(LOOSER_ID);
    byte[] looserName = Bytes.toBytes(LOOSER_NAME);
    byte[] looserScore = Bytes.toBytes(LOOSER_SCORE);
    byte[] looserOldRating = Bytes.toBytes(LOOSER_OLD_RATING);
    byte[] looserNewRating = Bytes.toBytes(LOOSER_NEW_RATING);
    byte[] looserPos = Bytes.toBytes(LOOSER_POS);
    byte[] round = Bytes.toBytes(ROUND);
    byte[] division = Bytes.toBytes(DIVISION);
    byte[] date = Bytes.toBytes(DATE);
    byte[] lexicon = Bytes.toBytes(LEXICON);

    HTable hTable;
    
    /**
     * The Constructor. Establishes the connection with HBase.
     * @param zkHost
     * @throws IOException
     */
    public HBaseScrabble(String zkHost) throws IOException {
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", zkHost.split(":")[0]);
        config.set("hbase.zookeeper.property.clientPort", zkHost.split(":")[1]);
        HBaseConfiguration.addHbaseResources(config);
        this.hBaseAdmin = new HBaseAdmin(config);
        hTable = new HTable(config,table);
    }

    public void createTable() throws IOException {


        HTableDescriptor hTable = new HTableDescriptor(TableName.valueOf(DB_NAME));
        HColumnDescriptor family = new HColumnDescriptor(cf);
        family.setMaxVersions(10); 
        hTable.addFamily(family);

        this.hBaseAdmin.createTable(hTable);
    }

    public void loadTable(String folder)throws IOException{


        List<Put> listPuts = new ArrayList<>();
        Put put=null;
        int numberOfLine = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(folder + "/scrabble_games.csv"))) {
            String line;	   
            while ((line = br.readLine()) != null) {
            String[] csvValues = line.split(REGEX);
                if (numberOfLine!= 0) {
                     byte[] key = this.getKey(
                     Bytes.toBytes(Integer.parseInt(csvValues[1])),Bytes.toBytes(Integer.parseInt(csvValues[0])));
                     put = new Put(key);
                     long ts = System.currentTimeMillis();
                     put.add(cf, gameId, ts, Bytes.toBytes(Integer.parseInt(csvValues[0])));
                     put.add(cf, tourneyId, ts, Bytes.toBytes(Integer.parseInt(csvValues[1])));
                     put.add(cf, tie, ts, Bytes.toBytes(csvValues[2]));
                     put.add(cf, winnerId, ts, Bytes.toBytes(Integer.parseInt(csvValues[3])));
                     put.add(cf, winnerName, ts, Bytes.toBytes(csvValues[4]));
                     put.add(cf, winnerScore, ts, Bytes.toBytes(Integer.parseInt(csvValues[5])));
                     put.add(cf, winnerOldRating, ts, Bytes.toBytes(Integer.parseInt(csvValues[6])));
                     put.add(cf, winnerNewRating, ts, Bytes.toBytes(Integer.parseInt(csvValues[7])));
                     put.add(cf, winnerPos, ts, Bytes.toBytes(Integer.parseInt(csvValues[8])));
                     put.add(cf, looserId, ts, Bytes.toBytes(Integer.parseInt(csvValues[9])));
                     put.add(cf, looserName, ts, Bytes.toBytes(csvValues[10]));
                     put.add(cf, looserScore, ts, Bytes.toBytes(Integer.parseInt(csvValues[11])));
                     put.add(cf, looserOldRating, ts, Bytes.toBytes(Integer.parseInt(csvValues[12])));
                     put.add(cf, looserNewRating, ts, Bytes.toBytes(Integer.parseInt(csvValues[13])));
                     put.add(cf, looserPos, ts, Bytes.toBytes(Integer.parseInt(csvValues[14])));
                     put.add(cf, round, ts, Bytes.toBytes(Integer.parseInt(csvValues[15])));
                     put.add(cf, division, ts, Bytes.toBytes(Integer.parseInt(csvValues[16])));
                     put.add(cf, date, ts, Bytes.toBytes(csvValues[17]));
                     put.add(cf, lexicon, ts, Bytes.toBytes(csvValues[18]));
                     listPuts.add(put);
                     if(numberOfLine % 10000 == 0) {
                        hTable.put(listPuts);
                        listPuts = new ArrayList<>();
                     }
                }
                numberOfLine++;

            }
            hTable.put(listPuts);
        }

        System.out.println("End of load table with " + String.valueOf(numberOfLine) + " rows affected");
        System.exit(-1);
    }



    /**
     * This method generates the key
     * @param csvValues The value of each column
     * @param keyTable The position of each value that is required to create the key in the array of csvValues.
     * @return The encoded key to be inserted in HBase
     */
    private byte[] getKey(String[] csvValues, int[] keyTable) {
        String keyString = "";
        for (int keyId : keyTable){
            keyString += Bytes.toBytes(csvValues[keyId]);
        }
        byte[] key = Bytes.toBytes(keyString);

        return key;
    }


    private byte[] getKey(byte[] key1, byte[] key2){

        byte[] key = new byte[8];

        key[0] = key1[0];
        key[1] = key1[1];
        key[2] = key1[2];
        key[3] = key1[3];
        key[4] = key2[0];
        key[5] = key2[1];
        key[6] = key2[2];
        key[7] = key2[3];

        return key;
    }

    public List<String> query1(String tourneyid, String winnername) throws IOException {

        List<String>  list = new ArrayList<>();

        int tourneyPlusOne = Integer.parseInt(tourneyid) +1;

        byte[] key = this.getKey(Bytes.toBytes(Integer.parseInt(tourneyid)), Bytes.toBytes(0));
        byte[] key2 = this.getKey(Bytes.toBytes(tourneyPlusOne)  , Bytes.toBytes(0));

        Scan scan = new Scan(key, key2);
        scan.addColumn(cf,winnerName);
        scan.addColumn(cf,looserId);

        Filter filter = new SingleColumnValueFilter(cf, winnerName, 
        		CompareFilter.CompareOp.EQUAL,Bytes.toBytes(winnername));
        scan.setFilter(filter);
        ResultScanner result = hTable.getScanner(scan);

        for(Result r: result) {
            int id = Bytes.toInt(r.getValue(cf,looserId));
            list.add(String.valueOf(id));
        }
        result.close();
        return  list;
    }

    public List<String> query2(String firsttourneyid, String lasttourneyid) throws IOException {

        List<String>  list = new ArrayList<>();
        Map<Integer, Map<Integer, Integer>> recordsByPlayer = new HashMap<>();

        int begin = Integer.parseInt(firsttourneyid);
        int end = Integer.parseInt(lasttourneyid);
        int range = end - begin;

        byte[] key = this.getKey(Bytes.toBytes(begin), Bytes.toBytes(0));
        byte[] key2 = this.getKey(Bytes.toBytes(end)  , Bytes.toBytes(0));
        Scan scan = new Scan(key, key2);
        scan.addColumn(cf,Bytes.toBytes(WINNER_ID));
        scan.addColumn(cf,Bytes.toBytes(LOOSER_ID));
        scan.addColumn(cf,Bytes.toBytes(TOURNEY_ID));

        ResultScanner result = hTable.getScanner(scan);

        Result res = result.next();

        while (res!= null && !res.isEmpty()) {
            int looserId = Bytes.toInt(res.getValue(cf, Bytes.toBytes(LOOSER_ID)));
            int winnerId = Bytes.toInt(res.getValue(cf, Bytes.toBytes(WINNER_ID)));
            int tourneyId = Bytes.toInt(res.getValue(cf, Bytes.toBytes(TOURNEY_ID)));


            // verificar si el looser id ya existe en el mapa
            // si ya existe entonces actualizar su respectivo mapa para el actual torneo
            // de lo contrario crear un mapa y agregar una participacion en el actual torneo
            if (recordsByPlayer.containsKey(looserId)) {
                Map<Integer, Integer> attendances = recordsByPlayer.get(looserId);
                if (attendances.containsKey(tourneyId)) {
                    attendances.put(tourneyId, attendances.get(tourneyId) + 1);
                    recordsByPlayer.put(looserId, attendances);
                } else {
                    attendances.put(tourneyId, 1);
                    recordsByPlayer.put(looserId, attendances);
                }

            } else {
                Map<Integer, Integer> attendances = new HashMap<>();
                attendances.put(tourneyId, 1);
                recordsByPlayer.put(looserId, attendances);
            }

            // verificar si el winner id ya existe en el mapa
            // si ya existe entonces actualizar su respectivo mapa para el actual torneo
            // de lo contrario crear un mapa y agregar una participacion en el actual torneo
            if (recordsByPlayer.containsKey(winnerId)) {
                Map<Integer, Integer> attendances = recordsByPlayer.get(winnerId);

                if (attendances.containsKey(tourneyId)) {
                    attendances.put(tourneyId, attendances.get(tourneyId) + 1);
                    recordsByPlayer.put(winnerId, attendances);
                } else {
                    attendances.put(tourneyId, 1);
                    recordsByPlayer.put(winnerId, attendances);
                }

            } else {
                Map<Integer, Integer> attendances = new HashMap<>();
                attendances.put(tourneyId, 1);
                recordsByPlayer.put(winnerId, attendances);
            }
            res = result.next();
        }


        // verificar quienes han participado más de una vez en cada torneo.
        for(Integer idPlayer : recordsByPlayer.keySet()) {
            Map<Integer,Integer> m1 = recordsByPlayer.get(idPlayer);
            boolean isLessThanTwo = false;
            for(Integer val : m1.values()) {
                if (val < 2) {
                    isLessThanTwo = true;
                }
            }

            if (!isLessThanTwo && m1.values().size() == range) {
                list.add(String.valueOf(idPlayer));
            }

        }
        result.close();
        return  list;

    }

    public List<String> query3(String tourneyid) throws IOException {
        List<String>  list = new ArrayList<>();

        int tourneyPlusOne = Integer.parseInt(tourneyid) +1;

        byte[] key = this.getKey(Bytes.toBytes(Integer.parseInt(tourneyid)), Bytes.toBytes(0));
        byte[] key2 = this.getKey(Bytes.toBytes(tourneyPlusOne)  , Bytes.toBytes(0));

        Scan scan = new Scan(key, key2);
        scan.addColumn(cf, looserId);
        scan.addColumn(cf, gameId);
        scan.addColumn(cf, winnerId);
        scan.addColumn(cf, tie);

        Filter filter = new SingleColumnValueFilter(cf, tie, CompareFilter.CompareOp.EQUAL,Bytes.toBytes("True"));
        scan.setFilter(filter);
        ResultScanner result = hTable.getScanner(scan);


        for(Result r : result){

            int localGameId = Bytes.toInt(r.getValue(cf, gameId));
            int localLooserId = Bytes.toInt(r.getValue(cf, looserId));
            int localWinnerId = Bytes.toInt(r.getValue(cf, winnerId));

            StringBuilder sb = new StringBuilder();
            sb.append(localGameId);
            sb.append("-");
            sb.append(localLooserId);
            sb.append("-");
            sb.append(localWinnerId);
            list.add(sb.toString());
        }
        System.out.println("The output format is : gameId-looserId-winnerId");
        result.close();
        return  list;
     }


    public static void main(String[] args) throws IOException {
        if(args.length<2){
            System.out.println("Error: \n1)ZK_HOST:ZK_PORT, \n2)action [createTable, loadTable, query1, query2, query3], \n3)Extra parameters for loadTables and queries:\n" +
                    "\ta) If loadTable: csvsFolder.\n " +
                    "\tb) If query1: tourneyid winnername.\n  " +
                    "\tc) If query2: firsttourneyid lasttourneyid.\n  " +
                    "\td) If query3: tourneyid.\n  ");
            System.exit(-1);
        }
        HBaseScrabble hBaseScrabble = new HBaseScrabble(args[0]);
        if(args[1].toUpperCase().equals("CREATETABLE")){
            hBaseScrabble.createTable();
        }
        else if(args[1].toUpperCase().equals("LOADTABLE")){
            if(args.length!=3){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)action [createTables, loadTables], 3)csvsFolder");
                System.exit(-1);
            }
            else if(!(new File(args[2])).isDirectory()){
                System.out.println("Error: Folder "+args[2]+" does not exist.");
                System.exit(-2);
            }
            hBaseScrabble.loadTable(args[2]);
        }
        else if(args[1].toUpperCase().equals("QUERY1")){
            if(args.length!=4){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)query1, " +
                        "3) tourneyid 4) winnername");
                System.exit(-1);
            }

            List<String> opponentsName = hBaseScrabble.query1(args[2], args[3]);
            System.out.println("There are "+opponentsName.size()+" opponents of winner "+args[3]+" that play in tourney "+args[2]+".");
            System.out.println("The list of opponents is: "+Arrays.toString(opponentsName.toArray(new String[opponentsName.size()])));
        }

        else if(args[1].toUpperCase().equals("QUERY2")){
            if(args.length!=4){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)query2, " +
                        "3) firsttourneyid 4) lasttourneyid");
                System.exit(-1);
            }
            List<String> playerNames =hBaseScrabble.query2(args[2], args[3]);
            System.out.println("There are "+playerNames.size()+" players that participates in more than one tourney between tourneyid "+args[2]+" and tourneyid "+args[3]+" .");
            System.out.println("The list of players is: "+Arrays.toString(playerNames.toArray(new String[playerNames.size()])));
        }
        else if(args[1].toUpperCase().equals("QUERY3")){
            if(args.length!=3){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2) query3, " +
                        "3) tourneyid");
                System.exit(-1);
            }
            List<String> games = hBaseScrabble.query3(args[2]);
            System.out.println("There are "+games.size()+" that ends in tie in tourneyid "+args[2]+" .");
            System.out.println("The list of games is: "+Arrays.toString(games.toArray(new String[games.size()])));
        }
        else{
            System.out.println("Error: \n1)ZK_HOST:ZK_PORT, \n2)action [createTable, loadTable, query1, query2, query3], \n3)Extra parameters for loadTables and queries:\n" +
                    "\ta) If loadTable: csvsFolder.\n " +
                    "\tb) If query1: tourneyid winnername.\n  " +
                    "\tc) If query2: firsttourneyid lasttourneyid.\n  " +
                    "\td) If query3: tourneyid.\n  ");
            System.exit(-1);
        }

    }



}
