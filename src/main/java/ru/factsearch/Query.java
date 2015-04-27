package ru.factsearch;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.core.FileAppender;
import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.cli.*;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.BufferedOutputStream;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.StringJoiner;

/**
 *
 */
public class Query {
    private static Logger log;
    private static final int _batchSize = 1000;
    private static final DecimalFormat decimalFormat = new DecimalFormat("###,###");
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static String[] keyColumnNames;
    private static DataType[] keyColumnTypes;

    @SuppressWarnings("static-access")
    public static void main(String[] args){
        // Parse command line, set config variables
        Options options = new Options();
        options.addOption(OptionBuilder
                .withArgName("host addresses")
                .hasArg()
                .withDescription(" connection point node host")
                .withLongOpt("host")
                .create());
        options.addOption(OptionBuilder
                .withArgName("port")
                .hasArg()
                .withDescription(" connection point node port")
                .withLongOpt("port")
                .create());
        options.addOption(OptionBuilder
                .withArgName("username")
                .hasArg()
                .withDescription(" username for authentication")
                .withLongOpt("user")
                .create());
        options.addOption(OptionBuilder
                .withArgName("password")
                .hasArg()
                .withDescription(" password for authentication")
                .withLongOpt("pass")
                .create());
        options.addOption(OptionBuilder
                .withArgName("cql")
                .hasArg()
                .withDescription(" CQL query")
                .withLongOpt("cql")
                .isRequired()
                .create());
        options.addOption(OptionBuilder
                .withArgName("column_name, column_name")
                .hasArgs(10)
                .withValueSeparator(',')
                .isRequired()
                .withDescription("Names of key columns. If there are more than one bigint/int/varint column specified Sphinx key will be generated from key columns using hash function.")
                .withLongOpt("keys")
                .create());
        options.addOption(OptionBuilder
                .withArgName("debug file name")
                .hasArg()
                .withDescription(" turns debug mode. You need to specify debug file name.")
                .withLongOpt("debug")
                .create());
        CommandLineParser parser = new BasicParser();
        String cql = "";
        String[] connectionPoints = null;
        int connectionPointPort = 0;
        String user = null;
        String pass = null;
        boolean isDebugOn = false;
        try {
            CommandLine cmd = parser.parse( options, args );
            if (cmd.hasOption("host")) {
                connectionPoints = cmd.getOptionValue("host").split(",");
            } else {
                connectionPoints = new String[]{"localhost"};
            }
            if (cmd.hasOption("port")) {
                connectionPointPort = Integer.parseInt(cmd.getOptionValue("port"));
            } else {
                connectionPointPort = 9042;
            }
            if (cmd.hasOption("user")){
                user = cmd.getOptionValue("user");
                if (cmd.hasOption("pass")) {
                    pass = cmd.getOptionValue("pass");
                } else {
                    pass = "";
                }
            }
            if (cmd.hasOption("debug")) {
                isDebugOn = true;
                setUpLogger(cmd.getOptionValue("debug"));
            }

            int idx = 0;
            String[] tmpKeyColumnNames = new String[100];
            for (String keyColumnName: cmd.getOptionValues("keys")){
                if (!"".equals(keyColumnName)){
                    tmpKeyColumnNames[idx++] = keyColumnName;
                }
            }
            keyColumnNames = Arrays.copyOf(tmpKeyColumnNames, idx);
            cql = cmd.getOptionValue("cql");
        } catch (ParseException|NumberFormatException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("query -keys col1,col2,... [-other_options] -cql <CQL QUERY>\n", options);
            System.exit(-1);
        }

        Cluster cluster;
        if (user != null) {
            cluster = Cluster.builder()
                    .addContactPoints(connectionPoints)
                    .withPort(connectionPointPort)
                    .withCredentials(user, pass)
                    .withSocketOptions(new SocketOptions().setReadTimeoutMillis(40000))
                    .withReconnectionPolicy(new ExponentialReconnectionPolicy(500, 300000))
                    .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
                    .build();
        } else {
            cluster = Cluster.builder()
                    .addContactPoints(connectionPoints)
                    .withPort(connectionPointPort)
                    .withSocketOptions(new SocketOptions().setReadTimeoutMillis(40000))
                    .build();
        }
        XMLOutputFactory xof =  XMLOutputFactory.newInstance();
        long totalTimer = System.nanoTime();
        try (Session session = cluster.connect()) {
            XMLStreamWriter writer = xof.createXMLStreamWriter(new BufferedOutputStream(System.out), "UTF-8");
            writer.writeStartDocument("utf-8", "1.0");
            writer.setPrefix("sphinx", "sphinx");
            writer.writeStartElement("sphinx", "docset");
            Statement statement = new SimpleStatement(cql);
            statement.setFetchSize(_batchSize);
            ResultSet rs = session.execute(statement);
            int counter = 0;
            int total = 0;
            long timer = System.nanoTime();
            for (Row row : rs) {
                processRow(row, writer, rs.getColumnDefinitions());
                if (isDebugOn && counter++ > _batchSize) {
                    total = total + counter;
                    log.debug("Read records: {} processing time: {} msec", total, durationFormatted(timer));
                    counter = 0;
                    timer = System.nanoTime();
                }
            }
            writer.writeCharacters("\n");
            writer.writeEndElement(); // sphinx:docset
            writer.flush();
            writer.close();
            if (isDebugOn) log.debug("Query export successfully. Total processing time: {} msec", durationFormatted(totalTimer));
        } catch (Exception e) {
            if (isDebugOn) {
                log.error("Exception: ", e);
            } else {
                e.printStackTrace();
            }
        } finally {
            cluster.close();
        }
    }

    private static void processRow(Row row, XMLStreamWriter writer, ColumnDefinitions columnDefinitions) throws XMLStreamException{
        writer.writeCharacters("\n");
        writer.writeStartElement("sphinx", "document");
        writer.writeAttribute("id", getId(row, columnDefinitions));
        for (ColumnDefinitions.Definition definition: columnDefinitions) {
            writer.writeStartElement(definition.getName());
            writeValue(row, definition.getName(), definition.getType(), writer);
            writer.writeEndElement();
        }
        writer.writeEndElement(); // sphinx:document
    }

    private static String getId(Row row, ColumnDefinitions columnDefinitions){
        if (keyColumnTypes == null) {
            keyColumnTypes = new DataType[keyColumnNames.length];
            for(int i = 0; i < keyColumnNames.length; i++){
                keyColumnTypes[i] = columnDefinitions.getType(keyColumnNames[i]);
            }
        }
        if (keyColumnNames.length == 1 &&
                (DataType.bigint().equals(keyColumnTypes[0]) || DataType.cint().equals(keyColumnTypes[0]) || DataType.varint().equals(keyColumnTypes[0]))) {
            return Integer.toString(row.getInt(keyColumnNames[0]));
        }

        long hashBase = 0;
        StringJoiner sj = new StringJoiner(" ");
        for (int i = 0; i < keyColumnNames.length; i++){
            // If at least one of key columns is int or long use it for hashBase
            if ((DataType.cint().equals(keyColumnTypes[i]) || DataType.bigint().equals(keyColumnTypes[i])) && hashBase == 0) {
                hashBase = row.getInt(keyColumnNames[i]);
            } else {
                // other columns concatenate into big string
                sj.add(getValue(row, keyColumnNames[i], keyColumnTypes[i]));
            }
        }

        return Long.toString(getStringKey(hashBase, sj.toString()));
    }

    private static void writeValue(Row row, String name, DataType dataType, XMLStreamWriter writer) throws XMLStreamException{
        if (DataType.ascii().equals(dataType) || DataType.text().equals(dataType) || DataType.varchar().equals(dataType)){
            parseString(row.getString(name), writer);
        } else if (DataType.blob().equals(dataType)){
            writer.writeCData(row.getBytes(name).toString());
        } else {
            writer.writeCharacters(getValue(row, name, dataType));
        }
    }

    private static String getValue(Row row, String name, DataType dataType){
        if (DataType.cint().equals(dataType)){
            return Integer.toString(row.getInt(name));
        } else if (DataType.bigint().equals(dataType)){
            return Long.toString(row.getLong(name));
        } else if (DataType.ascii().equals(dataType) || DataType.text().equals(dataType) || DataType.varchar().equals(dataType)){
            return row.getString(name);
        } else if (DataType.cboolean().equals(dataType)){
            return Boolean.toString(row.getBool(name));
        } else if (DataType.cdouble().equals(dataType)){
            return Double.toString(row.getDouble(name));
        } else if (DataType.blob().equals(dataType)){
            return row.getBytes(name).toString();
        } else if (DataType.cfloat().equals(dataType)){
            return Float.toString(row.getFloat(name));
        } else if (DataType.counter().equals(dataType)){
            return Integer.toString(row.getInt(name));
        } else if (DataType.decimal().equals(dataType)){
            return row.getDecimal(name).toString();
        } else if (DataType.inet().equals(dataType)){
            return row.getInet(name).toString();
        } else if (DataType.timestamp().equals(dataType) || DataType.timeuuid().equals(dataType)){
            return row.getDate(name).toString();
        } else if (DataType.varint().equals(dataType)) {
            return row.getVarint(name).toString();
        } else {
            for (DataType primitiveType: DataType.allPrimitiveTypes()){
                if (DataType.set(primitiveType).equals(dataType)) {
                    return collectionToString(row.getSet(name, primitiveType.asJavaClass()));
                } else if (DataType.list(primitiveType).equals(dataType)) {
                    return collectionToString(row.getList(name, primitiveType.asJavaClass()));
                }
            }
        }
        return "";
    }

    private static <T> String collectionToString(Collection<T> set){
        if (set.isEmpty()){
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (T obj:set){
            sb.append(obj.toString()).append(" ");
        }
        return sb.substring(0, sb.length() - 1);
    }

    private static void parseString(String text, XMLStreamWriter writer) throws XMLStreamException{
        int strLen = text.length();
        if ((text.charAt(0) == '[' && text.charAt(strLen - 1) == ']') ||
                (text.charAt(0) == '{' && text.charAt(strLen-1) == '}')){
            try {
                String parsedJson = "";
                List<Integer[]> sense = objectMapper.readValue(text, new TypeReference<List<Integer[]>>() {});
                for (Integer[] mem: sense){
                    StringJoiner sj = new StringJoiner(" ", "<mem>", "</mem>");
                    for (Integer memId: mem){
                        sj.add(memId.toString());
                    }
                    parsedJson += sj.toString();
                }
                writer.writeCData(parsedJson);
            } catch (Exception e) {
                log.warn("JSON is no valid, returned text itself. text: {} ", text);
                writer.writeCharacters(text);
            }
        } else {
            writer.writeCharacters(text);
        }
    }

    public static long getStringKey (long hashBase, String str){
        if (str == null) {
            return 0;
        }
        long hash = hashBase;
        for (char c:str.toCharArray()) {
            hash = c + (hash << 6) + (hash << 16) - hash;
        }
        if (hash > 0) {
            return hash;
        } else {
            return ~hash + 1;
        }
    }

    private static String durationFormatted(long startTimeInNanoseconds){
        return decimalFormat.format((System.nanoTime() - startTimeInNanoseconds) / 1000000L);
    }

    @SuppressWarnings("unchecked")
    private static void setUpLogger(String debugFile){
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();

        FileAppender appender = new FileAppender();
        appender.setContext(loggerContext);
        appender.setFile(debugFile);
        appender.setAppend(false);
        PatternLayoutEncoder encoder = new PatternLayoutEncoder();
        encoder.setContext(loggerContext);
        encoder.setPattern("%d{HH:mm:ss} [%level] %logger{32} - %msg%n");
        encoder.start();
        appender.setEncoder(encoder);
        appender.start();

        log = loggerContext.getLogger("Main");
        log.setLevel(Level.DEBUG);
        log.addAppender(appender);
    }
}
