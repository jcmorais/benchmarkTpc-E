package benchtpce.parser;

import benchtpce.trace.Trace;
import com.google.common.io.Resources;
import benchtpce.common.ObjectSerializable;
import benchtpce.common.TpcConfig;
import benchtpce.trace.Entry;
import benchtpce.trace.Write;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import benchtpce.tpce.TpcE;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by carlosmorais on 19/11/2016.
 */
public class Parser {
    private static final Logger logger = LoggerFactory.getLogger(Parser.class);

    private Map<Integer,String> readWriteset(NodeList nodes, int start, int end){
        Map<Integer,String> values = new HashMap<Integer, String>();

        for (int i = start; i < end; i++) {
            if(!nodes.item(i).getNodeName().equals("i"))
                continue;

            Node childNode = nodes.item(i);
            String content = childNode.getTextContent();
            int pos=-1;
            String value=null;
            if (content.matches("@(\\d)='(.)+'")){
                pos =  Integer.parseInt(content.substring(1,2));
                value = content.substring(4, content.length()-1);
            }
            else if (content.matches("@(\\d{2})='(.)+'")){
                pos = Integer.parseInt(content.substring(1,3));
                value = content.substring(5, content.length()-1);
            } else if (content.matches("@(\\d)=(.)+")){
                pos = Integer.parseInt(content.substring(1,2));
                value = content.substring(3);
            }
            else if (content.matches("@(\\d){2}=(.)+")){
                pos = Integer.parseInt(content.substring(1,3));
                value = content.substring(4);
            }
            values.put(pos, value);
        }
        return values;
    }


    private Write readWritesetToWrite(Write write, NodeList nodes){
        Map<Integer,String> values = new HashMap<Integer, String>();
        Map<Integer,String> oldValues = new HashMap<Integer, String>();
        TpcE tpcE = new TpcE();

        if (write.getStatement() == Write.Statement.UPDATE) {
            oldValues = readWriteset(nodes, 0, nodes.getLength()/2);
            values = readWriteset(nodes, nodes.getLength()/2, nodes.getLength());
            write.setOldValues(oldValues);

            Map<Integer,String> newValues = new HashMap<Integer, String>();
            for (Integer k : oldValues.keySet()) {
                List<Integer> keys = tpcE.tableKeys.get(write.getTable());
                //System.out.println(""+write + keys +oldValues);
                if(!write.getTable().equals("txn_info") && !write.getTable().equals("seq_trade_id") && keys.contains(k) || !oldValues.get(k).equals(values.get(k)))
                    newValues.put(k, values.get(k));
                write.setValues(newValues);
            }
        }
        else {
            values = readWriteset(nodes, 0, nodes.getLength());
            write.setValues(values);
        }
        return write;
    }

    public Trace readXmlFile(String path){
        Set<Entry> entryList = new TreeSet<>();

        try {
            logger.debug("Parsing file "+path);
            File file = new File(path);
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(file);
            doc.getDocumentElement().normalize();
            NodeList nList = doc.getElementsByTagName("entry");

            for (int temp = 0; temp < nList.getLength(); temp++) {
                Entry entry = new Entry();
                Map<Integer, String> writeset;
                NodeList child = nList.item(temp).getChildNodes();
                boolean commit=false, start=false, rollback=false;

                for (int i = 0; i < child.getLength(); i++) {
                    if(!start){
                        if(child.item(i).getTextContent().contains("BEGIN")) {
                            //avança posições até ao writeset
                            while (!child.item(i).getNodeName().equals("writeset"))
                                i++;

                            writeset = readWriteset(child.item(i).getChildNodes(), 0,
                                    child.item(i).getChildNodes().getLength());
                            entry.setType(writeset.get(2));
                            entry.setStartTimestamp(writeset.get(4));

                            start = true;
                        }
                    }
                    else if(!commit){
                        Write write = new Write();

                        //avança posições até ao statement
                        while (!child.item(i).getNodeName().equals("statement") ) {
                            if(child.item(i).getTextContent().contains("ROLLBACK")) {
                                rollback = true;
                                break;
                            }
                            i++;
                        }

                        if(rollback) {
                            entry.setRollback(true);
                            continue;
                        }

                        String statement = child.item(i).getTextContent().split(" ")[0];
                        String table = child.item(i).getTextContent().split("\\.")[1];

                        if(statement.equals("INSERT"))
                            write.setStatement(Write.Statement.INSERT);
                        else if(statement.equals("UPDATE"))
                            write.setStatement(Write.Statement.UPDATE);
                        else if(statement.equals("DELETE"))
                            write.setStatement(Write.Statement.DELETE);

                        write.setTable(table);

                        //avança posições até ao writeset
                        while (!child.item(i).getNodeName().equals("writeset"))
                            i++;
                        writeset = readWriteset(child.item(i).getChildNodes(), 0,
                                child.item(i).getChildNodes().getLength());
                        write = readWritesetToWrite(write, child.item(i).getChildNodes());

                        //discard txn_info
                        if(!table.equals("txn_info") && !table.equals("seq_trade_id")){
                                entry.addWrite(write);
                        }
                        else if(table.equals("txn_info"))  {
                            //avança posições até ao statement
                            while (!child.item(i).getNodeName().equals("transaction_id")) {
                                if(child.item(i).getTextContent().contains("ROLLBACK")) {
                                    rollback = true;
                                    break;
                                }
                                i++;
                            }

                            if(rollback) {
                                entry.setRollback(true);
                                continue;
                            }

                            int id = Integer.parseInt(child.item(i).getTextContent());
                            entry.setId(id);
                            entry.setCommitTimestamp(writeset.get(4));

                            commit = true;
                        }
                    }
                }
                //só estou a adicionar entrys com writeset
                if(entry.getWriteset().size()>0)
                    entryList.add(entry);
            }

        } catch (SAXException e) {
            e.printStackTrace();
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return new Trace(entryList);
    }


    public List<Trace> loadTrace(TpcConfig tpcConfig) {
        List<Trace> res = new ArrayList<>();
        String path;

        if (tpcConfig.isLoadFromXML())
            return loadTraceFromXML(tpcConfig.getLoadPath(), tpcConfig.getFilesList());
        else
            return loadTraceFromSerialized(tpcConfig.getLoadPath(), tpcConfig.getFilesList());
    }

    private List<Trace> loadTraceFromSerialized(String loadPath, List<String> filesList) {
        List<Trace> res = new ArrayList<>();
        String path;

        for (String file : filesList) {
            path = Resources.getResource(loadPath+file).getPath();
            res.add((Trace) ObjectSerializable.FileToObject(path));
        }
        return res;
    }


    public List<Trace> loadTraceFromXML(String loadPath, List<String> filesList) {
        List<Trace> res = new ArrayList<>();
        String path;

        for (String file : filesList) {
            path = Resources.getResource(loadPath+file).getPath();
            Trace t = readXmlFile(path);
            t.setFilename(file);
            res.add(t);
        }
        return res;
    }
}
