package ldbc.snb.datagen.util;

import ldbc.snb.datagen.hadoop.LdbcDatagen;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.TreeMap;

public class ScaleFactors {

    public final TreeMap<String, ScaleFactor> value;

    public static final String SCALE_FACTORS_FILE = "scale_factors.xml";
    public static final ScaleFactors INSTANCE = new ScaleFactors();

    private ScaleFactors() {
        try {
            value = new TreeMap<>();
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(LdbcDatagen.class.getResourceAsStream("/" + SCALE_FACTORS_FILE));
            doc.getDocumentElement().normalize();

            System.out.println("Reading scale factors..");
            NodeList nodes = doc.getElementsByTagName("scale_factor");
            for (int i = 0; i < nodes.getLength(); i++) {
                Node node = nodes.item(i);
                if (node.getNodeType() == Node.ELEMENT_NODE) {
                    Element element = (Element) node;
                    String scaleFactorName = element.getAttribute("name");
                    ScaleFactor scaleFactor = new ScaleFactor();
                    NodeList properties = ((Element) node).getElementsByTagName("property");
                    for (int j = 0; j < properties.getLength(); ++j) {
                        Element property = (Element) properties.item(j);
                        String name = property.getElementsByTagName("name").item(0).getTextContent();
                        String value = property.getElementsByTagName("value").item(0).getTextContent();
                        scaleFactor.properties.put(name, value);
                    }
                    System.out.println("Available scale factor configuration set " + scaleFactorName);
                    value.put(scaleFactorName, scaleFactor);
                }
            }
            System.out.println("Number of scale factors read " + value.size());
        } catch (ParserConfigurationException | IOException | SAXException e) {
            throw new RuntimeException(e);
        }
    }
}
