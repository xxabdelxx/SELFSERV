package project.selfserv.configuration;
import java.io.File;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import org.w3c.dom.Node;
import org.w3c.dom.Element;

public class ConfigManager {
	
	public static ConfigManager ConfigManagerInstance = new ConfigManager();
	
	File inputFile = new File("config.xml");
	private static final Logger LOG = LoggerFactory.getLogger(ConfigManager.class);
	DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
	DocumentBuilder dBuilder;
	Document doc;
	
	//############### Config Primitives ###################
	//--- KAA config
    private String KEYS_DIR;
    private String USER_EXTERNAL_ID;
    private String USER_ACCESS_TOKEN;
    //--- MongoDb config
    private String url;
	private String collectionName;
	private int SAMPLINGFREQUENCY = 0;
	private float ALPHA_MAX_ANGLE = 0F;
	private float MIN_GlUCOMETER_THRESHOLD = 0.0F;
	private float MAX_CGM = 0.0F;
	private float MAX_HEARTRATE = 0.0F;
	private float MAX_TEMPERATURE = 0.0F;
	private float MAX_ACCELEROMETER = 0.0F;
	private float MAX_GSR = 0.0F;
	
	public ConfigManager() {
		// TODO Auto-generated constructor stub
		try 
		{
			dBuilder = dbFactory.newDocumentBuilder();
			doc = dBuilder.parse(inputFile);
			doc.getDocumentElement().normalize();
			this.loadConfig();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public String getKEYS_DIR() {
		return KEYS_DIR;
	}

	public String getUSER_EXTERNAL_ID() {
		return USER_EXTERNAL_ID;
	}

	public String getUSER_ACCESS_TOKEN() {
		return USER_ACCESS_TOKEN;
	}
	
    public String getUrl() {
		return url;
	}

	public String getCollectionName() {
		return collectionName;
	}

	public int getSAMPLINGFREQUENCY() {
		return SAMPLINGFREQUENCY;
	}

	public float getALPHA_MAX_ANGLE() {
		return ALPHA_MAX_ANGLE;
	}

	public float getMIN_GlUCOMETER_THRESHOLD() {
		return MIN_GlUCOMETER_THRESHOLD;
	}

	public float getMAX_CGM() {
		return MAX_CGM;
	}

	public float getMAX_HEARTRATE() {
		return MAX_HEARTRATE;
	}

	public float getMAX_TEMPERATURE() {
		return MAX_TEMPERATURE;
	}

	public float getMAX_ACCELEROMETER() {
		return MAX_ACCELEROMETER;
	}

	public float getMAX_GSR() {
		return MAX_GSR;
	}

	public void loadConfig()
	{
		try 
		{
	            Node nNodeKaa     = doc.getElementsByTagName("kaaconfig").item(0);
	            Node nNodeMongoDb = doc.getElementsByTagName("mongoconfig").item(0);
	            Node nNodeAlgorithm = doc.getElementsByTagName("algorithmconfig").item(0);
	            
	            if (
	            	nNodeKaa.getNodeType() == Node.ELEMENT_NODE 
	            	&& nNodeMongoDb.getNodeType() == Node.ELEMENT_NODE 
	            	&& nNodeAlgorithm.getNodeType() == Node.ELEMENT_NODE
	               )  
	            {
	               Element eElementKaa = (Element) nNodeKaa;
	               USER_ACCESS_TOKEN = eElementKaa.getElementsByTagName("USER_ACCESS_TOKEN").item(0).getTextContent().toString();
	               USER_EXTERNAL_ID  = eElementKaa.getElementsByTagName("USER_EXTERNAL_ID").item(0).getTextContent().toString();
	               KEYS_DIR 		 = eElementKaa.getElementsByTagName("KEYS_DIR").item(0).getTextContent().toString();
	               
	               Element eElementMongoDb = (Element) nNodeMongoDb;
	               url 					= eElementMongoDb.getElementsByTagName("url").item(0).getTextContent().toString();
	               collectionName     	= eElementMongoDb.getElementsByTagName("collectionName").item(0).getTextContent().toString();
	               
	               Element eElementAlgorithm = (Element) nNodeAlgorithm;
	               SAMPLINGFREQUENCY        = Integer.parseInt(eElementAlgorithm.getElementsByTagName("SAMPLINGFREQUENCY").item(0).getTextContent());
	               ALPHA_MAX_ANGLE			= Float.parseFloat(eElementAlgorithm.getElementsByTagName("ALPHA_MAX_ANGLE").item(0).getTextContent());
	               MIN_GlUCOMETER_THRESHOLD = Float.parseFloat(eElementAlgorithm.getElementsByTagName("MIN_GlUCOMETER_THRESHOLD").item(0).getTextContent());
	               MAX_CGM					= Float.parseFloat(eElementAlgorithm.getElementsByTagName("MAX_CGM").item(0).getTextContent());
	               MAX_HEARTRATE			= Float.parseFloat(eElementAlgorithm.getElementsByTagName("MAX_HEARTRATE").item(0).getTextContent());
	               MAX_TEMPERATURE			= Float.parseFloat(eElementAlgorithm.getElementsByTagName("MAX_TEMPERATURE").item(0).getTextContent());
	               MAX_ACCELEROMETER		= Float.parseFloat(eElementAlgorithm.getElementsByTagName("MAX_ACCELEROMETER").item(0).getTextContent());
	               MAX_GSR					= Float.parseFloat(eElementAlgorithm.getElementsByTagName("MAX_GSR").item(0).getTextContent());
	               
	               
	            }
	    }
		catch (Exception e)
		{
	         e.printStackTrace();
	    }

	}
	
}
