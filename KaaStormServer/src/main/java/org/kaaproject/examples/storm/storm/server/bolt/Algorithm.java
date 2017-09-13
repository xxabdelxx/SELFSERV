package org.kaaproject.examples.storm.storm.server.bolt;

import java.nio.ByteBuffer;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.flume.Event;
import org.kaaproject.examples.storm.storm.server.producer.AvroFlumeEventProducer;
import project.selfserv.kaa.sensors.data.sensorsDataCollection;
import project.selfserv.kaa.sensors.data.acceletometer.acceleroMetersensor;

import org.kaaproject.kaa.server.common.log.shared.KaaFlumeEventReader;
import org.omg.CORBA.Current;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import project.selfserv.configuration.ConfigManager;
import project.selfserv.events.EventsManager;

/**
 * @author q$
 *
 */
@SuppressWarnings("serial")
public class Algorithm implements IRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(Algorithm.class);
    public static final String DEFAULT_FLUME_PROPERTY_PREFIX = "flume-avro-forward";

    private static final KaaFlumeEventReader<sensorsDataCollection> kaaReader = new KaaFlumeEventReader<sensorsDataCollection>(sensorsDataCollection.class);
    private AvroFlumeEventProducer producer;
    private OutputCollector collector;
    
    /*
     * Requirement : 
     * vi is the value read by sensor i at time t and fi = 300 is the sampling
     * frequency i is in [0, 6]; max(i) is the threshold for sensor i, i is in [1, 6]
     * 0 : 
     * 1 : 
     * 2 : 
     * 3 : 
     * 4 : 
     * 5 :
     * 6 : 
     * 
     */
    
    /*************************   Constant  Values for Algorithm  *************************/
    
    //Minimum threshold for sensor s0 : GLUCOMETRE
    private static float MIN_GlUCOMETER_THRESHOLD;
    //The maximum angle
    private static float ALPHA_MAX_ANGLE = -1.0F;
    //a threshold for the glucose value
	private static final float THRESHOLD_GLUCOSE_VALUE = MIN_GlUCOMETER_THRESHOLD * 1.25F ;
	//Max value for Each sensor
	private HashMap<String,Float> MAX_SENSORS_VAL = new HashMap<String,Float>();
    /*************************************************************************************/
    
	/*************************          Initial  Values       ****************************/
    //Initial value of glucose
    private float glucosePrevValue = 0.0F;
    //Vi is the value read by sensor i at time t
    private HashMap<String, Float> sensorsValues = new HashMap<String, Float>();
    //The estimated anticipation time of a potential hypoglycemia
    private float estimatedTimeofHypo = 0.0F;
    //The frequency for sampling the glucose sensor
    private int glucoseSimplingFrequency;
    private int F_SimplingFreq;
    //private int 
    /*************************************************************************************/
    
	/*************************    Variable for sensors' Data  ****************************/
	float CGM_Value = 0F;
	float heartRatesensor_Value =  0F;
	float bodyTemperaturesensor_Value =  0F;
	acceleroMetersensor acceleroMetersensor_Value;
    float xAxis =  0F;
	float yAxis =  0F;
	float zAxis =  0F;
	float galvanicSkinRespsensor_Value =  0F; 
	/*************************************************************************************/
	
	//Angle of the curve 
	private double theta;
	//Slope of the curve
	private double m;						
	
	// Variable for Help
	private long  currentTime;
	private boolean caseTwoReached = false;
	/************************************************************************************/
	
    public Algorithm() {
		super();
		//get Configuration
		MAX_SENSORS_VAL.put("MAX_CGM",ConfigManager.ConfigManagerInstance.getMAX_CGM());
		MAX_SENSORS_VAL.put("MAX_ACCELEROMETER",ConfigManager.ConfigManagerInstance.getMAX_ACCELEROMETER());
		MAX_SENSORS_VAL.put("MAX_GSR",ConfigManager.ConfigManagerInstance.getMAX_GSR());
		MAX_SENSORS_VAL.put("MAX_HEARTRATE",ConfigManager.ConfigManagerInstance.getMAX_HEARTRATE());
		MAX_SENSORS_VAL.put("MAX_TEMPERATURE",ConfigManager.ConfigManagerInstance.getMAX_TEMPERATURE());
		MIN_GlUCOMETER_THRESHOLD 	= ConfigManager.ConfigManagerInstance.getMIN_GlUCOMETER_THRESHOLD();
		ALPHA_MAX_ANGLE 			= ConfigManager.ConfigManagerInstance.getALPHA_MAX_ANGLE();
		glucoseSimplingFrequency 	= ConfigManager.ConfigManagerInstance.getSAMPLINGFREQUENCY();
		//initializing the sampling Freq
		F_SimplingFreq 				= glucoseSimplingFrequency;
	}
    
    public String getFlumePropertyPrefix() {
        return DEFAULT_FLUME_PROPERTY_PREFIX;
    }

	public void setProducer(AvroFlumeEventProducer producer) {
        this.producer = producer;
    }

    @SuppressWarnings("rawtypes")
    public void prepare(Map config, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        Properties sinkProperties  = new Properties();
        LOG.info("Looking for flume properties");
        for (Object key : config.keySet()) {
            if (key.toString().startsWith(this.getFlumePropertyPrefix())) {
                LOG.info("Found:Key:" + key.toString() + ":" + config.get(key));
                sinkProperties.put(key.toString().replace(this.getFlumePropertyPrefix() + ".",""),
                        config.get(key));
            }
        }
    }
    

    public void execute(Tuple input) {
        try 
        {
        	/******************* Get Sensors' Data from Spout ********************/
            CGM_Value = input.getFloatByField("CGM_Value");
            heartRatesensor_Value = input.getFloatByField("HeartRatesensor_Value");
            bodyTemperaturesensor_Value = input.getFloatByField("bodyTemperaturesensor_Value");
			acceleroMetersensor acceleroMetersensor_Value = (acceleroMetersensor) input.getValueByField("acceleroMetersensor_Value");
		    xAxis = acceleroMetersensor_Value.getXAxis();
			yAxis = acceleroMetersensor_Value.getYAxis();
			zAxis = acceleroMetersensor_Value.getZAxis();
			galvanicSkinRespsensor_Value = input.getFloatByField("galvanicSkinRespsensor_Value");
			/*********************************************************************/
			
			//Time of Sensors' Values
			currentTime = input.getLongByField("timestamp");
			
			/******************* Store Sensors' Data in List  ********************/
			sensorsValues.put("CGM_Value",CGM_Value);
			sensorsValues.put("heartRatesensor_Value",heartRatesensor_Value);
			sensorsValues.put("bodyTemperaturesensor_Value",bodyTemperaturesensor_Value);
			sensorsValues.put("galvanicSkinRespsensor_Value",galvanicSkinRespsensor_Value);
			//sensorsValues.put("",acceleroMetersensor_Value);
			/*********************************************************************/
			
			//if the algorithm enters the Case 2 where Theta >= Alpha
			//it should enter another loop
			//the Storm architecture is by it self a loop iterations
			if(caseTwoReached)
			{
				LOG.info("### DEBUG ### -> CASE 2 - CONDITION : caseTwoReached");
				m     = slopOfCurve( glucosePrevValue , sensorsValues.get("CGM_Value")  , glucoseSimplingFrequency);
				theta = thetaAngle(m);
				estimatedTimeofHypo = estimatedGlucoTakesMin0(glucosePrevValue,sensorsValues.get("CGM_Value"),glucoseSimplingFrequency,MIN_GlUCOMETER_THRESHOLD);
				glucosePrevValue = sensorsValues.get("CGM_Value");
				{    // * ** *** **** ***** Debug ***** **** *** ** *
					LOG.info("### DEBUG ### -> Slop of curve m     :  "+m);
					LOG.info("### DEBUG ### -> theta               :  "+theta);
					LOG.info("### DEBUG ### -> estimatedTimeofHypo :  "+estimatedTimeofHypo);
					LOG.info("### DEBUG ### -> glucosePrevValue    :  "+glucosePrevValue);
				}
				
				if(theta + 360 >= ALPHA_MAX_ANGLE + 360 ) 
				{
					LOG.info("### DEBUG ### -> CASE 2 - CONDITION : theta > = ALPHA");
					glucoseSimplingFrequency = F_SimplingFreq / 3;
					{    // * ** *** **** ***** Debug ***** **** *** ** *
						LOG.info("### DEBUG ### -> glucoseSimplingFrequency     :  "+glucoseSimplingFrequency);
						LOG.info("### DEBUG ### -> Slop of curve m     :  "+m);
						LOG.info("### DEBUG ### -> theta               :  "+theta);
						LOG.info("### DEBUG ### -> glucosePrevValue    :  "+glucosePrevValue);
						LOG.info("### DEBUG ### -> glucoseCurrValue    :  "+sensorsValues.get("CGM_Value"));
					}
				}
					
				if(theta + 360 < ALPHA_MAX_ANGLE + 360 && m > 0) 
				{
					LOG.info("### DEBUG ### -> CASE 2 - Risk Factor State -  CONDITION : theta < ALPHA && m > 0");
					glucoseSimplingFrequency = F_SimplingFreq / 2;
					{    // * ** *** **** ***** Debug ***** **** *** ** *
						LOG.info("### DEBUG ### -> glucoseSimplingFrequency     :  "+glucoseSimplingFrequency);
						LOG.info("### DEBUG ### -> Slop of curve m     :  "+m);
						LOG.info("### DEBUG ### -> theta               :  "+theta);
						LOG.info("### DEBUG ### -> glucosePrevValue    :  "+glucosePrevValue);
						LOG.info("### DEBUG ### -> glucoseCurrValue    :  "+sensorsValues.get("CGM_Value"));
					}
				}
				
				if(m <= 0)
				{
					LOG.info("### DEBUG ### -> CASE 2 - Stop Sampling Sensors -  CONDITION : m < = 0");	
					F_SimplingFreq = glucoseSimplingFrequency;
					caseTwoReached = false;
					{    // * ** *** **** ***** Debug ***** **** *** ** *
						LOG.info("### DEBUG ### -> glucoseSimplingFrequency  = F_SimplingFreq   :  "+glucoseSimplingFrequency);
						LOG.info("### DEBUG ### -> Slop of curve m     :  "+m);
						LOG.info("### DEBUG ### -> theta               :  "+theta);
						LOG.info("### DEBUG ### -> glucosePrevValue    :  "+glucosePrevValue);
						LOG.info("### DEBUG ### -> glucoseCurrValue    :  "+sensorsValues.get("CGM_Value"));
					}
				}
				
				if(!compareIsSafe(sensorsValues, MAX_SENSORS_VAL))
				{
					LOG.info("#### CASE 2 : ! ALERT HYPOGLYCEMIA - CONDITION : compareIsSafe ####");
					{    // * ** *** **** ***** Debug ***** **** *** ** *
						LOG.info("### DEBUG ### -> Slop of curve m     :  "+m);
						LOG.info("### DEBUG ### -> theta               :  "+theta + 360);
						LOG.info("### DEBUG ### -> ALPHA               :  "+ALPHA_MAX_ANGLE + 360);
						LOG.info("### DEBUG ### -> estimatedTimeofHypo :  "+estimatedTimeofHypo);
						LOG.info("### DEBUG ### -> glucosePrevValue    :  "+glucosePrevValue);
						LOG.info("### DEBUG ### -> glucoseCurrValue    :  "+sensorsValues.get("CGM_Value"));
					}
				}
			}
			else
			{
				//glucoseValue is G as initial value = 0.
				if(glucosePrevValue == 0.0F)
				{
					//sensorsValues(0) is CGM value
					glucosePrevValue = sensorsValues.get("CGM_Value");
				}
				//The algorithm now have two values of Glucuse
				//Previous Value G and current value sensorsValues.get(0);
				else
				{	
					//--------------- Previous Value -- New Value of Glucose -- Sampling Freq
					m     = slopOfCurve( glucosePrevValue , sensorsValues.get("CGM_Value")  , glucoseSimplingFrequency);
					theta = thetaAngle(m);
					estimatedTimeofHypo = estimatedGlucoTakesMin0(glucosePrevValue,sensorsValues.get("CGM_Value"),glucoseSimplingFrequency,MIN_GlUCOMETER_THRESHOLD);
					glucosePrevValue = sensorsValues.get("CGM_Value");
					
					{    // * ** *** **** ***** Debug ***** **** *** ** *
						LOG.info("### DEBUG ### -> Slop of curve m     :  "+m);
						LOG.info("### DEBUG ### -> theta               :  "+theta);
						LOG.info("### DEBUG ### -> estimatedTimeofHypo :  "+estimatedTimeofHypo);
						LOG.info("### DEBUG ### -> glucosePrevValue    :  "+glucosePrevValue);
					}
	
					//case 1 where Theta < alpha and CGM reached the THRESHOLD
					//we add 360 to compare positive values of the two angles
					//if we compare the nagative values , the logic will be false
					if(theta + 360 < ALPHA_MAX_ANGLE + 360 && sensorsValues.get("CGM_Value") < THRESHOLD_GLUCOSE_VALUE)
					{
						LOG.info("#### CASE 1 : LOW GLUCOSE ####");
						{    // * ** *** **** ***** Debug ***** **** *** ** *
							LOG.info("### DEBUG ### -> Slop of curve m     :  "+m);
							LOG.info("### DEBUG ### -> theta               :  "+theta);
							LOG.info("### DEBUG ### -> ALPHA               :  "+ALPHA_MAX_ANGLE);
							LOG.info("### DEBUG ### -> estimatedTimeofHypo :  "+estimatedTimeofHypo);
							LOG.info("### DEBUG ### -> glucosePrevValue    :  "+glucosePrevValue);
							LOG.info("### DEBUG ### -> glucoseCurrValue    :  "+sensorsValues.get("CGM_Value"));
						}
						
						LOG.info("#### CASE 1 : COMPARING SENSORS VALUES WITH MAX VALUES ALLOWED ... ####");
						//initiate the sampling of the other Sensors
						//we have already the values of other sensors
						//if compareIsSafe returns true that means the patient is safe 
						//and no vital sign is under the or above the normal ( i.e : max min Vals)
						if(!compareIsSafe(sensorsValues, MAX_SENSORS_VAL))
						{
							LOG.info("#### CASE 1 : LOW GLUCOSE ! ALERT HYPOGLYCEMIA ####");
							{    // * ** *** **** ***** Debug ***** **** *** ** *
								LOG.info("### DEBUG ### -> Slop of curve m     :  "+m);
								LOG.info("### DEBUG ### -> theta               :  "+theta + 360);
								LOG.info("### DEBUG ### -> ALPHA               :  "+ALPHA_MAX_ANGLE + 360);
								LOG.info("### DEBUG ### -> estimatedTimeofHypo :  "+estimatedTimeofHypo);
								LOG.info("### DEBUG ### -> glucosePrevValue    :  "+glucosePrevValue);
								LOG.info("### DEBUG ### -> glucoseCurrValue    :  "+sensorsValues.get("CGM_Value"));
							}
						}
					}
					//case 0 where Theta < Alpha means normal state
					else if(theta +360 < ALPHA_MAX_ANGLE + 360)
					{
						LOG.info("#### CASE 0 : NORMAL STATE ####");
						{    // * ** *** **** ***** Debug ***** **** *** ** *
							LOG.info("### DEBUG ### -> Slop of curve m     :  "+m);
							LOG.info("### DEBUG ### -> theta               :  "+theta);
							LOG.info("### DEBUG ### -> ALPHA               :  "+ALPHA_MAX_ANGLE);
							LOG.info("### DEBUG ### -> estimatedTimeofHypo :  "+estimatedTimeofHypo);
							LOG.info("### DEBUG ### -> glucosePrevValue    :  "+glucosePrevValue);
							LOG.info("### DEBUG ### -> glucoseCurrValue    :  "+sensorsValues.get("CGM_Value"));
						}
					}
					else if(theta + 360 >= ALPHA_MAX_ANGLE + 360)
					{
						//Pre-Hypoglycemia State
						glucoseSimplingFrequency = F_SimplingFreq / 3;
						caseTwoReached = true;
					}
				}
			}            
            //All seems to be nice, notify storm.spout about it
            this.collector.ack(input);
        } catch (Exception e) {
            LOG.warn("Failing tuple: " + input);
            LOG.warn("Exception: ", e);
            //Notify storm.spout about fail
            this.collector.fail(input);
        }
    }

    public static long getCurrentTime(){
		//get the current timestamp
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		return ((timestamp.getTime()/1000)+(timestamp.getTime()%1));
	}

	@Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("GSMvalue","heartRate","temperature","FrequencySimpling","currentTime"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
	
	/**
	 * calculate the slope of the curve
	 * @param PrevValueOfGlucose
	 * @param CurrentValueOfGlucose
	 * @param SamplingFreq
	 * @return the slop of curve
	 */
	public float slopOfCurve(float PrevValueOfGlucose,float CurrentValueOfGlucose,int SamplingFreq)
	{
		return (( PrevValueOfGlucose - CurrentValueOfGlucose) / SamplingFreq);
	}
	
	/**
	 * @param m : double : Slope of the curve
	 * @return the angle theta
	 */
	public float thetaAngle(double m){
		
		float theta= (float) Math.atan(m);
		return (float) Math.toDegrees(theta);
		
	}
	
	/**
	 * @param PrevValueOfGlucose
	 * @param CurrentValueOfGlucose
	 * @param SamplingFreq
	 * @param minGlucoseValue
	 * @return T that is estimated for glucose level 
	 * takes the value min0 while the slope m is kept.
	 */
	public float estimatedGlucoTakesMin0(float PrevValueOfGlucose,float CurrentValueOfGlucose,int SamplingFreq,float minGlucoseValue)
	{
		return SamplingFreq * ((CurrentValueOfGlucose - minGlucoseValue) / (PrevValueOfGlucose - CurrentValueOfGlucose )); 
	}
	
	/**
	 * @param sensorsVals
	 * @param sensorsMax
	 * @return true if all sensors's values still under the danger zone (e.q doesn't reach the max value)
	 */
	public boolean compareIsSafe(HashMap<String, Float> sensorsVals,HashMap<String, Float> sensorsMax)
	{
		if
		(
			 //	   sensorsVals.get("CGM_Value") > sensorsMax.get("MAX_CGM")
		     // || sensorsVals.get("") > sensorsMax.get("MAX_ACCELEROMETER")
				   sensorsVals.get("galvanicSkinRespsensor_Value") > sensorsMax.get("MAX_GSR")
				|| sensorsVals.get("heartRatesensor_Value") > sensorsMax.get("MAX_HEARTRATE")
				|| sensorsVals.get("bodyTemperaturesensor_Value") > sensorsMax.get("MAX_TEMPERATURE")
		)
			return false;
		
		return true;
	}

}

