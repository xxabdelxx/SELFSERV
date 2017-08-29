package org.kaaproject.examples.storm.storm.server.bolt;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Properties;

import org.apache.flume.Event;
import org.kaaproject.examples.storm.storm.server.producer.AvroFlumeEventProducer;
import project.selfserv.kaa.sensors.data.sensorsDataCollection;
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

@SuppressWarnings("serial")
public class Algorithm implements IRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(Algorithm.class);
    public static final String DEFAULT_FLUME_PROPERTY_PREFIX = "flume-avro-forward";

    private static final KaaFlumeEventReader<sensorsDataCollection> kaaReader = new KaaFlumeEventReader<sensorsDataCollection>(sensorsDataCollection.class);
    private AvroFlumeEventProducer producer;
    private OutputCollector collector;
    
    
    /********************* fixed data **********************/
    
    public static final double AlphaMax=-1.0;						//the maximum angle
	public static final double MAXGSR=0.0;                    //the threshold for heartRate sensor 
	public static final double MAXtemperature=0.0;                    //the threshold for heartRate sensor  
	public static final double Min0=70.0;
	public static final double threshold = Min0*1.25;               //a threshold for the glucose value
	
    /*******************************************************/
	
	private static double G=0.0; 					// initial value of glucose
	private static double theta;					// angle of the curve 
	private static double m;						// the slope of the curve
	private static long  T;
	private static int  v0;
	private static int  v1;
	private static int  v2;
	private static int  FS;
	private static boolean vg = false;
    private static boolean vf = false;
			
	/*******************************************************/
	
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
        try {
            
        	System.out.println("id--->"+input.getSourceComponent());
	           
            
	           
	           
            LOG.info("Message  ===> ");
            LOG.info(input.getInteger(0).toString());
            LOG.info(input.getInteger(1).toString());
            LOG.info(input.getInteger(2).toString());
            LOG.info(input.getInteger(3).toString());
            LOG.info(input.getLong(4).toString());
            
            v0=input.getInteger(0);     //GCMvalur
            v1=input.getInteger(1);		//GSR
            v2=input.getInteger(2);		//temperature
            FS=input.getInteger(3);		//FreauencySinpling							
            T=input.getLong(4);		//currenteTime
            
            int [] V={v1,v2};          //table of sensors value
            double [] Max={MAXGSR,MAXtemperature};
            
            
            System.out.println(" G =====>"+G);
            System.out.println(" v0 =====>"+v0);
            System.out.println(" v1 =====>"+V[0]);
            System.out.println(" v2=====>"+V[1]);
            System.out.println(" FS=====>"+FS);
            
            m=slop_of_Curve(G, v0, FS);
	        System.out.println("slop_of_Curve =====>"+m);
	        
	        theta=Angle_Theta(m)%360;
	        System.out.println("Angle_Theta   =====>"+theta);
	        
	        
	        G=v0;
            
           if(vg == false){
        	   
        	   System.out.println("vg is false ");
	        // --------case 0: 
			
			 if((theta > AlphaMax) && (v0 < threshold))
	        {
	        	System.out.println("Low glucose");
	        	
	        	int tmp=1;
	        	
	        	for(int i=0;i<2;i++){
	        	if(V[i] < Max[i])
	        	{
	        	
	        	 tmp = 0;
	        		
	        	}
	        	
	       }
	        	if(tmp==1){
	        		
	        	System.out.println("**********Alert hypoglycemia in T : "+T+"******");
	        	
	        	}
	        }
	        
	        
			 // --------case 1: 
	        
	        
			else if((theta > AlphaMax))
	        {
	        	
	        	System.out.println("*************normale*************");
	        	
	        }
			 
          
			 // --------case 2: 
	        
	        else if((theta < (AlphaMax)))
	        {
	        	
	        	
	        	
	        		//final Publisher publisher = new Publisher();
	        		//publisher.start(1);
	        		
	        	
	        	
	        		System.out.println("****************Pre-hypoglycemia state**********************");
	        	
	        		
	        		vg=true;
	        	
	        }
			 
			 if(vg == true){
				 
				 
				 System.out.println("vg is true ");
				//------------case 0--------------------
			        
			        if(theta < AlphaMax)
			        {
			        	
			        	System.out.println("******************Pre-hypoglycemia state loop2*******************");
			        	
			        	//----------------ajust the frequency ----------------------
			        	
			        	//	final Publisher publisher = new Publisher();
			        	//	publisher.start(1);
			        		
			        		
			        		vg = true;
			        	
			        	
			        }
			        
			        
			        //------------case 1---------------------
			        
			        
			        else if((theta > AlphaMax) && (m<0))
			        {
			        	
			        	//----------------ajust the frequency ----------------------
			        	
			        	//final Publisher publisher = new Publisher();
			        //		publisher.start(2);
			        		
			        		System.out.println("**************Risk factors state********************");
			        	
			        		
			        		vg = true;
			        }
			        
			        
			        
			        //-------------case 2 --------------------
			        
			        
			        else if(m>0)
			        {
			        	
			        	
			        	 //   final Publisher publisher = new Publisher();
			        	//	publisher.start(3);
			        		
			        		System.out.println("************stop*****************");
			        		
			        	
			        		vg = false;
			        		
			        		
			        		
			        }
			        
			        //-------------case 3 --------------------
			        
			        
			        int tmp2=1;
		        	
		        	for(int i=0;i<2;i++){
		        	if(V[i] < Max[i])
		        	{
		        	
		        	 tmp2 = 0;
		        		
		        	}
		        	
		       }
		        	if(tmp2==1){
		        		
		        	System.out.println("**********Alert hypoglycemia in T : "+T+"******");
		        	
		        	
		        	vg = false;
		        	
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
    
    
    
	// ------------- calculate the slope of the curve
	
	public double slop_of_Curve(double LCGMvalue,double ACGMvalue,int f){
		
		return ((ACGMvalue-LCGMvalue)/f);
		
	}
	
	
	// ------------- calculate the angle theta
	public double Angle_Theta(double m){
		
		double theta=Math.atan(m);
		return Math.toDegrees(theta);
		
	}
	
	
/*	Thread thread=new Thread(new Runnable() {
		
		@Override
		public void run() {
			// TODO Auto-generated method stub
			
			
			System.out.println("boucle 2");
    		double G2 = G;
    		
		while(true){	
		
			
	try {
			
    		
    		int v0_2=v0;
	        int v1_2=v1;
	        int v2_2=v2;
	        int FS2=FS;
	        
	        //if((FS2==0)&& (G2==G)){continue;}
	        System.out.println("G2 =====>"+G2);
	        System.out.println("V0_2 =====>"+v0_2);
	        
	        
	        
	        double m2=slop_of_Curve(G2, v0_2, FS2);
	        System.out.println("slop_of_Curve =====>"+m2);
	        
	        double theta2=Angle_Theta(m2)%360;
	        System.out.println("Angle_Theta   =====>"+theta2);
	        
	        G2 = v0_2;
	        
	        
	        int [] V2={v1_2,v2_2};          //table of sensors value
            double [] Max2={MAXGSR,MAXtemperature}; // table of MAXi
	        
	        
	      
	        //------------case 0--------------------
	        
	        if(theta2 < AlphaMax)
	        {
	        	
	        	System.out.println("******************Pre-hypoglycemia state loop2*******************");
	        	
	        	//----------------ajust the frequency ----------------------
	        	
	        		final Publisher publisher = new Publisher();
	        		publisher.start(1);
	        		
	        	
	        	
	        }
	        
	        
	        //------------case 1---------------------
	        
	        
	        else if((theta2 > AlphaMax) && (m2<0))
	        {
	        	
	        	//----------------ajust the frequency ----------------------
	        	
	        	final Publisher publisher = new Publisher();
	        		publisher.start(2);
	        		
	        		System.out.println("**************Risk factors state********************");
	        	
	        }
	        
	        
	        
	        //-------------case 2 --------------------
	        
	        
	        else if(m2>0)
	        {
	        	
	        	
	        	    final Publisher publisher = new Publisher();
	        		publisher.start(3);
	        		
	        		System.out.println("************stop*****************");
	        		
	        	
	        		Thread.currentThread().stop();
	        		
	        		
	        		
	        }
	        
	        //-------------case 3 --------------------
	        
	        
	        int tmp2=1;
        	
        	for(int i=0;i<2;i++){
        	if(V2[i] < Max2[i])
        	{
        	
        	 tmp2 = 0;
        		
        	}
        	
       }
        	if(tmp2==1){
        		
        	System.out.println("**********Alert hypoglycemia in T : "+T+"******");
        	
        	}
	        
	       
				Thread.sleep(FS);
	} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		}
	});
	*/
    
   
}

