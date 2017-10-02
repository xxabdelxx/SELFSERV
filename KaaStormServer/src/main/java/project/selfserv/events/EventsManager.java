package project.selfserv.events;
import org.kaaproject.examples.storm.storm.server.bolt.Algorithm;
import org.kaaproject.examples.storm.storm.server.bolt.AvroSinkBolt;
import org.kaaproject.kaa.client.event.FindEventListenersCallback;
import org.kaaproject.kaa.client.DesktopKaaPlatformContext;
import org.kaaproject.kaa.client.Kaa;
import org.kaaproject.kaa.client.KaaClient;
import org.kaaproject.kaa.client.KaaClientProperties;
import org.kaaproject.kaa.client.SimpleKaaClientStateListener;
import org.kaaproject.kaa.client.event.EventFamilyFactory;
import org.kaaproject.kaa.client.event.registration.UserAttachCallback;
import org.kaaproject.kaa.common.endpoint.gen.SyncResponseResultType;
import org.kaaproject.kaa.common.endpoint.gen.UserAttachResponse;
import project.selfserv.kaa.exchangeInfo.*;
import project.selfserv.kaa.exchangeMessages.*;
import project.selfserv.kaa.exchangeMessages.Request.Request;
import project.selfserv.kaa.exchangeMessages.Response.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import project.selfserv.configuration.ConfigManager;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;


public class EventsManager {

    private static final Logger LOG = LoggerFactory.getLogger(AvroSinkBolt.class);
    private static  String KEYS_DIR ;
    //Credentials for attaching an endpoint to the user.
    private static  String USER_EXTERNAL_ID ;
    private static  String USER_ACCESS_TOKEN ;

    private KaaClient kaaClient;
    private selfservExchangeInformationsEventClassFamily SamplingEventFamily;

    public static EventsManager EventsManagerInstance = new EventsManager();
    List<String> FQNs = new LinkedList<>();
    
	public EventsManager() {
		// TODO Auto-generated constructor stub
		KEYS_DIR 		  = ConfigManager.ConfigManagerInstance.getKEYS_DIR();
		USER_ACCESS_TOKEN = ConfigManager.ConfigManagerInstance.getUSER_ACCESS_TOKEN();
		USER_EXTERNAL_ID  = ConfigManager.ConfigManagerInstance.getUSER_EXTERNAL_ID();

		
	}
    /**
     * Startup current endpoint and init event listeners for receiving events
     * @throws IOException
     */
    public boolean start() throws IOException {
    	boolean started = true;
        try {

            // Setup working directory for endpoint
            KaaClientProperties endpointProperties = new KaaClientProperties();
            endpointProperties.setWorkingDirectory(KEYS_DIR);

            // Create the Kaa desktop context for the application
            DesktopKaaPlatformContext desktopKaaPlatformContext = new DesktopKaaPlatformContext(endpointProperties);

            // Create a Kaa client and add a listener which creates a log record
            // as soon as the Kaa client is started.
            final CountDownLatch startupLatch = new CountDownLatch(1);
            kaaClient = Kaa.newClient(desktopKaaPlatformContext, new SimpleKaaClientStateListener() {
                @Override
                public void onStarted() {
                    LOG.info("### DEBUG ### ##KAA## -> STARTING KAA CLIENT... ##");
                    startupLatch.countDown();
                }

                @Override
                public void onStopped() {
                    LOG.info("### DEBUG ### ##KAA## -> STOPING KAA CLIENT... ##");
                }
            }, true);

            //Start the Kaa client and connect it to the Kaa server.
            kaaClient.start();
            LOG.info("### DEBUG ### ##KAA## -> KAA CLIENT STARTED ! ##");
            startupLatch.await();
            // EventUtil.sleepForSeconds(3);

            //Obtain the event family factory.
            final EventFamilyFactory eventFamilyFactory = kaaClient.getEventFamilyFactory();
            //Obtain the concrete event family.
            SamplingEventFamily = eventFamilyFactory.getselfservExchangeInformationsEventClassFamily();
    	    FQNs.add(project.selfserv.kaa.exchangeMessages.Request.Request.class.getName());
    	    FQNs.add(project.selfserv.kaa.exchangeMessages.Response.Response.class.getName());
    	    kaaClient.findEventListeners(FQNs, new FindEventListenersCallback() {
    	        @Override
    	        public void onEventListenersReceived(List<String> eventListeners) {
    	            
    	        	LOG.info("### DEBUG ### ##KAA## -> EventListeners : "+Arrays.toString(eventListeners.toArray()));
    	        }   
    	        @Override
    	        public void onRequestFailed() {
    	        	LOG.info("### DEBUG ### ##KAA##-> EventListeners : FAILED");
    	        }
    	    });
            // Add event listeners to the family factory.
               SamplingEventFamily.addListener(new selfservExchangeInformationsEventClassFamily.Listener() {
				
				@Override
				public void onEvent(Response event, String source) {
					// TODO Auto-generated method stub
					LOG.info("### DEBUG ### ##KAA##-> Response event arrived from : "+source+" at "+event.getTimestamp() +" With message :  "+event.getMessage1());
				}
				
				@Override
				public void onEvent(Request event, String source) {
					// TODO Auto-generated method stub
					sendSamplingFreqToGateWay(getCurrentTime(),Algorithm.AlgorithmInstance.getGlucoseSimplingFrequency());
					LOG.info("### DEBUG ### ##KAA##-> Request event arrived from : "+source+" at "+event.getTimestamp() +" With message :  "+event.getMessage1());
				}
			});
             // attach endpoint to user - only endpoints attached to the same user
            // can do events exchange among themselves
            attachToUser(USER_EXTERNAL_ID,USER_ACCESS_TOKEN);

        } catch (InterruptedException e) {
        	started = false;
            LOG.warn("Thread interrupted when wait for attach current endpoint to user", e);
        }
        return started;
    }

    /**
     * Attach endpoint to specified user.
     * Only endpoints attached to the same user can do events exchange among themselves
     *
     * @param userAccessToken user access token that allows to do endpoint attach to this user
     * @param userId user ID
     */
    public void attachToUser(String userId, String userAccessToken) {
        try {
            // Attach the endpoint to the user
            // This application uses a trustful verifier, therefore
            // any user credentials sent by the endpoint are accepted as valid.
            final CountDownLatch attachLatch = new CountDownLatch(1);
            kaaClient.attachUser(userId, userAccessToken, new UserAttachCallback() {
                @Override
                public void onAttachResult(UserAttachResponse response) {
                    LOG.info("### DEBUG ### ##KAA## -> Attach to user result: {}", response.getResult());
                    if (response.getResult() == SyncResponseResultType.SUCCESS) {
                        LOG.info("### DEBUG ### ##KAA## -> Current endpoint have been successfully attached to user [ID={}]!", userId);
                    } else {
                        LOG.error("### DEBUG ### ##KAA## -> Attaching current endpoint to user [ID={}] FAILED.", userId);
                        LOG.error("### DEBUG ### ##KAA## -> Attach response: {}", response);
                        LOG.error("### DEBUG ### ##KAA## -> Events exchange will be NOT POSSIBLE.");
                    }
                    attachLatch.countDown();
                }
            });

            attachLatch.await();
            // EventUtil.sleepForSeconds(3);
        } catch (InterruptedException e) {
            LOG.warn("Thread interrupted when wait for attach current endpoint to user", e);
        }
    }
    
    public static long getCurrentTime(){
  		//get the current timestamp
  		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
  		return ((timestamp.getTime()/1000)+(timestamp.getTime()%1));
  	}

    /*public void sendEvent(long timesTamp , int state)
    {
    	//SamplingEventFamily.sendEventToAll(new SamplingEvent(timesTamp,state));
    	LOG.info("########################### EVENT SENT : OK ############################");
    }*/
    
    /**
     * @param timesTamp : long , time of sent 
     * @param SamplingFreq : current sampling Frequency
     */
    public void sendSamplingFreqToGateWay(long timesTamp , int SamplingFreq)
    {
    	SamplingEventFamily.sendEventToAll(new Response(timesTamp,SamplingFreq));
    	LOG.info("### DEBUG ### ##EVENT## -> Sampling Freq SENT : Timestamp : "+timesTamp+" and Freq : "+ SamplingFreq+" #####");
    }

    /**
     * Stops current endpoint.
     */
    public void stop() {
        kaaClient.stop();
        LOG.info("### DEBUG ### ##KAA## -> KAA CLIENT STOPED ! ##");
        
    }


}
