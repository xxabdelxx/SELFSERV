package gatewaykaaapplication;

import org.kaaproject.kaa.client.DesktopKaaPlatformContext;
import org.kaaproject.kaa.client.Kaa;
import org.kaaproject.kaa.client.KaaClient;
import org.kaaproject.kaa.client.KaaClientProperties;
import org.kaaproject.kaa.client.SimpleKaaClientStateListener;
import org.kaaproject.kaa.client.event.EventFamilyFactory;
import org.kaaproject.kaa.client.event.registration.UserAttachCallback;
import org.kaaproject.kaa.client.logging.DefaultLogUploadStrategy;
import org.kaaproject.kaa.client.logging.LogStorageStatus;
import org.kaaproject.kaa.client.logging.LogUploadStrategyDecision;
import org.kaaproject.kaa.common.endpoint.gen.SyncResponseResultType;
import org.kaaproject.kaa.common.endpoint.gen.UserAttachResponse;
import project.selfserv.kaa.event.sampling.SamplingEvent;
import project.selfserv.kaa.sensors.data.sensorsDataCollection;
import project.selfserv.kaa.event.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;


public class EventsManager {


	
    private static final Logger LOG = LoggerFactory.getLogger(EventsManager.class);
    private static final String KEYS_DIR = "keys_for_selfserv";
    //Credentials for attaching an endpoint to the user.
    private static final String USER_EXTERNAL_ID = "trustfull_User_any";
    private static final String USER_ACCESS_TOKEN = "14940577420335682368";

    private KaaClient kaaClient;
    private SelfServEventClassFamilly SamplingEventFamily;

	public EventsManager() {
		// TODO Auto-generated constructor stub
		
		
	}
    /**
     * Startup current endpoint and init event listeners for receiving chat user messages and
     * events to manage chat list
     * @throws IOException
     */
    public void start() throws IOException {
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
                    LOG.info("--= Kaa client started =--");
                    startupLatch.countDown();
                }

                @Override
                public void onStopped() {
                    LOG.info("--= Kaa client stopped =--");
                }
            }, true);
            
            kaaClient.setLogUploadStrategy(new DefaultLogUploadStrategy() {
                @Override
                public LogUploadStrategyDecision isUploadNeeded(LogStorageStatus status) {
                    if (status.getRecordCount() >= 1) {
                        return LogUploadStrategyDecision.UPLOAD;
                    }
                    return LogUploadStrategyDecision.NOOP;
                }
            });
           

            //Start the Kaa client and connect it to the Kaa server.
            kaaClient.start();
            

            startupLatch.await();
            // EventUtil.sleepForSeconds(3);

            //Obtain the event family factory.
            final EventFamilyFactory eventFamilyFactory = kaaClient.getEventFamilyFactory();
            //Obtain the concrete event family.
            SamplingEventFamily = eventFamilyFactory.getSelfServEventClassFamilly();

            // Add event listeners to the family factory.
            SamplingEventFamily.addListener(new SelfServEventClassFamilly.Listener() {
				@Override
				public void onEvent(SamplingEvent event, String source) {
					// TODO Auto-generated method stub
					LOG.info("############## EVENT has been Recieved : "+event.getTimestamp()+" | "+event.getState());
				}
            });
             // attach endpoint to user - only endpoints attached to the same user
            // can do events exchange among themselves
            attachToUser(USER_EXTERNAL_ID,USER_ACCESS_TOKEN);

        } catch (InterruptedException e) {
            LOG.warn("Thread interrupted when wait for attach current endpoint to user", e);
        }
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
                    LOG.info("Attach to user result: {}", response.getResult());
                    if (response.getResult() == SyncResponseResultType.SUCCESS) {
                        LOG.info("Current endpoint have been successfully attached to user [ID={}]!", userId);
                    } else {
                        LOG.error("Attaching current endpoint to user [ID={}] FAILED.", userId);
                        LOG.error("Attach response: {}", response);
                        LOG.error("Events exchange will be NOT POSSIBLE.");
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
    
    public void sendEvent(long timesTamp , int state)
    {
    	SamplingEventFamily.sendEventToAll(new SamplingEvent(timesTamp,state));
    	LOG.info("################################################## Event Sent ############################");
    }
    
    public void sendRecord(sensorsDataCollection report)
    {
    	kaaClient.addLogRecord(report);
    }

    /**
     * Stops current endpoint.
     */
    public void stop() {
        kaaClient.stop();
    }


}
