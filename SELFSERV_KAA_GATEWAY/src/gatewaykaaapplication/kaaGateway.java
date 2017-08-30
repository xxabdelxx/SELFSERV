package gatewaykaaapplication;

import org.kaaproject.kaa.client.DesktopKaaPlatformContext;
import org.kaaproject.kaa.client.Kaa;
import org.kaaproject.kaa.client.KaaClient;
import org.kaaproject.kaa.client.SimpleKaaClientStateListener;
import org.kaaproject.kaa.client.logging.DefaultLogUploadStrategy;
import org.kaaproject.kaa.client.logging.LogStorageStatus;
import org.kaaproject.kaa.client.logging.LogUploadStrategyDecision;
import project.selfserv.kaa.sensors.data.*;
import project.selfserv.kaa.sensors.data.acceletometer.acceleroMetersensor;
import project.selfserv.kaa.sensors.data.samples.samples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;


public class kaaGateway {

    private static final Logger LOG = LoggerFactory.getLogger(kaaGateway.class);
    private static Random random = new Random();
    // A Kaa client.
    private static KaaClient kaaClient;
    private static EventsManager EM = new EventsManager();

    static double getRandomDouble(int max) {
        return random.nextDouble() * max;
    }
    /*
     * A demo application that shows how to use the Kaa logging API.
     */
    public static void main(String[] args) throws Exception{
        LOG.info("Storm data analytics started");
        LOG.info("--= Press any key to exit =--");

        // The default strategy uploads logs after either a threshold logs count
        // or a threshold logs size has been reached.
        // The following custom strategy uploads every log record as soon as it is created.
        // Set a custom strategy for uploading logs.
        
        EM.start();
       
        //########################################START###################################################
        while(true) {
                sensorsDataCollection report = new sensorsDataCollection();
                report.setTimestamp(System.currentTimeMillis());
                List<samples> SensorsDataList = new ArrayList<samples>();
                samples SensorsData = new samples();
  			    SensorsData.setCGMsensor((float) 6.5);
  			    SensorsData.setHeartRatesensor((float) 6.5);
  			    SensorsData.setBodyTemperaturesensor((float) 6.5);
  			    List<acceleroMetersensor> acceleroMetersensor_Value = new ArrayList<acceleroMetersensor>();
  			    acceleroMetersensor accelero = new acceleroMetersensor((float) 6.5,(float) 6.5,(float) 6.5);
  			    acceleroMetersensor_Value.add(accelero);
  			    SensorsData.setAcceleroMetersensor(acceleroMetersensor_Value);
  			    SensorsData.setGalvanicSkinRespsensor((float) 6.5);
  			    SensorsDataList.add(SensorsData);
  			    report.setSamples(SensorsDataList);
  			  
                LOG.info("------------------Sensors Data--------------------------------");
                LOG.info("TimesTamp : "+report.getTimestamp());
                LOG.info("CGM_Value : "+SensorsData.getCGMsensor());
                LOG.info("HeartRatesensor_Value : "+ SensorsData.getHeartRatesensor());
                LOG.info("bodyTemperaturesensor_Value : "+SensorsData.getBodyTemperaturesensor());
                // LOG.info("acceleroMetersensor (x,y,z) : ("+((acceleroMetersensor) SensorsData.getAcceleroMetersensor()).getXAxis()+","+acceleroMetersensor_Value.getYAxis()+","+acceleroMetersensor_Value.getZAxis()+")");
                LOG.info("acceleroMetersensor (x,y,z) : "+
                                                            SensorsData.getAcceleroMetersensor().get(0).getXAxis()+"|"+
                                                            SensorsData.getAcceleroMetersensor().get(0).getYAxis()+"|"+
                                                            SensorsData.getAcceleroMetersensor().get(0).getZAxis()+")"
                        );
                LOG.info("galvanicSkinRespsensor_Value : "+SensorsData.getGalvanicSkinRespsensor());
                
                
                EM.sendRecord(report);

                TimeUnit.SECONDS.sleep(2);
                
                //kaaClient.stop();
                //LOG.info("Storm data analytics stopped");
        }
        //#########################################END####################################################



        // Wait for the Enter key before exiting.
        
        // Stop the Kaa client and release all the resources which were in use.
        

        

    }
}
