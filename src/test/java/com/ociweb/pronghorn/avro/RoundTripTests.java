package com.ociweb.pronghorn.avro;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.xml.parsers.ParserConfigurationException;

import org.junit.Test;
import org.xml.sax.SAXException;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import static com.ociweb.pronghorn.ring.RingBufferConfig.*;
import com.ociweb.pronghorn.ring.loader.TemplateHandler;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.route.SplitterStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import static com.ociweb.pronghorn.stage.scheduling.GraphManager.*;

import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;

public class RoundTripTests {

    //raw avero writer to decoder
    //encoder to raw avero reader
    //decoder to encoder round trip
    
    @Test
    public void testThis() {
        
        FieldReferenceOffsetManager from = buildFROM();        
        assertTrue(null!=from);
        
        RingBufferConfig busConfig = new RingBufferConfig(from, 10, 64);
        RingBufferConfig rawConfig = new RingBufferConfig(FieldReferenceOffsetManager.RAW_BYTES, 10, 64); 
        
        File avroSchemaFile = null;        
        
        GraphManager gm = new GraphManager();
        
        int seed = 42;
        
        /* Possible DSL for graph in groovy
         
          generator splitter [encoder decoder, noop] validate
          
         */
                
        PronghornStage generator = new TestGenerator(gm, seed, pipe(busConfig));        
        SplitterStage splitter = new SplitterStage(gm, getOutputPipe(gm, generator, 1), pipe(busConfig.grow2x()), pipe(busConfig.grow2x()));        
        AvroEncodeStage encoder = new AvroEncodeStage(gm, getOutputPipe(gm, splitter, 1), pipe(rawConfig), avroSchemaFile );
        AvroDecodeStage decoder = new AvroDecodeStage(gm, getOutputPipe(gm, encoder, 1), pipe(busConfig), avroSchemaFile );        
        PronghornStage validateResults = new TestValidator(gm, getOutputPipe(gm, splitter, 2), getOutputPipe(gm, decoder, 1));
        
        //start the timer       
        final long start = System.currentTimeMillis();
        
        GraphManager.enableBatching(gm);
        
        StageScheduler scheduler = new ThreadPerStageScheduler(GraphManager.cloneAll(gm));        
        scheduler.startup();        
        
        //blocks until all the submitted runnables have stopped
       
        long TIMEOUT_SECONDS = 10;
        //this timeout is set very large to support slow machines that may also run this test.
        boolean cleanExit = scheduler.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      
        long duration = System.currentTimeMillis()-start;
        
        
    }
    
    public static FieldReferenceOffsetManager buildFROM() {
        try {
            return TemplateHandler.loadFrom("/singleTemplate.xml");
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }   
        return null;
        
    }
    
}
