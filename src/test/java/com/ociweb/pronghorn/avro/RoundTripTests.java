package com.ociweb.pronghorn.avro;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.xml.parsers.ParserConfigurationException;

import org.junit.Test;
import org.xml.sax.SAXException;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.ring.loader.TemplateHandler;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import com.ociweb.pronghorn.stage.stream.ToOutputStreamStage;

public class RoundTripTests {

    //raw avero writer to decoder
    //encoder to raw avero reader
    //decoder to encoder round trip
    
    @Test
    void testThis() {
        
        FieldReferenceOffsetManager from = buildFROM();        
        assertTrue(null!=from);
        
        RingBufferConfig busConfig = new RingBufferConfig(from, 10, 64);
        RingBufferConfig rawConfig = new RingBufferConfig(FieldReferenceOffsetManager.RAW_BYTES, 10, 64); 
        
        File avroSchemaFile = null;        
        
        GraphManager gm = new GraphManager();
        
        int seed = 42;
        
        PronghornStage generator = new TestGenerator(gm, seed, new RingBuffer(busConfig));        
        AvroEncodeStage aes = new AvroEncodeStage(gm, GraphManager.getInputRing(gm, generator, 1), new RingBuffer(rawConfig), avroSchemaFile );
        AvroDecodeStage ads = new AvroDecodeStage(gm, GraphManager.getInputRing(gm, aes, 1), new RingBuffer(busConfig), avroSchemaFile );        
        PronghornStage validateResults = new TestValidator(gm, seed, GraphManager.getInputRing(gm, ads, 1));
        
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
