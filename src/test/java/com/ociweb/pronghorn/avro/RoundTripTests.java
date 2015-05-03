package com.ociweb.pronghorn.avro;

import static com.ociweb.pronghorn.ring.RingBufferConfig.pipe;
import static com.ociweb.pronghorn.stage.scheduling.GraphManager.getOutputPipe;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import javax.xml.parsers.ParserConfigurationException;

import org.junit.Test;
import org.xml.sax.SAXException;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.ring.loader.TemplateHandler;
import com.ociweb.pronghorn.ring.stream.StreamingReadVisitorMatcher;
import com.ociweb.pronghorn.ring.stream.StreamingVisitorReader;
import com.ociweb.pronghorn.ring.stream.StreamingVisitorWriter;
import com.ociweb.pronghorn.ring.stream.StreamingWriteVisitorGenerator;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.route.SplitterStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
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
        int iterations = 10;
        
        /* Possible DSL for graph in groovy
         
          generator splitter [encoder decoder, noop] validate
          
         */
                
        PronghornStage generator = new TestGenerator(gm, seed, iterations, pipe(busConfig));        
        SplitterStage splitter = new SplitterStage(gm, getOutputPipe(gm, generator, 1), pipe(busConfig.grow2x()), pipe(busConfig.grow2x()));        
        
        AvroEncodeStage encoder = new AvroEncodeStage(gm, getOutputPipe(gm, splitter, 1), pipe(rawConfig), avroSchemaFile );
        AvroDecodeStage decoder = new AvroDecodeStage(gm, getOutputPipe(gm, encoder), pipe(busConfig), avroSchemaFile );        
        
        PronghornStage validateResults = new TestValidator(gm, getOutputPipe(gm, splitter, 2), getOutputPipe(gm, decoder));
        
        
//simple test using split
//        PronghornStage generator = new TestGenerator(gm, seed, iterations, pipe(busConfig));        
//        SplitterStage splitter = new SplitterStage(gm, getOutputPipe(gm, generator, 1), pipe(busConfig.grow2x()), pipe(busConfig.grow2x()));       
//        PronghornStage validateResults = new TestValidator(gm, getOutputPipe(gm, splitter, 2), getOutputPipe(gm, splitter, 1));
  
        
//simple test with no split        
//       PronghornStage generator1 = new TestGenerator(gm, seed, iterations, pipe(busConfig));        
//       PronghornStage generator2 = new TestGenerator(gm, seed, iterations ,pipe(busConfig));       
//        
//       PronghornStage validateResults = new TestValidator(gm, getOutputPipe(gm, generator1, 1), getOutputPipe(gm, generator2, 1));
        
        
        
        
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
    
    @Test
    public void matchingTestPositive() {
        
        FieldReferenceOffsetManager from = buildFROM();      
        byte primaryRingSizeInBits = 8; 
        byte byteRingSizeInBits = 18;
        
        RingBuffer ring1 = new RingBuffer(new RingBufferConfig(primaryRingSizeInBits, byteRingSizeInBits, null, from));
        RingBuffer ring2 = new RingBuffer(new RingBufferConfig(primaryRingSizeInBits, byteRingSizeInBits, null, from));
        
        ring1.initBuffers();
        ring2.initBuffers();
        
        int commonSeed = 42;         
        
        StreamingWriteVisitorGenerator swvg1 = new StreamingWriteVisitorGenerator(from, new Random(commonSeed), 30, 30);        
        StreamingVisitorWriter svw1 = new StreamingVisitorWriter(ring1, swvg1);
        
        StreamingWriteVisitorGenerator swvg2 = new StreamingWriteVisitorGenerator(from, new Random(commonSeed), 30, 30);        
        StreamingVisitorWriter svw2 = new StreamingVisitorWriter(ring2, swvg2);
                
        //now use matcher to confirm the same.
        StreamingReadVisitorMatcher srvm = new StreamingReadVisitorMatcher(ring1);
        StreamingVisitorReader svr = new StreamingVisitorReader(ring2, srvm);//new StreamingReadVisitorDebugDelegate(srvm) );

        svw1.startup();
        svw2.startup();
        svr.startup();
        
        int i = 6;
        while (--i>=0) {
            svw1.run();
            svw2.run();
            svr.run();
        }
        
        //confirm that both rings contain the exact same thing
        assertTrue(Arrays.equals(ring1.buffer, ring2.buffer));
        assertTrue(Arrays.equals(ring1.byteBuffer, ring2.byteBuffer));
        
        
        svr.shutdown();
        
        svw1.shutdown();
        svw2.shutdown();
        
    }
    
}
