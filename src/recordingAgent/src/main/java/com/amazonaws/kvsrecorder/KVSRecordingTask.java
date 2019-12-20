package com.amazonaws.kvsrecorder;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.kinesisvideo.parser.ebml.InputStreamParserByteSource;
import com.amazonaws.kinesisvideo.parser.mkv.StreamingMkvReader;
import com.amazonaws.kinesisvideo.parser.utilities.FragmentMetadataVisitor;
import com.amazonaws.regions.Regions;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.transcribestreaming.model.AudioStream;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;

public class KVSRecordingTask {

    private static final Regions REGION = Regions.fromName(System.getenv("AWS_REGION"));
    private static final String RECORDINGS_BUCKET_NAME = System.getenv("RECORDINGS_BUCKET_NAME");
    private static final String RECORDINGS_KEY_PREFIX = System.getenv("RECORDINGS_KEY_PREFIX");
    private static final boolean RECORDINGS_PUBLIC_READ_ACL = Boolean.parseBoolean(System.getenv("RECORDINGS_PUBLIC_READ_ACL"));
    private static final String START_SELECTOR_TYPE = System.getenv("START_SELECTOR_TYPE");

    private static final Logger logger = LoggerFactory.getLogger(KVSRecordingTask.class);
    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");


    /**
     * Handler function for the Lambda
     *
     * @param request
     * @return
     */
    public String handleRequest(RecordingRequest request) throws Exception {
        logger.info("received request : " + request.toString());
        startKVSToTranscribeStreaming(
                request.getStreamARN(),
                request.getStartFragmentNum(),
                request.getConnectContactId(),
                request.getSaveCallRecording(),
                request.isStreamAudioFromCustomer(),
                request.isStreamAudioToCustomer());
        return "SOMETHING FFS";
    }

    /**
     * Starts streaming between KVS and Transcribe
     * The transcript segments are continuously saved to the Dynamo DB table
     * At end of the streaming session, the raw audio is saved as an s3 object
     *
     * @param streamARN
     * @param startFragmentNum
     * @param contactId
     * @throws Exception
     */
    private void startKVSToTranscribeStreaming(String streamARN, String startFragmentNum, String contactId,
                                               Optional<Boolean> saveCallRecording,
                                               boolean isStreamAudioFromCustomerEnabled, boolean isStreamAudioToCustomerEnabled) throws Exception {
        String streamName = streamARN.substring(streamARN.indexOf("/") + 1, streamARN.lastIndexOf("/"));

        KVSStreamTrackObject kvsStreamTrackObjectFromCustomer = null;
        KVSStreamTrackObject kvsStreamTrackObjectToCustomer = null;

        if (isStreamAudioFromCustomerEnabled) {
            kvsStreamTrackObjectFromCustomer = getKVSStreamTrackObject(streamName, startFragmentNum, KVSUtils.TrackName.AUDIO_FROM_CUSTOMER.getName(), contactId);
        }
        if (isStreamAudioToCustomerEnabled) {
            kvsStreamTrackObjectToCustomer = getKVSStreamTrackObject(streamName, startFragmentNum, KVSUtils.TrackName.AUDIO_TO_CUSTOMER.getName(), contactId);
        }
        try {
            logger.info("Saving audio bytes to location");

            //Write audio bytes from the KVS stream to the temporary file
            if (kvsStreamTrackObjectFromCustomer != null) {
                writeAudioBytesToKvsStream(kvsStreamTrackObjectFromCustomer, contactId);
            }
            if (kvsStreamTrackObjectToCustomer != null) {
                writeAudioBytesToKvsStream(kvsStreamTrackObjectToCustomer, contactId);
            }

        } finally {
            if (kvsStreamTrackObjectFromCustomer != null) {
                closeFileAndUploadRawAudio(kvsStreamTrackObjectFromCustomer, contactId, saveCallRecording);
            }
            if (kvsStreamTrackObjectToCustomer != null) {
                closeFileAndUploadRawAudio(kvsStreamTrackObjectToCustomer, contactId, saveCallRecording);
            }
        }
    }


    /**
     * Closes the FileOutputStream and uploads the Raw audio file to S3
     *
     * @param kvsStreamTrackObject
     * @param saveCallRecording should the call recording be uploaded to S3?
     * @throws IOException
     */
    private void closeFileAndUploadRawAudio(KVSStreamTrackObject kvsStreamTrackObject, String contactId, Optional<Boolean> saveCallRecording) throws IOException {

        kvsStreamTrackObject.getInputStream().close();
        kvsStreamTrackObject.getOutputStream().close();

        //Upload the Raw Audio file to S3
        if ((saveCallRecording.isPresent() ? saveCallRecording.get() : false) && (new File(kvsStreamTrackObject.getSaveAudioFilePath().toString()).length() > 0)) {
            AudioUtils.uploadRawAudio(REGION, RECORDINGS_BUCKET_NAME, RECORDINGS_KEY_PREFIX, kvsStreamTrackObject.getSaveAudioFilePath().toString(), contactId, RECORDINGS_PUBLIC_READ_ACL,
                    getAWSCredentials());
        } else {
            logger.info("Skipping upload to S3.  saveCallRecording was disabled or audio file has 0 bytes: " + kvsStreamTrackObject.getSaveAudioFilePath().toString());
        }

    }

    /**
     * Create all objects necessary for KVS streaming from each track
     *
     * @param streamName
     * @param startFragmentNum
     * @param trackName
     * @param contactId
     * @return
     * @throws FileNotFoundException
     */
    private KVSStreamTrackObject getKVSStreamTrackObject(String streamName, String startFragmentNum, String trackName,
                                                         String contactId) throws FileNotFoundException {
        InputStream kvsInputStream = KVSUtils.getInputStreamFromKVS(streamName, REGION, startFragmentNum, getAWSCredentials(), START_SELECTOR_TYPE);
        StreamingMkvReader streamingMkvReader = StreamingMkvReader.createDefault(new InputStreamParserByteSource(kvsInputStream));

        FragmentMetadataVisitor.BasicMkvTagProcessor tagProcessor = new FragmentMetadataVisitor.BasicMkvTagProcessor();
        FragmentMetadataVisitor fragmentVisitor = FragmentMetadataVisitor.create(Optional.of(tagProcessor));

        String fileName = String.format("%s_%s_%s.raw", contactId, DATE_FORMAT.format(new Date()), trackName);
        Path saveAudioFilePath = Paths.get("/tmp", fileName);
        FileOutputStream fileOutputStream = new FileOutputStream(saveAudioFilePath.toString());

        return new KVSStreamTrackObject(kvsInputStream, streamingMkvReader, tagProcessor, fragmentVisitor, saveAudioFilePath, fileOutputStream, trackName);
    }


    /**
     * Write the kvs stream to the output buffer
     *
     * @param kvsStreamTrackObject
     * @param contactId
     * @throws Exception
     */
    private void writeAudioBytesToKvsStream(KVSStreamTrackObject kvsStreamTrackObject, String contactId) throws Exception {

        ByteBuffer audioBuffer = KVSUtils.getByteBufferFromStream(kvsStreamTrackObject.getStreamingMkvReader(),
                kvsStreamTrackObject.getFragmentVisitor(), kvsStreamTrackObject.getTagProcessor(), contactId, kvsStreamTrackObject.getTrackName());

        while (audioBuffer.remaining() > 0) {
            byte[] audioBytes = new byte[audioBuffer.remaining()];
            audioBuffer.get(audioBytes);
            kvsStreamTrackObject.getOutputStream().write(audioBytes);
            audioBuffer = KVSUtils.getByteBufferFromStream(kvsStreamTrackObject.getStreamingMkvReader(),
                    kvsStreamTrackObject.getFragmentVisitor(), kvsStreamTrackObject.getTagProcessor(), contactId, kvsStreamTrackObject.getTrackName());
        }
    }

    /**
     * @return AWS credentials to be used to connect to s3 (for fetching and uploading audio) and KVS
     */
    private static AWSCredentialsProvider getAWSCredentials() {
        return DefaultAWSCredentialsProviderChain.getInstance();
    }


    /**
     * KVSAudioStreamPublisher implements audio stream publisher.
     * It emits audio events from a KVS stream asynchronously in a separate thread
     */
    private static class KVSAudioStreamPublisher implements Publisher<AudioStream> {
        private final StreamingMkvReader streamingMkvReader;
        private String contactId;
        private OutputStream outputStream;
        private FragmentMetadataVisitor.BasicMkvTagProcessor tagProcessor;
        private FragmentMetadataVisitor fragmentVisitor;
        private String track;

        private KVSAudioStreamPublisher(StreamingMkvReader streamingMkvReader, String contactId, OutputStream outputStream,
                                        FragmentMetadataVisitor.BasicMkvTagProcessor tagProcessor, FragmentMetadataVisitor fragmentVisitor,
                                        String track) {
            this.streamingMkvReader = streamingMkvReader;
            this.contactId = contactId;
            this.outputStream = outputStream;
            this.tagProcessor = tagProcessor;
            this.fragmentVisitor = fragmentVisitor;
            this.track = track;
        }

        @Override
        public void subscribe(Subscriber<? super AudioStream> s) {
            s.onSubscribe(new KVSByteToAudioEventSubscription(s, streamingMkvReader, contactId, outputStream, tagProcessor, fragmentVisitor, track));
        }
    }
}
