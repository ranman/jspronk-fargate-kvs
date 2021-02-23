package com.amazonaws.kvsrecorder;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.kinesisvideo.parser.ebml.InputStreamParserByteSource;
import com.amazonaws.kinesisvideo.parser.ebml.MkvTypeInfos;
import com.amazonaws.kinesisvideo.parser.mkv.Frame;
import com.amazonaws.kinesisvideo.parser.mkv.MkvDataElement;
import com.amazonaws.kinesisvideo.parser.mkv.MkvElement;
import com.amazonaws.kinesisvideo.parser.mkv.MkvElementVisitException;
import com.amazonaws.kinesisvideo.parser.mkv.MkvStartMasterElement;
import com.amazonaws.kinesisvideo.parser.mkv.StreamingMkvReader;
import com.amazonaws.kinesisvideo.parser.utilities.FragmentMetadataVisitor;
import com.amazonaws.kinesisvideo.parser.utilities.MkvTag;
import com.amazonaws.kinesisvideo.parser.utilities.MkvTrackMetadata;
import com.amazonaws.regions.Regions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.HashMap;

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

        Map<String, KVSStreamTrackObject> outputStreamMap = new HashMap<>();

        if (isStreamAudioFromCustomerEnabled) {
            KVSStreamTrackObject fromCustomer = getKVSStreamTrackObject(KVSUtils.TrackName.AUDIO_FROM_CUSTOMER.getName(), contactId);
            outputStreamMap.put(KVSUtils.TrackName.AUDIO_FROM_CUSTOMER.getName(), fromCustomer);
        }
        if (isStreamAudioToCustomerEnabled) {
            KVSStreamTrackObject toCustomer = getKVSStreamTrackObject(KVSUtils.TrackName.AUDIO_TO_CUSTOMER.getName(), contactId);
            outputStreamMap.put(KVSUtils.TrackName.AUDIO_TO_CUSTOMER.getName(), toCustomer);
        }
        try {
            logger.info("Saving audio bytes to location");

            //Write audio bytes from the KVS stream to the temporary file
            startStreamingInternal(outputStreamMap, streamName, startFragmentNum, contactId);

        } finally {

            outputStreamMap.forEach((key, kvsTrackObject) -> {
                try {
                    closeFileAndUploadRawAudio(kvsTrackObject, contactId, saveCallRecording);
                } catch (IOException e) {
                    logger.error("Failed to upload {} for contactId {}", key, contactId);
                }
            });
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

        try {
            kvsStreamTrackObject.getOutputStream().close();
        } catch (Exception e) {
            logger.error("Failed closing output stream", e);
        }

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
     * @param trackName
     * @param contactId
     * @return
     * @throws FileNotFoundException
     */
    private KVSStreamTrackObject getKVSStreamTrackObject(String trackName, String contactId) throws FileNotFoundException {

        String fileName = String.format("%s_%s_%s.raw", contactId, DATE_FORMAT.format(new Date()), trackName);
        Path saveAudioFilePath = Paths.get("/tmp", fileName);
        FileOutputStream fileOutputStream = new FileOutputStream(saveAudioFilePath.toString());

        return new KVSStreamTrackObject(saveAudioFilePath, fileOutputStream, trackName);
    }



    private void startStreamingInternal(Map<String, KVSStreamTrackObject> mapping, String streamName, String fragmentNumber, String contactId) {
        InputStream kvsInputStream = KVSUtils.getInputStreamFromKVS(streamName, REGION, fragmentNumber, getAWSCredentials(), START_SELECTOR_TYPE);
        try {
            StreamingMkvReader streamingMkvReader = StreamingMkvReader.createDefault(new InputStreamParserByteSource(kvsInputStream));
            FragmentMetadataVisitor.BasicMkvTagProcessor tagProcessor = new FragmentMetadataVisitor.BasicMkvTagProcessor();
            FragmentMetadataVisitor fragmentVisitor = FragmentMetadataVisitor.create(Optional.of(tagProcessor));

            while (streamingMkvReader.mightHaveNext()) {
                Optional<MkvElement> mkvElementOptional = streamingMkvReader.nextIfAvailable();
                if (!mkvElementOptional.isPresent()) {
                    continue;
                }

                MkvElement mkvElement = (MkvElement) mkvElementOptional.get();
                try {
                    mkvElement.accept(fragmentVisitor);
                } catch (MkvElementVisitException e) {
                    logger.error("BasicMkvTagProcessor failed to accept mkvElement", e);
                }
                if (MkvTypeInfos.EBML.equals(mkvElement.getElementMetaData().getTypeInfo())) {
                    if (!(mkvElement instanceof MkvStartMasterElement)) {
                        continue;
                    }

                    Optional<String> contactIdFromStream = getContactIdFromStreamTag(tagProcessor);
                    if (contactIdFromStream.isPresent() && !contactIdFromStream.get().equals(contactId)) {
                        continue;
                    }

                    tagProcessor.clear();
                    continue;
                }

                if (!MkvTypeInfos.SIMPLEBLOCK.equals(mkvElement.getElementMetaData().getTypeInfo())) {
                    continue;
                }

                MkvDataElement dataElement = (MkvDataElement) mkvElement;
                Frame frame = (Frame) dataElement.getValueCopy().getVal();
                ByteBuffer audioBuffer = frame.getFrameData();
                long trackNumber = frame.getTrackNumber();
                MkvTrackMetadata metadata = fragmentVisitor.getMkvTrackMetadata(trackNumber);

                Optional<OutputStream> outputStream = getOutputStreamForTrackName(mapping, metadata.getTrackName(), fragmentVisitor);
                if (!outputStream.isPresent()) {
                    continue;
                }
                while (audioBuffer.remaining() > 0) {
                    byte[] audioBytes = new byte[audioBuffer.remaining()];
                    audioBuffer.get(audioBytes);

                    try {
                        // This can block the thread, when this stream isn't being read(flushed) and buffer is full.
                        // Make sure all the kvs tracks in `this.kvsTrackStreams` are being flushed out.
                        outputStream.get().write(audioBytes);
                    } catch (IOException e) {
                        logger.error("Failed to write to OutputStream for Track#{}", trackNumber, e);
                    }
                }
            }
        } catch (RuntimeException e) {
            // catching them and logging them just so the exceptions doesn't get lost as this method is running in different thread.
            logger.error("Exception in KVS streaming thread", e);
        } finally {
            try {
                kvsInputStream.close();
            } catch (IOException e) {
                logger.error("Failed closing KVS input stream", e);
            }
        }
    }

    private Optional<OutputStream> getOutputStreamForTrackName(Map<String, KVSStreamTrackObject> mapping, String trackName, FragmentMetadataVisitor fragmentVisitor) {
        KVSStreamTrackObject kvsStreamTrackObject = mapping.get(trackName);
        if (kvsStreamTrackObject != null) {
            return Optional.of(kvsStreamTrackObject.getOutputStream());
        }
        return Optional.empty();
    }


    /**
     * Extracts the contactId from the stream metadata object.
     * @param tagProcessor
     * @return
     */
    private Optional<String> getContactIdFromStreamTag(FragmentMetadataVisitor.BasicMkvTagProcessor tagProcessor) {
        Iterator iter = tagProcessor.getTags().iterator();

        MkvTag tag;
        do {
            if (!iter.hasNext()) {
                return Optional.empty();
            }

            tag = (MkvTag)iter.next();
        } while(!"ContactId".equals(tag.getTagName()));

        return Optional.of(tag.getTagValue());
    }

    /**
     * @return AWS credentials to be used to connect to s3 (for fetching and uploading audio) and KVS
     */
    private static AWSCredentialsProvider getAWSCredentials() {
        return DefaultAWSCredentialsProviderChain.getInstance();
    }

}
