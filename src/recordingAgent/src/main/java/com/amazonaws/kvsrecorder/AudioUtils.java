package com.amazonaws.kvsrecorder;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sound.sampled.*;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Utility class to download/upload audio files from/to S3
 *
 * <p>Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.</p>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 * <p>
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
public final class AudioUtils {

    private static final Logger logger = LoggerFactory.getLogger(AudioUtils.class);


    /**
     * Converts the given raw audio data into a wav file. Returns the wav file back.
     */
    private static File convertToWav(String audioFilePath) throws IOException, UnsupportedAudioFileException {
        File outputFile = new File(audioFilePath.replace(".raw", ".wav"));
        AudioInputStream source = new AudioInputStream(Files.newInputStream(Paths.get(audioFilePath)),
                new AudioFormat(8000, 16, 1, true, false), -1); // 8KHz, 16 bit, 1 channel, signed, little-endian
        AudioSystem.write(source, AudioFileFormat.Type.WAVE, outputFile);
        return outputFile;
    }

    /**
     * Saves the raw audio file as an S3 object
     *
     * @param region
     * @param bucketName
     * @param keyPrefix
     * @param audioFilePath
     * @param awsCredentials
     */
    public static void uploadRawAudio(Regions region, String bucketName, String keyPrefix, String audioFilePath, String contactId, boolean publicReadAcl, AWSCredentialsProvider awsCredentials) {
        File wavFile = null;
        try {

            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(region)
                    .withCredentials(awsCredentials)
                    .build();

            wavFile = convertToWav(audioFilePath);

            // upload the raw audio file to the designated S3 location
            String objectKey = keyPrefix + wavFile.getName();

            logger.info(String.format("Uploading Audio: to %s/%s from %s", bucketName, objectKey, wavFile));
            PutObjectRequest request = new PutObjectRequest(bucketName, objectKey, wavFile);
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentType("audio/wav");
            metadata.addUserMetadata("contact-id", contactId);
            request.setMetadata(metadata);

            if (publicReadAcl) {
                request.setCannedAcl(CannedAccessControlList.PublicRead);
            }

            PutObjectResult s3result = s3Client.putObject(request);

            logger.info("putObject completed successfully " + s3result.getETag());

        } catch (SdkClientException e) {
            logger.error("Audio upload to S3 failed: ", e);
            throw e;
        } catch (UnsupportedAudioFileException|IOException e) {
            logger.error("Failed to convert to wav: ", e);
        }
        finally {
            if (wavFile != null) {
                wavFile.delete();
            }
        }
    }
}
