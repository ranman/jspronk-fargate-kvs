package com.amazonaws.kvsrecorder;

import com.amazonaws.kinesisvideo.parser.mkv.StreamingMkvReader;
import com.amazonaws.kinesisvideo.parser.utilities.FragmentMetadataVisitor;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.file.Path;

public class KVSStreamTrackObject {
    private Path saveAudioFilePath;
    private FileOutputStream outputStream;
    private String trackName;

    public KVSStreamTrackObject(Path saveAudioFilePath, FileOutputStream outputStream, String trackName) {
        this.saveAudioFilePath = saveAudioFilePath;
        this.outputStream = outputStream;
        this.trackName = trackName;
    }

    public Path getSaveAudioFilePath() {
        return saveAudioFilePath;
    }

    public FileOutputStream getOutputStream() {
        return outputStream;
    }

    public String getTrackName() {
        return trackName;
    }
}
