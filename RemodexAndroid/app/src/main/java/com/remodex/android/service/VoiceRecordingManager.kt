package com.remodex.android.service

import android.Manifest
import android.annotation.SuppressLint
import android.content.Context
import android.content.pm.PackageManager
import android.media.AudioFormat
import android.media.AudioRecord
import android.media.MediaRecorder
import android.os.SystemClock
import androidx.core.content.ContextCompat
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import kotlin.math.log10
import kotlin.math.max
import kotlin.math.min
import kotlin.math.sqrt

data class VoiceRecordingClip(
    val wavData: ByteArray,
    val durationMs: Long
)

class VoiceRecordingException(message: String) : IllegalStateException(message)

class VoiceRecordingManager(
    private val context: Context
) {
    companion object {
        private const val TARGET_SAMPLE_RATE_HZ = 24_000
        private const val MAX_AUDIO_LEVELS = 240
        private val CANDIDATE_SAMPLE_RATES_HZ = intArrayOf(24_000, 48_000, 44_100, 16_000)
    }

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val _audioLevels = MutableStateFlow<List<Float>>(emptyList())
    val audioLevels: StateFlow<List<Float>> = _audioLevels.asStateFlow()

    private val _recordingDurationMs = MutableStateFlow(0L)
    val recordingDurationMs: StateFlow<Long> = _recordingDurationMs.asStateFlow()

    private var audioRecord: AudioRecord? = null
    private var recordJob: Job? = null
    private var captureSampleRateHz: Int = TARGET_SAMPLE_RATE_HZ
    private var capturedPcm = ByteArrayOutputStream()
    private var startedAtElapsedMs: Long = 0L

    val isRecording: Boolean
        get() = audioRecord != null

    @SuppressLint("MissingPermission")
    @Throws(VoiceRecordingException::class)
    suspend fun startRecording() {
        if (audioRecord != null) {
            throw VoiceRecordingException("Voice recording is already running.")
        }

        val initialized = createAudioRecord()
            ?: throw VoiceRecordingException("Unable to prepare the microphone recorder.")

        val recorder = initialized.first
        val frameBufferSize = initialized.second
        val shortBuffer = ShortArray(frameBufferSize / 2)

        captureSampleRateHz = initialized.third
        capturedPcm = ByteArrayOutputStream(frameBufferSize * 8)
        _audioLevels.value = emptyList()
        _recordingDurationMs.value = 0L
        startedAtElapsedMs = SystemClock.elapsedRealtime()
        audioRecord = recorder

        try {
            recorder.startRecording()
        } catch (_: IllegalStateException) {
            recorder.release()
            audioRecord = null
            throw VoiceRecordingException("Unable to prepare the microphone recorder.")
        }

        recordJob = scope.launch {
            val pcmChunk = ByteArray(shortBuffer.size * 2)
            while (audioRecord === recorder) {
                val readCount = recorder.read(shortBuffer, 0, shortBuffer.size)
                if (readCount <= 0) {
                    continue
                }

                val chunkSize = encodeShortsToLittleEndian(shortBuffer, readCount, pcmChunk)
                capturedPcm.write(pcmChunk, 0, chunkSize)
                _recordingDurationMs.value = SystemClock.elapsedRealtime() - startedAtElapsedMs
                updateAudioLevels(shortBuffer, readCount)
            }
        }
    }

    suspend fun stopRecording(): VoiceRecordingClip? {
        val recorder = audioRecord ?: return null
        audioRecord = null

        runCatching { recorder.stop() }
        recordJob?.cancelAndJoin()
        recordJob = null
        recorder.release()

        val rawPcm = capturedPcm.toByteArray()
        capturedPcm.reset()
        val durationMs = _recordingDurationMs.value
        _recordingDurationMs.value = 0L
        _audioLevels.value = emptyList()

        if (rawPcm.isEmpty()) {
            return null
        }

        val recordedSamples = decodeLittleEndianPcm(rawPcm)
        val normalizedSamples = if (captureSampleRateHz == TARGET_SAMPLE_RATE_HZ) {
            recordedSamples
        } else {
            resample(recordedSamples, captureSampleRateHz, TARGET_SAMPLE_RATE_HZ)
        }
        if (normalizedSamples.isEmpty()) {
            return null
        }

        return VoiceRecordingClip(
            wavData = encodeWav(normalizedSamples, TARGET_SAMPLE_RATE_HZ),
            durationMs = durationMs
        )
    }

    suspend fun cancelRecording() {
        val recorder = audioRecord ?: run {
            _audioLevels.value = emptyList()
            _recordingDurationMs.value = 0L
            return
        }

        audioRecord = null
        runCatching { recorder.stop() }
        recordJob?.cancelAndJoin()
        recordJob = null
        recorder.release()
        capturedPcm.reset()
        _audioLevels.value = emptyList()
        _recordingDurationMs.value = 0L
    }

    private fun createAudioRecord(): Triple<AudioRecord, Int, Int>? {
        if (ContextCompat.checkSelfPermission(
                context,
                Manifest.permission.RECORD_AUDIO
            ) != PackageManager.PERMISSION_GRANTED
        ) {
            return null
        }

        CANDIDATE_SAMPLE_RATES_HZ.forEach { sampleRate ->
            val minBufferSize = AudioRecord.getMinBufferSize(
                sampleRate,
                AudioFormat.CHANNEL_IN_MONO,
                AudioFormat.ENCODING_PCM_16BIT
            )
            if (minBufferSize <= 0) {
                return@forEach
            }

            val bufferSize = max(minBufferSize, sampleRate / 2)
            val recorder = try {
                AudioRecord(
                    MediaRecorder.AudioSource.MIC,
                    sampleRate,
                    AudioFormat.CHANNEL_IN_MONO,
                    AudioFormat.ENCODING_PCM_16BIT,
                    bufferSize
                )
            } catch (_: SecurityException) {
                return@forEach
            }

            if (recorder.state == AudioRecord.STATE_INITIALIZED) {
                return Triple(recorder, bufferSize, sampleRate)
            }

            recorder.release()
        }

        return null
    }

    private fun updateAudioLevels(samples: ShortArray, readCount: Int) {
        if (readCount <= 0) {
            return
        }

        var sumOfSquares = 0.0
        for (index in 0 until readCount) {
            val normalized = samples[index] / Short.MAX_VALUE.toFloat()
            sumOfSquares += normalized * normalized
        }

        val rms = sqrt(sumOfSquares / readCount)
        val decibels = 20 * log10(max(rms, 1e-6))
        val normalizedLevel = ((decibels + 50.0) / 50.0).toFloat().coerceIn(0f, 1f)
        val updatedLevels = (_audioLevels.value + normalizedLevel).let { levels ->
            if (levels.size > MAX_AUDIO_LEVELS) {
                levels.takeLast(MAX_AUDIO_LEVELS)
            } else {
                levels
            }
        }
        _audioLevels.value = updatedLevels
    }

    private fun encodeShortsToLittleEndian(
        samples: ShortArray,
        readCount: Int,
        scratch: ByteArray
    ): Int {
        var writeIndex = 0
        for (index in 0 until readCount) {
            val sample = samples[index].toInt()
            scratch[writeIndex] = (sample and 0xFF).toByte()
            scratch[writeIndex + 1] = ((sample shr 8) and 0xFF).toByte()
            writeIndex += 2
        }
        return writeIndex
    }

    private fun decodeLittleEndianPcm(rawPcm: ByteArray): ShortArray {
        val buffer = ByteBuffer.wrap(rawPcm).order(ByteOrder.LITTLE_ENDIAN)
        val samples = ShortArray(rawPcm.size / 2)
        var index = 0
        while (buffer.remaining() >= 2 && index < samples.size) {
            samples[index] = buffer.short
            index += 1
        }
        return if (index == samples.size) samples else samples.copyOf(index)
    }

    private fun resample(
        samples: ShortArray,
        sourceRateHz: Int,
        targetRateHz: Int
    ): ShortArray {
        if (samples.isEmpty() || sourceRateHz <= 0 || targetRateHz <= 0) {
            return ShortArray(0)
        }
        if (sourceRateHz == targetRateHz) {
            return samples
        }

        val ratio = targetRateHz.toDouble() / sourceRateHz.toDouble()
        val outputCount = (samples.size * ratio).toInt()
        if (outputCount <= 0) {
            return ShortArray(0)
        }

        val output = ShortArray(outputCount)
        val lastIndex = samples.lastIndex
        for (index in 0 until outputCount) {
            val sourceIndex = index / ratio
            val baseIndex = sourceIndex.toInt()
            val fraction = sourceIndex - baseIndex
            val first = samples[min(baseIndex, lastIndex)].toDouble()
            val second = samples[min(baseIndex + 1, lastIndex)].toDouble()
            output[index] = (first + ((second - first) * fraction))
                .toInt()
                .coerceIn(Short.MIN_VALUE.toInt(), Short.MAX_VALUE.toInt())
                .toShort()
        }

        return output
    }

    private fun encodeWav(samples: ShortArray, sampleRateHz: Int): ByteArray {
        val pcmByteCount = samples.size * 2
        val header = ByteBuffer.allocate(44 + pcmByteCount).order(ByteOrder.LITTLE_ENDIAN)
        header.put("RIFF".toByteArray())
        header.putInt(36 + pcmByteCount)
        header.put("WAVE".toByteArray())
        header.put("fmt ".toByteArray())
        header.putInt(16)
        header.putShort(1)
        header.putShort(1)
        header.putInt(sampleRateHz)
        header.putInt(sampleRateHz * 2)
        header.putShort(2)
        header.putShort(16)
        header.put("data".toByteArray())
        header.putInt(pcmByteCount)
        samples.forEach { sample -> header.putShort(sample) }
        return header.array()
    }
}
