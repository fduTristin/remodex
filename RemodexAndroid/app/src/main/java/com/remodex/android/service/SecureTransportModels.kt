package com.remodex.android.service

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import kotlinx.serialization.KSerializer
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.JsonDecoder
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonPrimitive

const val SECURE_PROTOCOL_VERSION = 2
const val PAIRING_QR_VERSION = 2
const val HANDSHAKE_TAG = "remodex-e2ee-v1"
const val MAX_PAIRING_AGE_MS = 5 * 60 * 1000L

@Serializable
data class CodexPairingQRPayload(
    val v: Int,
    val relay: String,
    val sessionId: String,
    val macDeviceId: String,
    val macIdentityPublicKey: String,
    @Serializable(with = FlexibleExpiresAtSerializer::class)
    val expiresAt: String? = null,
    val displayName: String? = null
) {
    val isExpired: Boolean get() {
        val exp = expiresAt?.trim()?.takeIf { it.isNotEmpty() } ?: return false
        return try {
            val ts = exp.toLongOrNull() ?: java.time.Instant.parse(exp).toEpochMilli()
            System.currentTimeMillis() > ts
        } catch (_: Exception) { false }
    }

    val isValid: Boolean get() = v == PAIRING_QR_VERSION
            && relay.isNotBlank()
            && sessionId.isNotBlank()
            && macDeviceId.isNotBlank()
            && macIdentityPublicKey.isNotBlank()
            && !isExpired
}

@OptIn(ExperimentalSerializationApi::class)
object FlexibleExpiresAtSerializer : KSerializer<String?> {
    override val descriptor: SerialDescriptor =
        PrimitiveSerialDescriptor("FlexibleExpiresAt", PrimitiveKind.STRING)

    override fun serialize(encoder: Encoder, value: String?) {
        if (value == null) {
            encoder.encodeNull()
        } else {
            encoder.encodeString(value)
        }
    }

    override fun deserialize(decoder: Decoder): String? {
        if (!decoder.decodeNotNullMark()) {
            decoder.decodeNull()
            return null
        }

        if (decoder is JsonDecoder) {
            return when (val element = decoder.decodeJsonElement()) {
                JsonNull -> null
                is JsonPrimitive -> element.content
                else -> throw SerializationException("Unsupported expiresAt payload: $element")
            }
        }

        return decoder.decodeString()
    }
}

@Serializable
data class CodexPhoneIdentityState(
    val phoneDeviceId: String,
    val identityPublicKey: String,     // Base64 Ed25519 public key
    val identityPrivateKey: String     // Base64 Ed25519 private key
)

@Serializable
data class CodexTrustedMacRecord(
    val macDeviceId: String,
    val macIdentityPublicKey: String,
    val displayName: String? = null,
    val lastConnectedAt: Long = 0
)

@Serializable
data class CodexTrustedMacRegistry(
    val macs: MutableMap<String, CodexTrustedMacRecord> = mutableMapOf()
)

// Handshake messages
@Serializable
data class SecureClientHello(
    val kind: String = "clientHello",
    val protocolVersion: Int = SECURE_PROTOCOL_VERSION,
    val sessionId: String,
    val handshakeMode: String,
    val phoneDeviceId: String,
    val phoneIdentityPublicKey: String,
    val phoneEphemeralPublicKey: String,
    val clientNonce: String
)

@Serializable
data class SecureServerHello(
    val kind: String = "serverHello",
    val protocolVersion: Int = SECURE_PROTOCOL_VERSION,
    val sessionId: String,
    val handshakeMode: String,
    val macDeviceId: String,
    val macIdentityPublicKey: String,
    val macEphemeralPublicKey: String,
    val serverNonce: String,
    val keyEpoch: Int,
    val bridgeReplayEpoch: String? = null,
    @Serializable(with = FlexibleExpiresAtSerializer::class)
    val expiresAtForTranscript: String? = null,
    val macSignature: String,
    val clientNonce: String? = null,
    val displayName: String? = null
)

@Serializable
data class SecureClientAuth(
    val kind: String = "clientAuth",
    val sessionId: String,
    val phoneDeviceId: String,
    val keyEpoch: Int,
    val phoneSignature: String
)

@Serializable
data class SecureReadyMessage(
    val kind: String = "secureReady",
    val sessionId: String? = null,
    val keyEpoch: Int? = null,
    val macDeviceId: String? = null
)

@Serializable
data class SecureResumeState(
    val kind: String = "resumeState",
    val sessionId: String,
    val keyEpoch: Int,
    val lastAppliedBridgeOutboundSeq: Int? = null,
    val bridgeReplayEpoch: String? = null
)

@Serializable
data class SecureErrorMessage(
    val kind: String = "secureError",
    val code: String? = null,
    val message: String? = null
)

@Serializable
data class SecureEnvelope(
    val kind: String = "encryptedEnvelope",
    val v: Int = SECURE_PROTOCOL_VERSION,
    val sessionId: String,
    val keyEpoch: Int,
    val sender: String,
    val counter: Long,
    val ciphertext: String,
    val tag: String
)

@Serializable
data class SecureApplicationPayload(
    @SerialName("bridgeOutboundSeq") val bridgeOutboundSeq: Int? = null,
    @SerialName("payloadText") val payloadText: String
)

// Session state during active encrypted connection
data class CodexSecureSession(
    val sessionId: String,
    val keyEpoch: Int,
    val bridgeReplayEpoch: String?,
    val phoneToMacKey: ByteArray,
    val macToPhoneKey: ByteArray,
    var phoneCounter: Long = 0,
    var lastMacCounter: Long = -1
) {
    fun nextPhoneCounter(): Long = phoneCounter++
}

data class CodexPendingHandshake(
    val sessionId: String,
    val handshakeMode: String,
    val clientNonce: ByteArray,
    val phoneEphemeralPrivateKey: ByteArray,
    val phoneEphemeralPublicKey: ByteArray,
    val phoneDeviceId: String,
    val phoneIdentityPublicKey: ByteArray,
    val phoneIdentityPrivateKey: ByteArray
)

enum class CodexSecureConnectionState {
    DISCONNECTED,
    CONNECTING,
    HANDSHAKING,
    CONNECTED_ENCRYPTED,
    ERROR
}

enum class CodexConnectionPhase {
    OFFLINE,
    CONNECTING,
    HANDSHAKING,
    LOADING_CHATS,
    SYNCING,
    CONNECTED
}

sealed class CodexConnectionRecoveryState {
    object Idle : CodexConnectionRecoveryState()
    data class Retrying(
        val attempt: Int,
        val message: String
    ) : CodexConnectionRecoveryState()
}
