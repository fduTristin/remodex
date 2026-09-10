package com.remodex.android.service

import android.util.Log
import com.remodex.android.data.store.SecureStore
import kotlinx.coroutines.flow.*
import kotlinx.serialization.json.Json
import java.util.UUID

class SecureTransport(
    private val crypto: CryptoHelpers,
    private val secureStore: SecureStore,
    private val json: Json
) {
    companion object {
        private const val TAG = "SecureTransport"
    }

    private var session: CodexSecureSession? = null
    private var pendingHandshake: CodexPendingHandshake? = null
    private var lastAppliedBridgeOutboundSeq: Int =
        secureStore.readString(SecureStore.LAST_APPLIED_BRIDGE_OUTBOUND_SEQ)?.toIntOrNull() ?: 0
    private var lastAppliedBridgeReplayEpoch: String? =
        secureStore.readString(SecureStore.LAST_APPLIED_BRIDGE_REPLAY_EPOCH)
            ?.trim()
            ?.takeIf { it.isNotEmpty() }
    private var replayRefreshRequired = false

    private val _state = MutableStateFlow(CodexSecureConnectionState.DISCONNECTED)
    val state: StateFlow<CodexSecureConnectionState> = _state.asStateFlow()
    private val _lastErrorMessage = MutableStateFlow<String?>(null)
    val lastErrorMessage: StateFlow<String?> = _lastErrorMessage.asStateFlow()

    // --- Phone Identity ---

    fun getOrCreatePhoneIdentity(): CodexPhoneIdentityState {
        secureStore.readCodable(SecureStore.PHONE_IDENTITY_STATE, CodexPhoneIdentityState.serializer())
            ?.let { return it }

        val keyPair = crypto.generateEd25519KeyPair()
        val identity = CodexPhoneIdentityState(
            phoneDeviceId = UUID.randomUUID().toString(),
            identityPublicKey = crypto.toBase64(keyPair.publicKey),
            identityPrivateKey = crypto.toBase64(keyPair.privateKey)
        )
        secureStore.writeCodable(SecureStore.PHONE_IDENTITY_STATE, CodexPhoneIdentityState.serializer(), identity)
        return identity
    }

    // --- Trusted Mac Registry ---

    fun getTrustedMacRegistry(): CodexTrustedMacRegistry {
        return secureStore.readCodable(SecureStore.TRUSTED_MAC_REGISTRY, CodexTrustedMacRegistry.serializer())
            ?: CodexTrustedMacRegistry()
    }

    private fun saveTrustedMac(macDeviceId: String, macIdentityPublicKey: String, displayName: String?) {
        val registry = getTrustedMacRegistry()
        registry.macs[macDeviceId] = CodexTrustedMacRecord(
            macDeviceId = macDeviceId,
            macIdentityPublicKey = macIdentityPublicKey,
            displayName = displayName,
            lastConnectedAt = System.currentTimeMillis()
        )
        secureStore.writeCodable(SecureStore.TRUSTED_MAC_REGISTRY, CodexTrustedMacRegistry.serializer(), registry)
    }

    // --- Handshake: Step 1 - Build clientHello ---

    fun buildClientHello(sessionId: String, handshakeMode: String): SecureClientHello {
        if (handshakeMode == "qr_bootstrap") {
            resetReplayState()
        }
        _lastErrorMessage.value = null

        val identity = getOrCreatePhoneIdentity()
        val ephemeral = crypto.generateX25519KeyPair()
        val clientNonce = crypto.randomBytes(32)

        pendingHandshake = CodexPendingHandshake(
            sessionId = sessionId,
            handshakeMode = handshakeMode,
            clientNonce = clientNonce,
            phoneEphemeralPrivateKey = ephemeral.privateKey,
            phoneEphemeralPublicKey = ephemeral.publicKey,
            phoneDeviceId = identity.phoneDeviceId,
            phoneIdentityPublicKey = crypto.fromBase64(identity.identityPublicKey),
            phoneIdentityPrivateKey = crypto.fromBase64(identity.identityPrivateKey)
        )

        _state.value = CodexSecureConnectionState.HANDSHAKING

        return SecureClientHello(
            sessionId = sessionId,
            handshakeMode = handshakeMode,
            phoneDeviceId = identity.phoneDeviceId,
            phoneIdentityPublicKey = identity.identityPublicKey,
            phoneEphemeralPublicKey = crypto.toBase64(ephemeral.publicKey),
            clientNonce = crypto.toBase64(clientNonce)
        )
    }

    // --- Handshake: Step 2 - Process serverHello, build clientAuth ---

    fun processServerHello(serverHello: SecureServerHello): SecureClientAuth? {
        val pending = pendingHandshake ?: run {
            Log.e(TAG, "No pending handshake for serverHello")
            _state.value = CodexSecureConnectionState.ERROR
            return null
        }

        val expectedMacDeviceId = secureStore.readString(SecureStore.RELAY_MAC_DEVICE_ID)
            ?.trim()
            ?.takeIf { it.isNotEmpty() }
        val expectedMacIdentityPublicKey = secureStore.readString(SecureStore.RELAY_MAC_IDENTITY_PUBLIC_KEY)
            ?.trim()
            ?.takeIf { it.isNotEmpty() }
            ?: getTrustedMacRegistry().macs[serverHello.macDeviceId]?.macIdentityPublicKey

        if (expectedMacDeviceId == null
            || expectedMacIdentityPublicKey == null
            || serverHello.protocolVersion != SECURE_PROTOCOL_VERSION
            || serverHello.sessionId != pending.sessionId
            || serverHello.handshakeMode != pending.handshakeMode
            || serverHello.clientNonce != crypto.toBase64(pending.clientNonce)
            || serverHello.macDeviceId != expectedMacDeviceId
            || serverHello.macIdentityPublicKey != expectedMacIdentityPublicKey
        ) {
            Log.e(TAG, "serverHello did not match the active pairing")
            _state.value = CodexSecureConnectionState.ERROR
            _lastErrorMessage.value = "The secure bridge identity did not match the paired Mac."
            return null
        }

        val macIdentityPubKey: ByteArray
        val macEphemeralPubKey: ByteArray
        val serverNonce: ByteArray
        val macSig: ByteArray
        try {
            macIdentityPubKey = crypto.fromBase64(serverHello.macIdentityPublicKey)
            macEphemeralPubKey = crypto.fromBase64(serverHello.macEphemeralPublicKey)
            serverNonce = crypto.fromBase64(serverHello.serverNonce)
            macSig = crypto.fromBase64(serverHello.macSignature)
        } catch (error: Exception) {
            Log.e(TAG, "Invalid serverHello encoding: ${error.message}")
            _state.value = CodexSecureConnectionState.ERROR
            _lastErrorMessage.value = "The secure bridge handshake was malformed."
            return null
        }

        val expiresAtStr = serverHello.expiresAtForTranscript ?: "0"

        // Build transcript
        val transcript = crypto.buildTranscriptBytes(
            sessionId = pending.sessionId,
            protocolVersion = serverHello.protocolVersion,
            handshakeMode = pending.handshakeMode,
            keyEpoch = serverHello.keyEpoch,
            macDeviceId = serverHello.macDeviceId,
            phoneDeviceId = pending.phoneDeviceId,
            macIdentityPublicKey = macIdentityPubKey,
            phoneIdentityPublicKey = pending.phoneIdentityPublicKey,
            macEphemeralPublicKey = macEphemeralPubKey,
            phoneEphemeralPublicKey = pending.phoneEphemeralPublicKey,
            clientNonce = pending.clientNonce,
            serverNonce = serverNonce,
            expiresAtForTranscript = expiresAtStr
        )

        // Verify Mac signature
        if (!crypto.ed25519Verify(macIdentityPubKey, transcript, macSig)) {
            Log.e(TAG, "Mac signature verification failed")
            _state.value = CodexSecureConnectionState.ERROR
            return null
        }

        // Match the bridge/iOS transcript domain separation: append a length-prefixed label.
        val clientAuthTranscript = crypto.buildClientAuthTranscript(transcript)
        val phoneSig = crypto.ed25519Sign(pending.phoneIdentityPrivateKey, clientAuthTranscript)

        // Derive session keys
        val sharedSecret = crypto.x25519SharedSecret(pending.phoneEphemeralPrivateKey, macEphemeralPubKey)
        val (phoneToMacKey, macToPhoneKey) = crypto.deriveSessionKeys(
            sharedSecret = sharedSecret,
            transcriptBytes = transcript,
            sessionId = pending.sessionId,
            macDeviceId = serverHello.macDeviceId,
            phoneDeviceId = pending.phoneDeviceId,
            keyEpoch = serverHello.keyEpoch
        )

        session = CodexSecureSession(
            sessionId = pending.sessionId,
            keyEpoch = serverHello.keyEpoch,
            bridgeReplayEpoch = serverHello.bridgeReplayEpoch?.trim()?.takeIf { it.isNotEmpty() },
            phoneToMacKey = phoneToMacKey,
            macToPhoneKey = macToPhoneKey
        )

        // Save trusted Mac
        saveTrustedMac(serverHello.macDeviceId, serverHello.macIdentityPublicKey, serverHello.displayName)

        return SecureClientAuth(
            sessionId = pending.sessionId,
            phoneDeviceId = pending.phoneDeviceId,
            keyEpoch = serverHello.keyEpoch,
            phoneSignature = crypto.toBase64(phoneSig)
        )
    }

    // --- Handshake: Step 3 - Process secureReady ---

    fun processSecureReady(ready: SecureReadyMessage): SecureResumeState? {
        val sess = session ?: run {
            Log.e(TAG, "No active session for secureReady")
            _state.value = CodexSecureConnectionState.ERROR
            return null
        }

        if (ready.sessionId != null && ready.sessionId != sess.sessionId) {
            Log.w(TAG, "Ignoring stale secureReady for session=${ready.sessionId}")
            return null
        }
        if (ready.keyEpoch != null && ready.keyEpoch != sess.keyEpoch) {
            Log.w(TAG, "Ignoring stale secureReady for epoch=${ready.keyEpoch}")
            return null
        }

        Log.d(TAG, "Secure channel established (epoch=${sess.keyEpoch})")
        pendingHandshake = null
        _lastErrorMessage.value = null
        val bridgeReplayEpoch = sess.bridgeReplayEpoch
        val previousReplayEpoch = lastAppliedBridgeReplayEpoch
        if (!bridgeReplayEpoch.isNullOrBlank()
            && !previousReplayEpoch.isNullOrBlank()
            && bridgeReplayEpoch != previousReplayEpoch
        ) {
            lastAppliedBridgeOutboundSeq = 0
            secureStore.deleteValue(SecureStore.LAST_APPLIED_BRIDGE_OUTBOUND_SEQ)
            replayRefreshRequired = true
        }
        if (!bridgeReplayEpoch.isNullOrBlank()) {
            lastAppliedBridgeReplayEpoch = bridgeReplayEpoch
            secureStore.writeString(SecureStore.LAST_APPLIED_BRIDGE_REPLAY_EPOCH, bridgeReplayEpoch)
        }
        _state.value = CodexSecureConnectionState.CONNECTED_ENCRYPTED
        return SecureResumeState(
            sessionId = sess.sessionId,
            keyEpoch = sess.keyEpoch,
            lastAppliedBridgeOutboundSeq = lastAppliedBridgeOutboundSeq,
            bridgeReplayEpoch = bridgeReplayEpoch
        )
    }

    fun processSecureError(error: SecureErrorMessage) {
        Log.e(TAG, "Secure error: ${error.code} - ${error.message}")
        pendingHandshake = null
        session = null
        _lastErrorMessage.value = when (error.code) {
            "pairing_expired" -> "Pairing expired. Scan a fresh QR code from your Mac."
            "phone_not_trusted" -> "This device is not trusted by the current bridge session. Scan a fresh QR code to pair again."
            "phone_identity_changed" -> "This device identity changed. Scan a fresh QR code from your Mac."
            else -> error.message?.ifBlank { "Connection failed. Scan a fresh QR code and try again." }
                ?: "Connection failed. Scan a fresh QR code and try again."
        }
        _state.value = CodexSecureConnectionState.ERROR
    }

    // --- Encrypt outgoing ---

    fun encryptMessage(plaintext: String): SecureEnvelope? {
        val sess = session ?: return null
        val payloadJson = json.encodeToString(SecureApplicationPayload.serializer(),
            SecureApplicationPayload(payloadText = plaintext))
        val counter = sess.nextPhoneCounter()
        val nonce = crypto.buildNonce("iphone", counter)
        val result = crypto.aesGcmEncrypt(sess.phoneToMacKey, nonce, payloadJson.toByteArray(Charsets.UTF_8))

        return SecureEnvelope(
            sessionId = sess.sessionId,
            keyEpoch = sess.keyEpoch,
            sender = "iphone",
            counter = counter,
            ciphertext = crypto.toBase64(result.ciphertext),
            tag = crypto.toBase64(result.tag)
        )
    }

    // --- Decrypt incoming ---

    fun decryptEnvelope(envelope: SecureEnvelope): String? {
        val sess = session ?: return null

        if (envelope.sessionId != sess.sessionId || envelope.keyEpoch != sess.keyEpoch) {
            Log.w(TAG, "Envelope session/epoch mismatch")
            return null
        }

        if (envelope.sender == "mac" && envelope.counter <= sess.lastMacCounter) {
            Log.w(TAG, "Replay detected: counter=${envelope.counter} <= last=${sess.lastMacCounter}")
            return null
        }

        val nonce = crypto.buildNonce(envelope.sender, envelope.counter)
        val key = if (envelope.sender == "mac") sess.macToPhoneKey else sess.phoneToMacKey
        val ciphertext = crypto.fromBase64(envelope.ciphertext)
        val tag = crypto.fromBase64(envelope.tag)

        return try {
            val plaintext = crypto.aesGcmDecrypt(key, nonce, ciphertext, tag)
            if (envelope.sender == "mac") {
                sess.lastMacCounter = envelope.counter
            }
            val payload = json.decodeFromString(SecureApplicationPayload.serializer(), String(plaintext, Charsets.UTF_8))
            payload.bridgeOutboundSeq?.let { bridgeOutboundSeq ->
                if (bridgeOutboundSeq <= lastAppliedBridgeOutboundSeq) {
                    return null
                }
                lastAppliedBridgeOutboundSeq = bridgeOutboundSeq
                secureStore.writeString(
                    SecureStore.LAST_APPLIED_BRIDGE_OUTBOUND_SEQ,
                    bridgeOutboundSeq.toString()
                )
            }
            payload.payloadText
        } catch (e: Exception) {
            Log.e(TAG, "Decrypt failed: ${e.message}")
            null
        }
    }

    // --- Trusted session resolve ---

    fun buildTrustedResolveRequest(
        macDeviceId: String
    ): TrustedResolveRequest? {
        val identity = getOrCreatePhoneIdentity()
        val nonce = crypto.randomBytes(32)
        val timestamp = System.currentTimeMillis()

        val transcript = crypto.buildTrustedResolveTranscript(
            macDeviceId = macDeviceId,
            phoneDeviceId = identity.phoneDeviceId,
            phoneIdentityPublicKey = crypto.fromBase64(identity.identityPublicKey),
            nonce = nonce,
            timestamp = timestamp
        )

        val signature = crypto.ed25519Sign(crypto.fromBase64(identity.identityPrivateKey), transcript)

        return TrustedResolveRequest(
            macDeviceId = macDeviceId,
            phoneDeviceId = identity.phoneDeviceId,
            phoneIdentityPublicKey = identity.identityPublicKey,
            timestamp = timestamp,
            nonce = crypto.toBase64(nonce),
            signature = crypto.toBase64(signature)
        )
    }

    fun reset() {
        session = null
        pendingHandshake = null
        _lastErrorMessage.value = null
        _state.value = CodexSecureConnectionState.DISCONNECTED
    }

    private fun resetReplayState() {
        lastAppliedBridgeOutboundSeq = 0
        secureStore.deleteValue(SecureStore.LAST_APPLIED_BRIDGE_OUTBOUND_SEQ)
        lastAppliedBridgeReplayEpoch = null
        secureStore.deleteValue(SecureStore.LAST_APPLIED_BRIDGE_REPLAY_EPOCH)
        replayRefreshRequired = true
    }

    fun resetReplayCursor(cursor: Int = 0, replayEpoch: String? = null) {
        lastAppliedBridgeOutboundSeq = cursor.coerceAtLeast(0)
        secureStore.writeString(
            SecureStore.LAST_APPLIED_BRIDGE_OUTBOUND_SEQ,
            lastAppliedBridgeOutboundSeq.toString()
        )
        replayEpoch?.trim()?.takeIf { it.isNotEmpty() }?.let {
            lastAppliedBridgeReplayEpoch = it
            secureStore.writeString(SecureStore.LAST_APPLIED_BRIDGE_REPLAY_EPOCH, it)
        }
        replayRefreshRequired = true
    }

    fun consumeReplayRefreshRequired(): Boolean {
        val required = replayRefreshRequired
        replayRefreshRequired = false
        return required
    }

    val isEncrypted: Boolean get() = session != null
    val currentSessionId: String? get() = session?.sessionId
    val currentKeyEpoch: Int? get() = session?.keyEpoch
}

data class TrustedResolveRequest(
    val macDeviceId: String,
    val phoneDeviceId: String,
    val phoneIdentityPublicKey: String,
    val timestamp: Long,
    val nonce: String,
    val signature: String
)
