package com.remodex.android.service

import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Test

class SecureTransportModelsTest {
    private val json = Json { ignoreUnknownKeys = true }

    @Test
    fun protocolVersionMatchesRemodex31() {
        assertEquals(2, SECURE_PROTOCOL_VERSION)
        assertEquals(
            2,
            SecureEnvelope(
                sessionId = "",
                keyEpoch = 0,
                sender = "iphone",
                counter = 0,
                ciphertext = "",
                tag = ""
            ).v
        )
    }

    @Test
    fun pairingPayloadAcceptsBridgeDisplayName() {
        val payload = json.decodeFromString<CodexPairingQRPayload>(
            """
            {
              "v": 2,
              "relay": "ws://127.0.0.1:9010/relay",
              "sessionId": "session",
              "macDeviceId": "mac",
              "macIdentityPublicKey": "a2V5",
              "expiresAt": 4102444800000,
              "displayName": "My Mac"
            }
            """.trimIndent()
        )

        assertEquals("My Mac", payload.displayName)
        assertFalse(payload.isExpired)
        assertNotNull(payload.expiresAt)
        assertEquals(2, payload.v)
    }

    @Test
    fun secureReadyResumeStateCarriesReplayEpoch() {
        val state = SecureResumeState(
            sessionId = "session",
            keyEpoch = 1,
            lastAppliedBridgeOutboundSeq = 7,
            bridgeReplayEpoch = "epoch-1"
        )

        val decoded = json.decodeFromString<SecureResumeState>(
            json.encodeToString(SecureResumeState.serializer(), state)
        )
        assertEquals("epoch-1", decoded.bridgeReplayEpoch)
        assertEquals(7, decoded.lastAppliedBridgeOutboundSeq)
    }

    @Test
    fun serverHelloAcceptsNumericTranscriptExpiryFromBridge() {
        val hello = json.decodeFromString<SecureServerHello>(
            """
            {
              "kind": "serverHello",
              "protocolVersion": 2,
              "sessionId": "session",
              "handshakeMode": "qr_bootstrap",
              "macDeviceId": "mac",
              "macIdentityPublicKey": "a2V5",
              "macEphemeralPublicKey": "a2V5",
              "serverNonce": "a2V5",
              "keyEpoch": 1,
              "bridgeReplayEpoch": "epoch-1",
              "expiresAtForTranscript": 4102444800000,
              "macSignature": "a2V5",
              "clientNonce": "a2V5",
              "displayName": "My Mac"
            }
            """.trimIndent()
        )

        assertEquals("4102444800000", hello.expiresAtForTranscript)
        assertEquals("epoch-1", hello.bridgeReplayEpoch)
    }
}
