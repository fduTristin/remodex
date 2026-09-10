package com.remodex.android.data.store

import android.content.Context
import android.content.SharedPreferences
import androidx.security.crypto.EncryptedSharedPreferences
import androidx.security.crypto.MasterKeys
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json

class SecureStore(context: Context) {

    private val json = Json { ignoreUnknownKeys = true; encodeDefaults = true }

    private val prefs: SharedPreferences by lazy {
        val masterKeyAlias = MasterKeys.getOrCreate(MasterKeys.AES256_GCM_SPEC)
        EncryptedSharedPreferences.create(
            "remodex_secure_prefs",
            masterKeyAlias,
            context,
            EncryptedSharedPreferences.PrefKeyEncryptionScheme.AES256_SIV,
            EncryptedSharedPreferences.PrefValueEncryptionScheme.AES256_GCM
        )
    }

    fun readString(key: String): String? = prefs.getString(key, null)

    fun writeString(key: String, value: String) {
        prefs.edit().putString(key, value).apply()
    }

    fun writeStringSync(key: String, value: String) {
        prefs.edit().putString(key, value).commit()
    }

    fun readData(key: String): ByteArray? {
        val encoded = prefs.getString(key, null) ?: return null
        return try { android.util.Base64.decode(encoded, android.util.Base64.NO_WRAP) }
        catch (_: Exception) { null }
    }

    fun writeData(key: String, value: ByteArray) {
        prefs.edit().putString(key, android.util.Base64.encodeToString(value, android.util.Base64.NO_WRAP)).apply()
    }

    fun <T> readCodable(key: String, serializer: KSerializer<T>): T? {
        val raw = prefs.getString(key, null) ?: return null
        return try { json.decodeFromString(serializer, raw) }
        catch (_: Exception) { null }
    }

    fun <T> writeCodable(key: String, serializer: KSerializer<T>, value: T) {
        prefs.edit().putString(key, json.encodeToString(serializer, value)).apply()
    }

    fun deleteValue(key: String) {
        prefs.edit().remove(key).apply()
    }

    fun deleteValueSync(key: String) {
        prefs.edit().remove(key).commit()
    }

    fun contains(key: String): Boolean = prefs.contains(key)

    companion object Keys {
        const val RELAY_SESSION_ID = "relaySessionId"
        const val RELAY_URL = "relayUrl"
        const val RELAY_MAC_DEVICE_ID = "relayMacDeviceId"
        const val RELAY_MAC_IDENTITY_PUBLIC_KEY = "relayMacIdentityPublicKey"
        const val PUSH_DEVICE_TOKEN = "pushDeviceToken"
        const val PHONE_IDENTITY_STATE = "phoneIdentityState"
        const val TRUSTED_MAC_REGISTRY = "trustedMacRegistry"
        const val LAST_APPLIED_BRIDGE_OUTBOUND_SEQ = "lastAppliedBridgeOutboundSeq"
        const val LAST_APPLIED_BRIDGE_REPLAY_EPOCH = "lastAppliedBridgeReplayEpoch"
        const val RENAMED_THREAD_NAMES = "renamedThreadNames"
        const val FORKED_THREAD_ORIGINS = "forkedThreadOrigins"
        const val THREAD_PROJECT_BINDINGS = "threadProjectBindings"
        const val LAST_ACTIVE_THREAD_ID = "lastActiveThreadId"
        const val AI_CHANGE_SET_LEDGER = "aiChangeSetLedger"
        const val LOCALLY_ARCHIVED_THREAD_IDS = "locallyArchivedThreadIDs"
        const val LOCALLY_DELETED_THREAD_IDS = "locallyDeletedThreadIDs"
        const val MESSAGE_HISTORY_KEY = "messageHistoryKey"
        const val HAS_SEEN_ONBOARDING = "hasSeenOnboarding"
        const val SELECTED_FONT_STYLE = "selectedFontStyle"
        const val SELECTED_MODEL_ID = "selectedModelId"
        const val SELECTED_REASONING_EFFORT = "selectedReasoningEffort"
        const val SELECTED_SERVICE_TIER = "selectedServiceTier"
        const val SELECTED_ACCESS_MODE = "selectedAccessMode"
        const val NOTIFICATION_PERMISSION_PROMPTED = "notificationPermissionPrompted"
        const val SIDEBAR_MAC_NICKNAME_PREFIX = "sidebarMacNickname."
    }
}
